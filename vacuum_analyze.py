#!/usr/bin/env python3
"""VACUUM ANALYZE the Rhino database from inside the backend pod.

Companion to ``docs_imp/db_performance_measurement_plan.md`` (§1.2 / §4 step 1):
the dev01 boot log showed ``[KR_STATS] ... live_tuples=0 dead_tuples=0`` for a
975k-row / 8.3 GB ``knowledge_reserve`` table — the pg_stat counters were reset
(Azure Flexible Server failover/restart) and never re-populated, so the planner
runs on dead estimates and autovacuum never triggers.  This script restores
sane statistics using only the app's own DB role (no az / portal / superuser):
the role owns its tables, so plain ``VACUUM (ANALYZE)`` works.

Scopes:

  * default        — the relational app tables in ``public`` owned by the
                     current role (always includes ``knowledge_reserve``).
  * ``--graph``    — the AGE graph label tables (schema = KG_AGE_GRAPH,
                     default ``rhino``), plus an inheritance-aware ``ANALYZE``
                     of the ``_ag_label_vertex`` / ``_ag_label_edge`` parents
                     so full-graph scans (the Shape A queries in the plan doc)
                     get real estimates.
  * ``--all``      — both.
  * ``--tables``   — explicit comma-separated list (schema-qualified or not).

``--analyze-only`` skips the VACUUM pass (much faster; fixes planner stats but
not bloat).  ``--dry-run`` lists the targets and exits.

Safety:

  * Runs with ``autocommit`` (VACUUM cannot run inside a transaction).
  * ``statement_timeout = 0`` for the session — VACUUM of an 8 GB table will
    blow through the pool default (DB_STATEMENT_TIMEOUT_MS=300000) otherwise.
  * ``lock_timeout`` (default 10s) — VACUUM takes ShareUpdateExclusiveLock;
    rather than queueing behind a long-running query and stalling, a table
    that cannot be locked in time is SKIPPED and reported.
  * Per-table failures (permissions, locks) are caught and reported; the
    sweep always continues.

Usage (from the backend pod — kubectl targets the StatefulSet pod):

  kubectl exec -it rhino-backend-0 -- python scripts/vacuum_analyze.py
  kubectl exec -it rhino-backend-0 -- python scripts/vacuum_analyze.py --all
  kubectl exec -it rhino-backend-0 -- python scripts/vacuum_analyze.py \
      --graph --analyze-only
  kubectl exec -it rhino-backend-0 -- python scripts/vacuum_analyze.py \
      --tables knowledge_reserve --verbose

Exit codes:
  0 — all targeted tables processed
  1 — at least one table failed or was skipped (details in output)
  2 — could not connect / no targets found
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field

import psycopg
from psycopg import sql

from project_graph.graph.config import (
    fetch_age_entra_token,
    get_age_conninfo,
    get_age_db_auth,
    get_age_graph_name,
)

_DEFAULT_LOCK_TIMEOUT_S = 10

# AGE bookkeeping tables in the graph schema that are not label tables and
# never need a manual sweep.
_GRAPH_SKIP_TABLES = frozenset({"_label_id_seq"})


@dataclass
class TableResult:
    table: str
    status: str = "pending"  # ok | skipped | failed
    seconds: float = 0.0
    detail: str = ""
    live_before: int | None = None
    dead_before: int | None = None
    live_after: int | None = None
    dead_after: int | None = None


@dataclass
class Report:
    results: list[TableResult] = field(default_factory=list)

    @property
    def failures(self) -> list[TableResult]:
        return [r for r in self.results if r.status in ("failed", "skipped")]


def _connect() -> psycopg.Connection:
    """Open a direct autocommit connection using the pod's RHINO_DB_* env.

    Mirrors the AGE driver's Entra handling: the conninfo omits the password
    in entra mode and a fresh token is injected per physical connection.
    """
    conninfo = get_age_conninfo()
    kwargs: dict[str, str] = {}
    if get_age_db_auth() == "entra":
        kwargs["password"] = fetch_age_entra_token()
    conn = psycopg.connect(conninfo, autocommit=True, **kwargs)
    return conn


def _prepare_session(conn: psycopg.Connection, lock_timeout_s: int) -> None:
    with conn.cursor() as cur:
        # Override the pool-default statement_timeout baked into conninfo
        # options; a table-wide VACUUM legitimately exceeds it.
        cur.execute("SET statement_timeout = 0")
        cur.execute(f"SET lock_timeout = '{int(lock_timeout_s)}s'")
        # Keep maintenance work off the critical path as much as the role
        # allows; harmless if the parameter is not settable.
        try:
            cur.execute("SET vacuum_cost_delay = 0")
        except psycopg.Error:
            pass


def _owned_public_tables(conn: psycopg.Connection) -> list[str]:
    """Relational app tables in ``public`` owned by the current role."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT format('%I.%I', schemaname, tablename)
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tableowner = current_user
            ORDER BY pg_total_relation_size(format('%I.%I', schemaname, tablename)::regclass) DESC
            """
        )
        return [row[0] for row in cur.fetchall()]


def _graph_label_tables(conn: psycopg.Connection, graph: str) -> tuple[list[str], list[str]]:
    """Return (label tables, inheritance parents) for the AGE graph schema.

    Parents (``_ag_label_vertex`` / ``_ag_label_edge``) are returned
    separately: VACUUM on an inheritance parent does not cascade, but
    ``ANALYZE parent`` samples the whole inheritance tree and stores the
    inherited statistics the full-graph scans depend on.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = %s AND tableowner = current_user
            ORDER BY pg_total_relation_size(format('%%I.%%I', schemaname, tablename)::regclass) DESC
            """,
            (graph,),
        )
        names = [row[0] for row in cur.fetchall()]
    parents = [
        f"{graph}.{n}" for n in names if n in ("_ag_label_vertex", "_ag_label_edge")
    ]
    labels = [
        f"{graph}.{n}"
        for n in names
        if n not in ("_ag_label_vertex", "_ag_label_edge")
        and n not in _GRAPH_SKIP_TABLES
    ]
    return labels, parents


def _split_qualified(table: str) -> tuple[str, str]:
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "public", table
    return schema.strip('"'), name.strip('"')


def _stat_tuple(conn: psycopg.Connection, table: str) -> tuple[int | None, int | None]:
    schema, name = _split_qualified(table)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n_live_tup, n_dead_tup FROM pg_stat_all_tables "
            "WHERE schemaname = %s AND relname = %s",
            (schema, name),
        )
        row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def _maintain_one(
    conn: psycopg.Connection,
    table: str,
    analyze_only: bool,
    verbose: bool,
) -> TableResult:
    result = TableResult(table=table)
    schema, name = _split_qualified(table)
    ident = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name))
    statement = (
        sql.SQL("ANALYZE {}").format(ident)
        if analyze_only
        else sql.SQL("VACUUM (ANALYZE) {}").format(ident)
    )
    result.live_before, result.dead_before = _stat_tuple(conn, table)
    started = time.monotonic()
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
        result.seconds = time.monotonic() - started
        result.live_after, result.dead_after = _stat_tuple(conn, table)
        result.status = "ok"
        if verbose:
            print(
                f"  ok      {table}  {result.seconds:8.2f}s  "
                f"live {result.live_before}->{result.live_after}  "
                f"dead {result.dead_before}->{result.dead_after}"
            )
    except psycopg.errors.LockNotAvailable as ex:
        result.seconds = time.monotonic() - started
        result.status = "skipped"
        result.detail = f"lock_timeout: {ex}".splitlines()[0]
        print(f"  SKIP    {table}  could not acquire lock (busy) — rerun later")
    except psycopg.errors.InsufficientPrivilege as ex:
        result.seconds = time.monotonic() - started
        result.status = "skipped"
        result.detail = f"not owner: {ex}".splitlines()[0]
        print(f"  SKIP    {table}  insufficient privilege (not owner)")
    except psycopg.Error as ex:
        result.seconds = time.monotonic() - started
        result.status = "failed"
        result.detail = str(ex).splitlines()[0]
        print(f"  FAIL    {table}  {result.detail}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="VACUUM ANALYZE the Rhino DB using the app role (no superuser needed)."
    )
    parser.add_argument(
        "--graph",
        action="store_true",
        help="target the AGE graph label tables instead of the public app tables",
    )
    parser.add_argument(
        "--all",
        dest="everything",
        action="store_true",
        help="target both the public app tables and the AGE graph label tables",
    )
    parser.add_argument(
        "--tables",
        help="explicit comma-separated table list (schema-qualified or public)",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="ANALYZE without VACUUM (faster; fixes planner stats, not bloat)",
    )
    parser.add_argument(
        "--lock-timeout",
        type=int,
        default=_DEFAULT_LOCK_TIMEOUT_S,
        help=f"seconds to wait for each table lock before skipping (default {_DEFAULT_LOCK_TIMEOUT_S})",
    )
    parser.add_argument("--dry-run", action="store_true", help="list targets and exit")
    parser.add_argument("--verbose", action="store_true", help="per-table timing detail")
    args = parser.parse_args()

    try:
        conn = _connect()
    except psycopg.Error as ex:
        print(f"ERROR: could not connect: {str(ex).splitlines()[0]}", file=sys.stderr)
        return 2

    graph = get_age_graph_name()
    try:
        _prepare_session(conn, args.lock_timeout)

        analyze_only_extra: list[str] = []
        if args.tables:
            targets = [t.strip() for t in args.tables.split(",") if t.strip()]
        else:
            targets = []
            if args.everything or not args.graph:
                targets.extend(_owned_public_tables(conn))
            if args.everything or args.graph:
                labels, parents = _graph_label_tables(conn, graph)
                targets.extend(labels)
                # Inheritance parents: ANALYZE-only (VACUUM doesn't cascade,
                # but ANALYZE stores the inherited stats the Shape A
                # full-graph scans need).
                analyze_only_extra.extend(parents)

        if not targets and not analyze_only_extra:
            print("ERROR: no owned tables found for the selected scope", file=sys.stderr)
            return 2

        mode = "ANALYZE" if args.analyze_only else "VACUUM (ANALYZE)"
        print(
            f"{mode} — {len(targets)} table(s)"
            + (f" + {len(analyze_only_extra)} inheritance parent(s) (ANALYZE only)"
               if analyze_only_extra else "")
            + f"  [db auth: {get_age_db_auth()}, graph schema: {graph}]"
        )
        if args.dry_run:
            for t in targets:
                print(f"  {t}")
            for t in analyze_only_extra:
                print(f"  {t}  (ANALYZE only)")
            return 0

        report = Report()
        sweep_started = time.monotonic()
        for t in targets:
            report.results.append(
                _maintain_one(conn, t, analyze_only=args.analyze_only, verbose=args.verbose)
            )
        for t in analyze_only_extra:
            report.results.append(
                _maintain_one(conn, t, analyze_only=True, verbose=args.verbose)
            )
        total = time.monotonic() - sweep_started

        ok = sum(1 for r in report.results if r.status == "ok")
        print(
            f"\nDone in {total:.1f}s: {ok} ok, "
            f"{sum(1 for r in report.results if r.status == 'skipped')} skipped, "
            f"{sum(1 for r in report.results if r.status == 'failed')} failed."
        )

        # Highlight the table that motivated this script.
        for r in report.results:
            if r.table.endswith("knowledge_reserve") and r.status == "ok":
                print(
                    f"knowledge_reserve: live {r.live_before}->{r.live_after}, "
                    f"dead {r.dead_before}->{r.dead_after} ({r.seconds:.1f}s). "
                    "Boot [KR_STATS] should now report non-zero live_tuples."
                )

        if report.failures:
            print("\nSkipped/failed tables:")
            for r in report.failures:
                print(f"  {r.status:7s} {r.table}  {r.detail}")
            return 1
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
