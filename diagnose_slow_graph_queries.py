#!/usr/bin/env python3
"""Diagnose the slow AGE graph queries (Shapes A/B/C) with hard evidence.

Companion to ``docs_imp/db_performance_measurement_plan.md`` §1.4. The plan doc
infers the root causes from log timings; this script *proves* them from inside
the pod with query plans and catalog facts, and prints the ready-to-run
``CREATE INDEX`` candidates. Read-only apart from the EXPLAIN ANALYZE probes
(``--deep``), which execute the real Shape A lookup once to time it.

Sections:

  1. context      — PG version, cache-hit ratio, graph size (label/edge table
                    counts, top tables by size incl. RELATED_TO).
  2. indexes      — every index in the graph schema; flags whether ANY label
                    table has an expression index on ``properties->>'"id"'``
                    (Shape A) and which edge tables index ``start_id``
                    (Shape B); also lists knowledge_reserve/citations indexes.
  3. plans        — EXPLAIN (costs only, instant) of the Shape A parent lookup
                    and the Shape B generic edge scan: the plan output shows
                    the N-way Append over every child table — the smoking gun.
  4. seq-scans    — top tables by ``seq_scan`` counter since stats reset:
                    which tables the workload actually full-scans.
  5. candidates   — ``CREATE INDEX CONCURRENTLY`` statements for the hottest
                    label tables (by seq_scan) ready to paste into a migration.

``--deep`` additionally runs EXPLAIN (ANALYZE, BUFFERS) on the real Shape A
single lookup + a 5-id batch (expect ~10-20s each on dev01 — that IS the
finding).

Usage (pod image predates this file — pipe over stdin; strip CRLF, name the
namespace and container explicitly — the pod also runs istio-proxy):

  (Get-Content -Raw DigitalTransformerBackend\\scripts\\diagnose_slow_graph_queries.py) -replace "`r","" |
    kubectl exec -i -n rhino rhino-backend-0 -c backend -- python -u -

  # deep pass + greppable UTF-8 capture:
  (Get-Content -Raw ...\\diagnose_slow_graph_queries.py) -replace "`r","" |
    kubectl exec -i -n rhino rhino-backend-0 -c backend -- python -u - --deep |
    Tee-Object -FilePath images\\diagnose_rhino_log.txt

Also note the app's built-in switch: setting ``KG_AGE_EXPLAIN_SLOW=true`` on
the deployment makes ``age_session`` log an EXPLAIN ANALYZE plan alongside
every ``age.query SLOW`` line for real traffic (see
``project_graph/graph/config.py::get_age_explain_slow_queries``).
"""

from __future__ import annotations

import argparse
import sys

import psycopg

from project_graph.graph.config import (
    fetch_age_entra_token,
    get_age_conninfo,
    get_age_db_auth,
    get_age_graph_name,
)

_DEEP_TIMEOUT_MS = 120_000
_TOP_N = 15


def _connect() -> psycopg.Connection:
    conn_kwargs: dict[str, str] = {}
    if get_age_db_auth() == "entra":
        conn_kwargs["password"] = fetch_age_entra_token()
    conn = psycopg.connect(get_age_conninfo(), autocommit=True, **conn_kwargs)
    # agtype's ->> operator lives in ag_catalog; without it on the search_path
    # every properties->>'"id"' probe fails (this is why an earlier run skipped
    # section 3 with "could not sample a node id").
    with conn.cursor() as cur:
        # SET cannot take bind parameters — compose the identifier safely.
        from psycopg import sql as _sql
        cur.execute(
            _sql.SQL("SET search_path = ag_catalog, {}, public").format(
                _sql.Identifier(get_age_graph_name())
            )
        )
    return conn


def _rows(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _one(conn, query, params=None):
    rows = _rows(conn, query, params)
    return rows[0] if rows else None


def _banner(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def section_context(conn, graph: str) -> None:
    _banner("1. CONTEXT")
    print("server:", _one(conn, "SELECT version()")[0].split(" on ")[0])
    hit = _one(
        conn,
        "SELECT round(100.0 * sum(blks_hit) / nullif(sum(blks_hit) + sum(blks_read), 0), 2) "
        "FROM pg_stat_database WHERE datname = current_database()",
    )[0]
    print(f"cache hit ratio (since stats reset): {hit}%  (<99% on a hot working set ⇒ I/O bound)")

    counts = _one(
        conn,
        """
        SELECT count(*) FILTER (WHERE c.relkind = 'r'),
               count(*) FILTER (WHERE c.relkind = 'r'
                                AND EXISTS (SELECT 1 FROM pg_inherits i
                                            WHERE i.inhrelid = c.oid
                                              AND i.inhparent = %(g)s::regclass))
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %(schema)s
        """,
        {"schema": graph, "g": f'{graph}."_ag_label_vertex"'},
    )
    print(f"graph schema '{graph}': {counts[0]} tables, {counts[1]} vertex label tables")

    print(f"\ntop {_TOP_N} graph tables by size (reltuples = planner's row estimate):")
    for name, size, tuples in _rows(
        conn,
        """
        SELECT c.relname, pg_size_pretty(pg_total_relation_size(c.oid)), c.reltuples::bigint
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %s
        """,
        (graph, _TOP_N),
    ):
        print(f"  {name:45s} {size:>10s}  rows≈{tuples}")


def section_indexes(conn, graph: str) -> tuple[bool, set[str]]:
    _banner("2. INDEX INVENTORY")
    idx = _rows(
        conn,
        "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = %s "
        "ORDER BY tablename, indexname",
        (graph,),
    )
    prop_id_tables = {t for t, _, d in idx if "properties" in d and '"id"' in d}
    start_id_tables = {t for t, _, d in idx if "start_id" in d}
    print(f"graph-schema indexes: {len(idx)} total across {len({t for t, _, _ in idx})} tables")
    print(
        f"label tables with an index on properties->>'\"id\"' (Shape A): "
        f"{sorted(prop_id_tables) if prop_id_tables else 'NONE  <-- Shape A root cause confirmed'}"
    )
    print(
        f"edge tables with an index on start_id (Shape B): "
        f"{len(start_id_tables)} ({', '.join(sorted(start_id_tables)[:8])}"
        f"{'...' if len(start_id_tables) > 8 else ''})"
        if start_id_tables
        else "edge tables with an index on start_id (Shape B): NONE"
    )

    print("\nrelational hot-table indexes (knowledge_reserve, citations):")
    for tbl, name, definition in _rows(
        conn,
        "SELECT tablename, indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = 'public' AND tablename IN ('knowledge_reserve', 'citations') "
        "ORDER BY tablename, indexname",
    ):
        print(f"  {tbl}.{name}: {definition.split(' USING ', 1)[-1]}")
    return bool(prop_id_tables), start_id_tables


def _sample_node_id(conn, graph: str) -> str | None:
    """Grab one real public node id from a small-ish label table."""
    for label in ("DynamicObject", "File", "Project"):
        try:
            row = _one(
                conn,
                f'SELECT properties->>\'"id"\' FROM {graph}."{label}" '
                "WHERE properties->>'\"id\"' IS NOT NULL LIMIT 1",
            )
            if row and row[0]:
                return row[0]
        except psycopg.Error:
            continue
    return None


def section_plans(conn, graph: str, deep: bool) -> None:
    _banner("3. QUERY PLANS (the smoking gun)")
    node_id = _sample_node_id(conn, graph)
    if not node_id:
        print("could not sample a node id — skipping plan section")
        return
    print(f"sample node_id: {node_id}\n")

    shape_a = (
        f"SELECT pv.id FROM {graph}._ag_label_vertex pv "
        "WHERE pv.properties->>'\"id\"' = %(node_id)s LIMIT 1"
    )
    print("--- Shape A parent lookup, EXPLAIN (costs only — count the Append children): ---")
    plan = _rows(conn, "EXPLAIN " + shape_a, {"node_id": node_id})
    appends = sum(1 for (line,) in plan if "Seq Scan" in line)
    for (line,) in plan[:6]:
        print(f"  {line}")
    print(f"  ... plan is {len(plan)} lines; {appends} sequential scans in the Append. "
          "Every children/ref-batch call pays all of them.")

    print("\n--- Shape B generic edge scan by start vertex, EXPLAIN (costs only): ---")
    plan = _rows(
        conn,
        f"EXPLAIN SELECT * FROM {graph}._ag_label_edge WHERE start_id = 0",
    )
    appends = sum(1 for (line,) in plan if "Seq Scan" in line)
    print(f"  plan is {len(plan)} lines; {appends} sequential scans — an unanchored -[r]-> "
          "pays every edge table (incl. RELATED_TO at ~11.8M rows).")

    if not deep:
        print("\n(re-run with --deep to EXPLAIN ANALYZE the real Shape A lookup — "
              "expect ~10-20s; that measured time IS the per-call cost users see)")
        return

    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {_DEEP_TIMEOUT_MS}")
    print("\n--- DEEP: Shape A single lookup, EXPLAIN (ANALYZE, BUFFERS): ---")
    try:
        plan = _rows(
            conn, "EXPLAIN (ANALYZE, BUFFERS) " + shape_a, {"node_id": node_id}
        )
        for (line,) in plan:
            if any(k in line for k in ("Execution Time", "Planning Time", "Buffers", "Limit")):
                print(f"  {line.strip()}")
    except psycopg.Error as ex:
        print(f"  timed out / failed: {str(ex).splitlines()[0]}")


def section_seq_scans(conn, graph: str) -> tuple[list[str], list[str]]:
    """Return (hot vertex label tables, hot edge tables) by seq-scan volume."""
    _banner("4. WHO GETS FULL-SCANNED (pg_stat seq_scan counters)")
    rows = _rows(
        conn,
        """
        SELECT s.schemaname || '.' || s.relname, s.seq_scan, s.seq_tup_read, s.idx_scan,
               EXISTS (SELECT 1 FROM pg_inherits i
                       WHERE i.inhrelid = (s.schemaname || '.' || quote_ident(s.relname))::regclass
                         AND i.inhparent = (quote_ident(s.schemaname) || '._ag_label_edge')::regclass
                      ) AS is_edge
        FROM pg_stat_all_tables s
        WHERE s.schemaname IN ('public', %s) AND s.seq_scan > 0
        ORDER BY s.seq_tup_read DESC LIMIT %s
        """,
        (graph, _TOP_N),
    )
    print(f"{'table':50s} {'seq_scans':>10s} {'rows_read_by_seq':>17s} {'idx_scans':>10s}  kind")
    hot_vertex: list[str] = []
    hot_edge: list[str] = []
    for name, seq, tup, idx_scan, is_edge in rows:
        kind = "edge" if is_edge else ("vertex" if name.startswith(f"{graph}.") else "sql")
        print(f"{name:50s} {seq:>10} {tup:>17} {idx_scan if idx_scan is not None else 0:>10}  {kind}")
        if name.startswith(f"{graph}."):
            (hot_edge if is_edge else hot_vertex).append(name.split(".", 1)[1])
    return hot_vertex, hot_edge


def section_candidates(
    hot_vertex: list[str],
    hot_edge: list[str],
    graph: str,
    have_prop_idx: bool,
    start_id_tables: set[str],
) -> None:
    _banner("5. CANDIDATE FIXES (paste into a migration after review)")
    if hot_edge:
        print("EDGE tables dominate the seq-scan volume — index their join keys first:\n")
        for label in hot_edge[:10]:
            marker = "  -- already has a start_id index?!" if label in start_id_tables else ""
            print(
                f'  CREATE INDEX CONCURRENTLY IF NOT EXISTS "ix_{label.lower()}_start_id"\n'
                f'    ON {graph}."{label}" (start_id);{marker}\n'
                f'  CREATE INDEX CONCURRENTLY IF NOT EXISTS "ix_{label.lower()}_end_id"\n'
                f'    ON {graph}."{label}" (end_id);'
            )
        print(
            "\n  ...and anchor the relationship label in Cypher instead of type(r) IN [...]:\n"
            "  MATCH (item:Label)-[r:HAS_RHINO_CITATIONS]->(child)   -- scans 1 edge table"
        )
    if hot_vertex:
        if have_prop_idx:
            print("\nVertex labels below are seq-scanned despite the prop-id indexes existing on")
            print("many labels — check section 2 coverage for THESE specific labels:")
        else:
            print("\nVertex label expression indexes (parents don't inherit):")
        for label in hot_vertex[:10]:
            print(
                f'  CREATE INDEX CONCURRENTLY IF NOT EXISTS "ix_{label.lower()}_prop_id"\n'
                f'    ON {graph}."{label}" ((properties->>\'"id"\'));'
            )
    print(
        "\nLive-traffic plan capture (no code changes): set KG_AGE_EXPLAIN_SLOW=true on the\n"
        "backend deployment — every `age.query SLOW` line then logs its EXPLAIN ANALYZE plan."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose slow AGE graph queries (read-only).")
    parser.add_argument(
        "--deep",
        action="store_true",
        help="also EXPLAIN ANALYZE the real Shape A lookup (executes it once, ~10-20s)",
    )
    args = parser.parse_args()

    try:
        conn = _connect()
    except psycopg.Error as ex:
        print(f"ERROR: could not connect: {str(ex).splitlines()[0]}", file=sys.stderr)
        return 2

    graph = get_age_graph_name()
    try:
        section_context(conn, graph)
        have_prop_idx, start_id_tables = section_indexes(conn, graph)
        section_plans(conn, graph, deep=args.deep)
        hot_vertex, hot_edge = section_seq_scans(conn, graph)
        section_candidates(hot_vertex, hot_edge, graph, have_prop_idx, start_id_tables)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
