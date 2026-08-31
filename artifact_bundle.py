#!/usr/bin/env python3
"""Bundle an artifact (type + record + optional dynamic objects) from one Rhino
environment and recreate it in another.

Two subcommands:

  export  -- fetch the artifact record + its artifact type (and composite member
             types) from a source environment, optionally run the Dynamic Object
             graph export, and zip everything into a portable bundle.

  import  -- read a bundle, recreate the artifact type (if not SYSTEM) and the
             artifact record in a target project, optionally run the Dynamic
             Object graph import from the bundle.

Auth: pass --token, or --username (+ --password / interactive prompt), or set
RHINO_TOKEN / RHINO_USERNAME / RHINO_PASSWORD. Credentials are never written to
the bundle or to disk.

Examples
--------
Export from staging (DOs exported via API too):

  python artifact_bundle.py export \
    --base-url https://stable-max-openai-gpt.stg.rhinoapi.com \
    --project-id 3b2a66d9-af6e-49fc-9142-5c7c0ecf4a73 \
    --artifact-id 609755a2-a9eb-457b-a5ba-33e0bc09de06 \
    --username you@example.com \
    --export-dos \
    --out artifact_bundle.zip

Export but reuse a DO export JSON you already downloaded from the UI:

  python artifact_bundle.py export ... --dos-file exported_dos.json --out artifact_bundle.zip

Import into local (target project already holds the imported DOs, so skip them):

  python artifact_bundle.py import \
    --base-url http://localhost:9080 \
    --project-id b28946c4-2f76-4048-883a-6269da3881ae \
    --username admin \
    --bundle artifact_bundle.zip

Import including the bundled DOs:

  python artifact_bundle.py import ... --import-dos
"""

from __future__ import annotations

import argparse
import getpass
import io
import json
import os
import subprocess
import sys
import time
import uuid as uuid_mod
import zipfile
from datetime import datetime, timezone
from typing import Any

import requests

DEFAULT_TIMEOUT = 60  # seconds per plain HTTP call
IO_TASK_POLL_INTERVAL = 3  # seconds between io-task result polls
IO_TASK_MAX_WAIT = 30 * 60  # give a large graph export/import half an hour

ARTIFACT_JSON = "artifact.json"
TYPE_JSON = "artifact_type.json"
MEMBER_DIR = "member_types/"
DOS_JSON = "dynamic_objects.json"
DO_DEFS_JSON = "do_definitions.json"
MANIFEST_JSON = "manifest.json"


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #


def _looks_like_api(resp: requests.Response) -> bool:
    """True when the response comes from the Flask API rather than the SPA.

    An SPA nginx serves index.html (200, text/html) for ANY path, so status
    alone cannot identify the API root -- require a JSON response too.
    """
    if resp.status_code not in (200, 401, 403):
        return False
    if "json" in (resp.headers.get("Content-Type") or ""):
        return True
    return resp.text.lstrip()[:1] in ("{", "[")


def _resolve_api_base(base_url: str, verify_tls: bool = True) -> str:
    """Find the API root: the backend serves routes at '/', while the
    frontend nginx proxies them under '/api'. Probe both."""
    base = base_url.rstrip("/")
    candidates = [base] if base.endswith("/api") else [base, base + "/api"]
    for candidate in candidates:
        try:
            resp = requests.get(f"{candidate}/projects", timeout=10, verify=verify_tls)
        except requests.RequestException:
            continue
        if _looks_like_api(resp):
            return candidate
    sys.exit(f"Could not find the API root under {base} (tried {', '.join(candidates)})")


class RhinoClient:
    """Minimal client for the Rhino DTB REST API."""

    def __init__(self, base_url: str, token: str, verify_tls: bool = True):
        self.api = _resolve_api_base(base_url, verify_tls)
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"
        self.session.verify = verify_tls

    # -- auth ---------------------------------------------------------------

    @staticmethod
    def login(base_url: str, username: str, password: str, verify_tls: bool = True) -> str:
        api = _resolve_api_base(base_url, verify_tls)
        resp = requests.post(
            f"{api}/login",
            json={"username": username, "password": password},
            timeout=DEFAULT_TIMEOUT,
            verify=verify_tls,
        )
        if resp.status_code == 401:
            sys.exit("Login failed: invalid username or password")
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            sys.exit("Login succeeded but no access_token in response")
        return token

    # -- plain REST helpers -------------------------------------------------

    @staticmethod
    def _json_or_die(resp: requests.Response, what: str) -> Any:
        if resp.status_code in (401, 422):
            sys.exit(f"{what} rejected ({resp.status_code}): token invalid or expired. "
                     "Copy a FRESH 'Authorization: Bearer ...' value from a request to an "
                     "/api/... endpoint in the browser Network tab (SSO envs accept the IdP "
                     "access token; password envs use the Rhino 'eyJ...' JWT).")
        if resp.status_code >= 400:
            sys.exit(f"{what} failed ({resp.status_code}): {resp.text[:500]}")
        if "json" not in (resp.headers.get("Content-Type") or "") and resp.text.lstrip()[:1] not in ("{", "["):
            sys.exit(f"{what} returned non-JSON (got {resp.headers.get('Content-Type')!r}) -- "
                     "this is usually an auth redirect or the SPA page, check base URL and token. "
                     f"First bytes: {resp.text[:200]!r}")
        return resp.json()

    def get(self, path: str, **kwargs: Any) -> Any:
        resp = self.session.get(f"{self.api}{path}", timeout=DEFAULT_TIMEOUT, **kwargs)
        return self._json_or_die(resp, f"GET {path}")

    def post(self, path: str, payload: Any, **kwargs: Any) -> Any:
        resp = self.session.post(f"{self.api}{path}", json=payload, timeout=DEFAULT_TIMEOUT, **kwargs)
        return self._json_or_die(resp, f"POST {path}") if resp.content else None

    def post_raw(self, path: str, payload: Any) -> requests.Response:
        """POST without exiting on HTTP errors -- caller inspects the response."""
        return self.session.post(f"{self.api}{path}", json=payload, timeout=DEFAULT_TIMEOUT)

    def patch(self, path: str, payload: Any) -> Any:
        resp = self.session.patch(f"{self.api}{path}", json=payload, timeout=DEFAULT_TIMEOUT)
        return self._json_or_die(resp, f"PATCH {path}") if resp.content else None

    # -- graph I/O background tasks ----------------------------------------

    def _wait_io_task(self, project_id: str, task_id: str, label: str) -> Any:
        """Poll /graph/io-tasks/<id>/result until the task finishes.

        The task runs server-side (Redis-backed), so transient network
        failures while polling -- LB idle resets, pod restarts, proxy
        blips -- must NOT abort the wait. Consecutive connection errors
        are retried with backoff; only a persistent outage gives up, and
        then with instructions to re-attach via the ``poll`` subcommand.
        """
        deadline = time.monotonic() + IO_TASK_MAX_WAIT
        path = f"/projects/{project_id}/graph/io-tasks/{task_id}/result"
        net_errors = 0
        seen_running = False
        while time.monotonic() < deadline:
            try:
                resp = self.session.get(f"{self.api}{path}", timeout=DEFAULT_TIMEOUT)
            except requests.RequestException as exc:
                net_errors += 1
                if net_errors > 20:
                    break
                print(f"  {label}: connection hiccup ({type(exc).__name__}), retrying "
                      f"({net_errors}/20) ...")
                time.sleep(min(30, IO_TASK_POLL_INTERVAL * net_errors))
                continue
            net_errors = 0
            if resp.status_code == 200:
                print(f"  {label}: done")
                return resp.json()
            if resp.status_code == 202:
                seen_running = True
                print(f"  {label}: running...")
                time.sleep(IO_TASK_POLL_INTERVAL)
                continue
            if resp.status_code == 404 and seen_running:
                # Task state can briefly vanish while a pod restarts and
                # the store reconnects; give it a grace window.
                net_errors += 1
                if net_errors > 6:
                    sys.exit(f"{label} task {task_id} disappeared from the task store -- the "
                             "backend may have restarted mid-task. Check whether the data "
                             "arrived before retrying (a re-run can duplicate imported DOs).")
                time.sleep(10)
                continue
            sys.exit(f"{label} task failed ({resp.status_code}): {resp.text[:500]}")
        sys.exit(
            f"{label} task {task_id} did not finish before the client gave up. It may still "
            f"be running server-side. Re-attach with:\n"
            f"  artifact_bundle.py poll --base-url <url> --project-id {project_id} "
            f"--task-id {task_id} <auth args>"
        )

    def export_dynamic_objects(
        self,
        project_id: str,
        run_id: str | None = None,
        do_ids: list[str] | None = None,
        scope: str | None = None,
    ) -> Any:
        params: dict[str, str] = {}
        if run_id:
            params["run_id"] = run_id
        if do_ids:
            params["do_ids"] = ",".join(do_ids)
        if scope:
            params["scope"] = scope
        resp = self.session.get(
            f"{self.api}/projects/{project_id}/graph/export/dynamic-objects",
            params=params,
            headers={"Accept": "text/event-stream"},
            timeout=DEFAULT_TIMEOUT,
        )
        if resp.status_code != 202:
            sys.exit(f"DO export enqueue failed ({resp.status_code}): {resp.text[:500]}")
        task_id = resp.json()["task_id"]
        print(f"  DO export task {task_id} enqueued")
        return self._wait_io_task(project_id, task_id, "DO export")

    def import_dynamic_objects(self, project_id: str, envelope: dict[str, Any]) -> Any:
        resp = self.session.post(
            f"{self.api}/projects/{project_id}/graph/import/dynamic-objects",
            json=envelope,
            headers={"Accept": "text/event-stream"},
            timeout=DEFAULT_TIMEOUT * 5,  # large payload upload
        )
        if resp.status_code != 202:
            sys.exit(f"DO import enqueue failed ({resp.status_code}): {resp.text[:500]}")
        task_id = resp.json()["task_id"]
        print(f"  DO import task {task_id} enqueued")
        return self._wait_io_task(project_id, task_id, "DO import")


# --------------------------------------------------------------------------- #
# Auth resolution
# --------------------------------------------------------------------------- #


def _warn_if_not_jwt(token: str) -> str:
    if not token.startswith("eyJ"):
        print(f"NOTE: token starts with {token[:6]!r} (not a Rhino 'eyJ...' JWT). That is fine for "
              "SSO environments, which accept the IdP access token the browser sends -- but it must "
              "be the exact bearer the SPA is currently using (copy it from a request to an /api/... "
              "endpoint in the Network tab; these expire quickly).")
    return token


def resolve_token(args: argparse.Namespace) -> str:
    """Token precedence: --token > env RHINO_TOKEN > username/password login."""
    if args.token:
        return _warn_if_not_jwt(args.token)
    env_token = os.environ.get("RHINO_TOKEN")
    if env_token:
        return _warn_if_not_jwt(env_token)
    username = args.username or os.environ.get("RHINO_USERNAME")
    if not username:
        sys.exit("No auth provided: use --token, RHINO_TOKEN, or --username/--password")
    password = args.password or os.environ.get("RHINO_PASSWORD") or getpass.getpass(
        f"Password for {username} @ {args.base_url}: "
    )
    print(f"Logging in as {username} ...")
    return RhinoClient.login(args.base_url, username, password, verify_tls=not args.insecure)


# --------------------------------------------------------------------------- #
# export
# --------------------------------------------------------------------------- #


def cmd_export(args: argparse.Namespace) -> None:
    client = RhinoClient(args.base_url, resolve_token(args), verify_tls=not args.insecure)
    pid = args.project_id

    print(f"Fetching artifact {args.artifact_id} ...")
    artifact = client.get(f"/projects/{pid}/artifacts/{args.artifact_id}")
    type_key = artifact.get("artifact_type")
    type_id = artifact.get("artifact_type_id")
    print(f"  artifact_type={type_key!r} scope={artifact.get('scope')} name={artifact.get('name')!r}")

    artifact_type: dict[str, Any] | None = None
    member_types: list[dict[str, Any]] = []

    if not type_id:
        # Older records store only the key -- try to resolve it from the list.
        listed = client.get(f"/projects/{pid}/artifact-types")
        match = next((t for t in listed if t.get("key") == type_key), None)
        type_id = match["id"] if match else None

    if type_id:
        print(f"Fetching artifact type {type_id} ...")
        artifact_type = client.get(f"/projects/{pid}/artifact-types/{type_id}")
        if artifact_type.get("scope") == "SYSTEM":
            print("  SYSTEM type: bundled for reference, target will use its own seeded copy")
        for member_id in artifact_type.get("member_type_ids") or []:
            print(f"Fetching composite member type {member_id} ...")
            member_types.append(client.get(f"/projects/{pid}/artifact-types/{member_id}"))
    else:
        print(f"  WARNING: no artifact_type row found for key {type_key!r}; "
              "target must already have a type with this key (or the viewer falls back)")

    do_definitions: Any = None
    defs_resp = client.session.get(
        f"{client.api}/dynamic-objects/definitions/export", params={"project_id": pid}, timeout=DEFAULT_TIMEOUT
    )
    if defs_resp.status_code == 200 and "json" in (defs_resp.headers.get("Content-Type") or ""):
        do_definitions = defs_resp.json()
        print(f"Fetched {len((do_definitions or {}).get('definitions') or [])} custom DO definition(s)")
    else:
        print(f"  WARNING: could not export DO definitions ({defs_resp.status_code}); bundle will omit them")

    dos_envelope: Any = None
    if args.dos_file:
        print(f"Reading DO export from {args.dos_file} ...")
        with open(args.dos_file, encoding="utf-8") as fh:
            dos_envelope = json.load(fh)
    elif args.export_dos:
        print("Running Dynamic Object export ...")
        dos_envelope = client.export_dynamic_objects(
            pid,
            run_id=args.run_id,
            do_ids=[d for d in (args.do_ids or "").split(",") if d] or None,
            scope=args.scope,
        )

    manifest = {
        "bundle_version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source_base_url": args.base_url,
        "source_project_id": pid,
        "source_artifact_id": args.artifact_id,
        "artifact_type_key": type_key,
        "has_dynamic_objects": dos_envelope is not None,
    }

    with zipfile.ZipFile(args.out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(MANIFEST_JSON, json.dumps(manifest, indent=2))
        zf.writestr(ARTIFACT_JSON, json.dumps(artifact, indent=2))
        if artifact_type is not None:
            zf.writestr(TYPE_JSON, json.dumps(artifact_type, indent=2))
        for member in member_types:
            zf.writestr(f"{MEMBER_DIR}{member['id']}.json", json.dumps(member, indent=2))
        if do_definitions is not None:
            zf.writestr(DO_DEFS_JSON, json.dumps(do_definitions, indent=2))
        if dos_envelope is not None:
            zf.writestr(DOS_JSON, json.dumps(dos_envelope))

    print(f"\nBundle written: {args.out}")
    print(f"  artifact:      {artifact.get('name') or args.artifact_id}")
    print(f"  type:          {type_key} ({(artifact_type or {}).get('scope', 'UNRESOLVED')})")
    print(f"  member types:  {len(member_types)}")
    print(f"  dynamic objs:  {'yes' if dos_envelope is not None else 'no (use --export-dos or --dos-file)'}")


# --------------------------------------------------------------------------- #
# import
# --------------------------------------------------------------------------- #


# Runs inside the backend container (docker/kubectl exec): re-stamps imported
# containers onto the target project's definition groups, then adopts imported
# item nodes (null project_id) so the viewer's bleed-filter keeps them.
_ALIGN_SNIPPET = r'''
import json
from app import app
with app.app_context():
    from sqlalchemy import text
    from common.database import db
    from dynamic_objects.graph_service import align_container_definition_groups, get_age_connection
    pid = "__PID__"
    rows = db.session.execute(text(
        "SELECT node_label, definition_group_id FROM do_definition WHERE project_id = :pid"
    ), {"pid": pid}).fetchall()
    mapping = {r.node_label: str(r.definition_group_id) for r in rows}
    groups = align_container_definition_groups(pid, mapping)
    with get_age_connection() as s:
        res = s.run(
            "MATCH (p:Project {id: $pid})-[:HAS_EXTRACTION]->(:Extraction)"
            "-[:HAS_DYNAMIC_OBJECT]->(d:DynamicObject)-[:PRODUCED]->(item) "
            "WHERE d.project_id = $pid AND item.project_id IS NULL "
            "SET item.project_id = $pid RETURN count(item) AS stamped",
            {"pid": pid},
        )
        stamped = res.rows[0].get("stamped") if res.rows else 0
    print("ALIGN_RESULT::" + json.dumps({"groups": groups, "stamped": stamped}))
'''


# Pure-SQL variant of the alignment, run through psql against the Postgres
# instance that hosts the AGE graph. Needs no backend container at all, so it
# works when the backend pod is read-only / exec-restricted. Same two repairs:
# container re-stamp per do_definition row, then item project_id adoption.
_ALIGN_SQL_TEMPLATE = r"""
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
DO $align$
DECLARE
  r RECORD;
BEGIN
  FOR r IN SELECT node_label, definition_group_id::text AS dgid
           FROM public.do_definition WHERE project_id = '__PID__'::uuid
  LOOP
    EXECUTE format(
      'SELECT * FROM cypher(%L, $q$ MATCH (p:Project {id: "__PID__"})'
      '-[:HAS_EXTRACTION]->(:Extraction)-[:HAS_DYNAMIC_OBJECT]->(d:DynamicObject) '
      'WHERE d.node_label = "%s" '
      'SET d.definition_group_id = "%s", d.project_id = "__PID__" '
      'RETURN count(d) $q$) AS (v agtype)',
      '__GRAPH__', r.node_label, r.dgid);
  END LOOP;
  EXECUTE format(
    'SELECT * FROM cypher(%L, $q$ MATCH (p:Project {id: "__PID__"})'
    '-[:HAS_EXTRACTION]->(:Extraction)-[:HAS_DYNAMIC_OBJECT]->(d:DynamicObject)'
    '-[:PRODUCED]->(item) '
    'WHERE d.project_id = "__PID__" AND item.project_id IS NULL '
    'SET item.project_id = "__PID__" '
    'RETURN count(item) $q$) AS (v agtype)',
    '__GRAPH__');
END
$align$;
SELECT 'ALIGN_SQL_DONE' AS marker;
"""


def _run_graph_alignment_sql(psql_prefix: str, pid: str, graph: str) -> None:
    """Run the graph repair as SQL via psql (e.g. in the Postgres container).

    ``psql_prefix`` must be a full psql invocation, e.g.
    ``docker exec digitaltransformerbackend-postgres-1 psql -U admin -d rhino``
    or ``kubectl exec -n <ns> <pg-pod> -- psql -U rhino_rw -d rhino``.
    """
    uuid_mod.UUID(pid)
    if not graph.replace("_", "").isalnum():
        sys.exit(f"Suspicious AGE graph name {graph!r}")
    sql = _ALIGN_SQL_TEMPLATE.replace("__PID__", pid).replace("__GRAPH__", graph)
    cmd = [*psql_prefix.split(), "-v", "ON_ERROR_STOP=1", "-c", sql]
    print(f"Aligning graph containers/items via: {psql_prefix} -c <sql> ...")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        proc = None
        error = str(exc)
    if proc is not None:
        error = (proc.stderr or proc.stdout or "").strip()
    if proc is None or proc.returncode != 0 or "ALIGN_SQL_DONE" not in proc.stdout:
        fallback = f"align_graph_{pid}.sql"
        with open(fallback, "w", encoding="utf-8") as fh:
            fh.write(sql)
        print("  WARNING: SQL alignment could not run:")
        for line in error.splitlines()[-5:]:
            print(f"    {line}")
        print(f"  SQL written to {fallback} -- run it with psql against the rhino database to finish.")
        return
    print("  graph containers re-stamped and imported items adopted (SQL mode)")


def _run_graph_alignment(exec_prefix: str, pid: str) -> None:
    """Run the container/item graph repair inside the backend container.

    ``exec_prefix`` is e.g. ``docker exec digitaltransformerbackend-api-1`` or
    ``kubectl exec -n rhino rhino-backend-0 --``; ``python -c <snippet>`` is
    appended. On failure the snippet is written next to the CWD so it can be
    run manually inside the backend container.
    """
    uuid_mod.UUID(pid)  # the pid is spliced into the snippet -- refuse non-UUIDs
    code = _ALIGN_SNIPPET.replace("__PID__", pid)
    cmd = [*exec_prefix.split(), "python", "-c", code]
    print(f"Aligning graph containers/items via: {exec_prefix} python -c <snippet> ...")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        proc = None
        error = str(exc)
    marker = None
    if proc is not None:
        error = (proc.stderr or proc.stdout or "").strip()
        for line in proc.stdout.splitlines():
            if line.startswith("ALIGN_RESULT::"):
                marker = line
                break
    if marker is None:
        fallback = f"align_graph_{pid}.py"
        with open(fallback, "w", encoding="utf-8") as fh:
            fh.write(code)
        print("  WARNING: graph alignment could not run via exec:")
        for line in error.splitlines()[-5:]:
            print(f"    {line}")
        print(f"  Snippet written to {fallback} -- run it with `python {fallback}`'s content "
              "inside the backend container (python -c \"$(cat ...)\") to finish the import.")
        return
    result = json.loads(marker.split("::", 1)[1])
    print(f"  aligned {result['groups']} label group(s); "
          f"stamped project_id on {result['stamped']} imported item node(s)")


def _delete_imported_dos(client: RhinoClient, pid: str) -> None:
    """Remove previously imported DO nodes so a re-import replaces cleanly.

    Uses the imported-do-instances surface (selects ``_imported: true``
    containers scoped to the project). On environments without that
    endpoint the re-import falls back to MERGE semantics: existing nodes
    are updated in place, new ones added, but nodes deleted at the source
    linger in the target.
    """
    url = f"{client.api}/projects/{pid}/graph/imported-do-instances"
    resp = client.session.get(url, timeout=DEFAULT_TIMEOUT)
    if resp.status_code != 200 or "json" not in (resp.headers.get("Content-Type") or ""):
        print(f"  WARNING: imported-do-instances endpoint unavailable ({resp.status_code}); "
              "skipping delete -- re-import will merge/update in place instead")
        return
    info = resp.json()
    print(f"  replacing previously imported DOs: {json.dumps(info)[:250]}")
    dresp = client.session.delete(url, json={}, timeout=600)
    if dresp.status_code < 400:
        print(f"    deleted: {dresp.text[:250]}")
    else:
        print(f"    WARNING: delete failed ({dresp.status_code}): {dresp.text[:250]} -- "
              "continuing; re-import will merge/update in place")


def _target_label_map(client: RhinoClient, pid: str) -> dict[str, str]:
    """node_label -> definition_group_id for every definition in the target."""
    listed = client.get("/dynamic-objects/definitions", params={"project_id": pid})
    return {
        d["node_label"]: d["definition_group_id"]
        for d in listed.get("definitions") or []
        if d.get("node_label") and d.get("definition_group_id")
    }


def _stamp_node_tree(node: dict[str, Any], pid: str) -> None:
    """Recursively set project_id on a node and its items/children subtrees."""
    props = node.get("properties")
    if isinstance(props, dict):
        props["project_id"] = pid
    for item in node.get("items") or []:
        if isinstance(item, dict):
            _stamp_node_tree(item, pid)
    for child_list in (node.get("children") or {}).values():
        for child in child_list or []:
            if isinstance(child, dict):
                _stamp_node_tree(child, pid)


def _collect_envelope_run_ids(obj: Any, found: set[str]) -> None:
    """Mirror of the backend's _collect_run_ids: every run_id string at any depth."""
    if isinstance(obj, dict):
        rid = obj.get("run_id")
        if isinstance(rid, str) and rid:
            found.add(rid)
        rids = obj.get("run_ids")
        if isinstance(rids, list):
            found.update(r for r in rids if isinstance(r, str) and r)
        for value in obj.values():
            _collect_envelope_run_ids(value, found)
    elif isinstance(obj, list):
        for item in obj:
            _collect_envelope_run_ids(item, found)


def _normalize_ids_to_project_prefix(envelope: dict[str, Any], src_pid: str) -> int:
    """Rewrite node ids from ``{run_id}:rest`` to ``{source_project}:rest``.

    The artifact viewer's items query only accepts items whose id starts
    with ``{project_id}:`` (or that carry a project_id property, which the
    import path strips). The backend import remaps a leading
    ``{source_project}:`` segment to the target project, so ids normalised
    to that shape land as ``{target_project}:rest`` and stay visible.
    Run-prefixed ids (from data that was itself imported once) would
    instead be remapped to ``{new_run_id}:rest`` and be filtered out.
    """
    run_ids: set[str] = set()
    _collect_envelope_run_ids(envelope, run_ids)
    prefixes = sorted(run_ids, key=len, reverse=True)
    changed = 0

    def _remap(value: str) -> str:
        nonlocal changed
        for old in prefixes:
            if value.startswith(old + ":"):
                changed += 1
                return src_pid + value[len(old):]
        return value

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ("id", "target_id") and isinstance(value, str):
                    obj[key] = _remap(value)
                else:
                    _walk(value)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    for do in envelope.get("dynamic_objects") or []:
        _walk(do)
    return changed


def _rewrite_envelope_for_target(envelope: dict[str, Any], pid: str, label_map: dict[str, str]) -> int:
    """Stamp target identity into a DO export envelope before importing it.

    Containers get the target's definition_group_id (resolved by node_label)
    and project_id; every nested item/child node gets project_id. Without
    this the imported nodes keep the SOURCE environment's ids and the
    artifact viewer's items query filters them all out.
    """
    def _strip_label_suffix(node: dict[str, Any]) -> None:
        """Rewrite suffixed item labels (routine writes: ``Label_8hex``) to bare.

        The target's items query pins the LOCAL definition's suffix plus the
        bare label; a SOURCE-env suffix matches neither, so routine-produced
        items would be invisible. Only strips when the bare label is a known
        definition, so unrelated underscored labels pass through untouched.
        """
        node_type = node.get("type")
        if isinstance(node_type, str) and "_" in node_type:
            bare, _, suffix = node_type.rpartition("_")
            if bare in label_map and len(suffix) == 8 and all(c in "0123456789abcdef" for c in suffix):
                node["type"] = bare
        for item in node.get("items") or []:
            if isinstance(item, dict):
                _strip_label_suffix(item)
        for child_list in (node.get("children") or {}).values():
            for child in child_list or []:
                if isinstance(child, dict):
                    _strip_label_suffix(child)

    count = 0
    for do in envelope.get("dynamic_objects") or []:
        props = do.get("properties")
        if isinstance(props, dict):
            label = props.get("node_label")
            if label and label in label_map:
                props["definition_group_id"] = label_map[label]
        _stamp_node_tree(do, pid)
        _strip_label_suffix(do)
        count += 1
    # full-graph scope payloads carry flat nodes instead of DO trees.
    for node in envelope.get("nodes") or []:
        if isinstance(node, dict):
            props = node.get("properties")
            if isinstance(props, dict):
                props["project_id"] = pid
                label = props.get("node_label")
                if label and label in label_map and "definition_group_id" in props:
                    props["definition_group_id"] = label_map[label]
            count += 1
    return count


def _needed_labels(types: list[dict[str, Any]]) -> set[str]:
    """Collect every do_node_label the given artifact types bind to."""
    labels: set[str] = set()
    for t in types:
        for entry in t.get("nav_sections") or []:
            if isinstance(entry, dict) and entry.get("do_node_label"):
                labels.add(entry["do_node_label"])
    return labels


def _collect_bundle_definitions(sources: list[Any]) -> dict[str, dict[str, Any]]:
    """Index every DO definition found in the given JSON blobs by node_label.

    Accepts the shapes we may encounter: the definitions-export payload
    ({definitions: [{name, node_label, data}]}), the DO graph-export envelope
    ({definitions: [{node_label, raw_md, ...} | {node_label, system_template}]}),
    or a bare list of either entry kind.
    """
    by_label: dict[str, dict[str, Any]] = {}
    for blob in sources:
        if blob is None:
            continue
        entries = blob.get("definitions") if isinstance(blob, dict) else blob
        for entry in entries or []:
            label = isinstance(entry, dict) and entry.get("node_label")
            if label and label not in by_label:
                by_label[label] = entry
    return by_label


def _ensure_do_definitions(
    client: RhinoClient, pid: str, needed: set[str], defs_by_label: dict[str, dict[str, Any]]
) -> set[str]:
    """Create missing DO definitions in the target; return labels still missing."""
    listed = client.get(f"/dynamic-objects/definitions", params={"project_id": pid})
    known = {d.get("node_label") for d in listed.get("definitions") or []}
    missing = needed - known
    if not missing:
        return set()
    print(f"  target is missing DO definition(s): {', '.join(sorted(missing))}")

    batch = [
        {"name": d["name"], "node_label": d["node_label"], "data": d["data"]}
        for label in sorted(missing)
        if (d := defs_by_label.get(label)) and d.get("data") is not None and d.get("name")
    ]
    if batch:
        resp = client.post_raw(
            "/dynamic-objects/definitions/import", {"project_id": pid, "definitions": batch}
        )
        if resp.status_code < 400:
            created = {d["node_label"] for d in batch}
            print(f"    created {len(created)} definition(s) via definitions/import")
            missing -= created
        else:
            print(f"    definitions/import failed ({resp.status_code}): {resp.text[:300]}")

    for label in sorted(missing):
        entry = defs_by_label.get(label)
        if not entry or not entry.get("raw_md"):
            continue
        resp = client.post_raw(
            "/dynamic-objects/definitions/from-markdown", {"project_id": pid, "raw_md": entry["raw_md"]}
        )
        if resp.status_code < 400:
            print(f"    created definition {label!r} via from-markdown")
            missing.discard(label)
        else:
            print(f"    from-markdown for {label!r} failed ({resp.status_code}): {resp.text[:300]}")
    return missing


def _strip_unknown_sections(types: list[dict[str, Any]], unresolved: set[str]) -> None:
    """Drop nav_sections whose labels can't exist in the target, warning loudly."""
    for t in types:
        sections = t.get("nav_sections") or []
        kept = [s for s in sections if not (isinstance(s, dict) and s.get("do_node_label") in unresolved)]
        if len(kept) != len(sections):
            dropped = [s.get("do_node_label") for s in sections if s not in kept]
            print(f"  WARNING: stripping unresolvable section(s) from type {t.get('key')!r}: "
                  f"{', '.join(map(str, dropped))} -- these sections will not appear in the artifact")
            t["nav_sections"] = kept


_IMPORT_RE = None  # compiled lazily; see _parse_imports


def _parse_imports(code: str) -> list[tuple[str, str]]:
    """Return (full_import_statement, module_specifier) pairs found in *code*."""
    import re
    global _IMPORT_RE  # noqa: PLW0603
    if _IMPORT_RE is None:
        _IMPORT_RE = re.compile(
            r"^import\s+(?:[^;'\"]|\n)*?from\s*[\"']([^\"']+)[\"'];?[^\S\n]*$"
            r"|^import\s*[\"']([^\"']+)[\"'];?[^\S\n]*$",
            re.MULTILINE,
        )
    out = []
    for m in _IMPORT_RE.finditer(code):
        out.append((m.group(0), m.group(1) or m.group(2)))
    return out


def _norm_module_path(importer: str, spec: str) -> str:
    """Resolve a relative import spec against the importer's path."""
    import posixpath
    base = posixpath.dirname(importer)
    resolved = posixpath.normpath(posixpath.join(base, spec))
    return resolved.lstrip("./")


def flatten_component(component_source: str, source_files: list[dict[str, str]]) -> str:
    """Inline 3.0.9 multi-file sandbox modules into one component for 3.0.7 hosts.

    The 3.0.9 sandbox mounts ``component_source`` at /App.jsx and resolves its
    relative imports from ``source_files``; a 3.0.7 host knows neither the
    table nor the bundler feature, so the modules are concatenated instead:
    external imports are hoisted (deduped by exact text), relative imports
    dropped, ``export`` keywords stripped from modules (the entry keeps its
    default export), and modules ordered by their import dependencies.
    """
    import re

    files = {f["path"].lstrip("./"): f["content"] for f in source_files}
    # Map without extension too, so "./utils/data.js" and "./utils/data" both hit.
    def _lookup_key(resolved: str) -> str | None:
        if resolved in files:
            return resolved
        for ext in (".jsx", ".js", ".ts", ".tsx"):
            if resolved + ext in files:
                return resolved + ext
        return None

    # Build dependency edges among source files.
    deps: dict[str, set[str]] = {p: set() for p in files}
    for path, code in files.items():
        for _stmt, spec in _parse_imports(code):
            if spec.startswith("."):
                key = _lookup_key(_norm_module_path(path, spec))
                if key:
                    deps[path].add(key)

    # Topological order (dependencies first); cycles fall back to name order.
    ordered: list[str] = []
    visiting: set[str] = set()

    def _visit(node: str) -> None:
        if node in ordered or node in visiting:
            return
        visiting.add(node)
        for dep in sorted(deps.get(node, ())):
            _visit(dep)
        visiting.discard(node)
        ordered.append(node)

    for path in sorted(files):
        _visit(path)

    # Merge external imports per module specifier: modules each import their
    # own slice of react/primitives, and duplicate default bindings ("React")
    # would be a SyntaxError once inlined into one module scope.
    merged: dict[str, dict[str, Any]] = {}  # spec -> {default, named:set, bare}

    def _merge_import(stmt: str, spec: str) -> None:
        entry_rec = merged.setdefault(spec, {"default": None, "named": set(), "bare": False})
        m = re.match(r"import\s+(.*?)\s*from", stmt, re.DOTALL)
        if not m:
            entry_rec["bare"] = True
            return
        clause = m.group(1).strip()
        named = re.search(r"\{([^}]*)\}", clause, re.DOTALL)
        if named:
            entry_rec["named"].update(n.strip() for n in named.group(1).split(",") if n.strip())
            clause = clause[: named.start()].rstrip(", \n")
        if clause and not clause.startswith("{"):
            entry_rec["default"] = clause.strip(", ")

    bodies: list[str] = []

    def _strip(code: str, *, is_entry: bool) -> str:
        for stmt, spec in _parse_imports(code):
            if not spec.startswith("."):
                _merge_import(stmt, spec)
            code = code.replace(stmt, "")
        if not is_entry:
            # Named exports become plain declarations in the single module scope.
            code = re.sub(r"^export\s+(?=(?:async\s+)?(?:function|const|let|var|class)\b)", "", code, flags=re.MULTILINE)
            code = re.sub(r"^export\s+default\s+", "", code, flags=re.MULTILINE)
            code = re.sub(r"^export\s*\{[^}]*\}\s*;?\s*$", "", code, flags=re.MULTILINE)
        return code.strip()

    for path in ordered:
        bodies.append(f"// ---- inlined from {path} ----\n" + _strip(files[path], is_entry=False))
    entry = _strip(component_source, is_entry=True)

    import_lines: list[str] = []
    for spec in sorted(merged):
        rec = merged[spec]
        parts = []
        if rec["default"]:
            parts.append(rec["default"])
        if rec["named"]:
            parts.append("{ " + ", ".join(sorted(rec["named"])) + " }")
        if parts:
            import_lines.append(f'import {", ".join(parts)} from "{spec}";')
        elif rec["bare"]:
            import_lines.append(f'import "{spec}";')

    return "\n".join(import_lines) + "\n\n" + "\n\n".join(bodies) + "\n\n// ---- entry ----\n" + entry


def shim_pull_data_component(component: str) -> tuple[str, list[dict[str, Any]]]:
    """Port a 3.0.9 pull-based sandbox component to the 3.0.7 push contract.

    3.0.9 components call ``useDoItems("Label", {limit})`` (a primitive that
    pulls items over a request bridge); 3.0.7 has neither the primitive nor
    the bridge — it pre-fetches items for the type's ``nav_sections`` into
    ``ctx.do_items[label]``. This rewrites the component to that contract:

    - collects every ``useDoItems`` label/limit and returns matching
      ``nav_sections`` entries so the host prefetches them,
    - removes ``useDoItems`` from the primitives import (3.0.7's validator
      rejects unknown primitives),
    - injects a local ``useDoItems`` returning ``{items, error, loading}``
      from ``ctx.do_items``, with the ctx captured by wrapping the entry.
    """
    import re

    calls: dict[str, int | None] = {}
    for m in re.finditer(r"\buseDoItems\s*\(\s*([\"'])([^\"'\n]+)\1(?:\s*,\s*\{[^}]*?limit\s*:\s*(\d+))?", component):
        label, limit = m.group(2), (int(m.group(3)) if m.group(3) else None)
        prev = calls.get(label)
        calls[label] = max(prev or 0, limit or 0) or None
    if not calls:
        return component, []

    def _drop_use_do_items(mm: Any) -> str:
        names = [n.strip() for n in mm.group(1).split(",") if n.strip() and n.strip() != "useDoItems"]
        return f'import {{ {", ".join(names)} }} from "@artifact/primitives";' if names else ""

    component = re.sub(
        r"import\s*\{([^}]*)\}\s*from\s*[\"']@artifact/primitives[\"'];?",
        _drop_use_do_items, component, count=1,
    )

    entry_match = re.search(r"export\s+default\s+function\s+\w*\s*\(", component)
    if entry_match:
        component = component.replace(entry_match.group(0), "function __UserEntry(", 1)
        component += (
            "\n\nexport default function App({ ctx }) {\n"
            "  __sandboxCtx = ctx;\n"
            "  return React.createElement(__UserEntry, { ctx });\n"
            "}\n"
        )
    else:
        print("  WARNING: could not locate the entry's default export to capture ctx; "
              "useDoItems shim will see no data")

    shim = (
        "// ---- 3.0.7 compatibility shim: useDoItems reads host-prefetched ctx.do_items ----\n"
        "let __sandboxCtx = null;\n"
        "function useDoItems(label) {\n"
        "  const items = (__sandboxCtx && __sandboxCtx.do_items && __sandboxCtx.do_items[label]) || [];\n"
        "  return { items, error: null, loading: false };\n"
        "}\n\n"
    )
    nav_sections = [
        {"id": lbl, "display_name": lbl, "do_node_label": lbl, **({"limit": lim} if lim else {})}
        for lbl, lim in sorted(calls.items())
    ]
    return shim + component, nav_sections


def _create_type_payload(source: dict[str, Any], member_id_map: dict[str, str]) -> dict[str, Any]:
    """Build the POST /artifact-types payload from an exported type dict.

    Target scope is implicitly CUSTOM (the create endpoint has no scope field),
    which also covers 3.0.8 LIBRARY types imported into a 3.0.7 environment.

    3.0.9 multi-file react_sandbox types (``source_files``) are flattened into
    a single ``component_source``, since older hosts cannot store or bundle
    the extra modules.
    """
    component = source.get("component_source") or ""
    source_files = source.get("source_files") or []
    nav_sections = source.get("nav_sections") or []
    if component and source_files:
        print(f"  flattening {len(source_files)} source file(s) into component_source "
              "(target host predates multi-file sandbox types)")
        component = flatten_component(component, source_files)
    if component and "useDoItems" in component:
        component, synthesized = shim_pull_data_component(component)
        if synthesized and not nav_sections:
            nav_sections = synthesized
            print(f"  shimmed useDoItems + synthesized {len(synthesized)} nav_section(s) "
                  f"({', '.join(s['do_node_label'] for s in synthesized[:6])}...)")
    return {
        "key": source["key"],
        "name": source["name"],
        "template": source.get("template") or "",
        "nav_template": source.get("nav_template"),
        "nav_sections": nav_sections,
        "routine_names": source.get("routine_names") or [],
        "render_mode": source.get("render_mode") or "nunjucks",
        "component_source": component,
        "member_type_ids": [member_id_map.get(m, m) for m in (source.get("member_type_ids") or [])],
        "nav_layout": source.get("nav_layout") or "top",
        "lifecycle_status": source.get("lifecycle_status") or "published",
        "preview_sample_items": source.get("preview_sample_items"),
    }


def _ensure_type(client: RhinoClient, pid: str, source: dict[str, Any],
                 existing_by_key: dict[str, dict[str, Any]], member_id_map: dict[str, str],
                 update: bool = False) -> str:
    """Return the target-environment id for the given source type, creating it if needed.

    With ``update=True`` an existing non-SYSTEM type is PATCHed with the
    bundle's content (template/component_source/nav_sections/...), so a
    re-import refreshes the rendering instead of silently keeping the old one.
    """
    key = source["key"]
    if key in existing_by_key:
        found = existing_by_key[key]
        if update and found.get("scope") == "SYSTEM":
            print(f"  type {key!r} is SYSTEM in target; cannot update, reusing as-is")
        elif update:
            print(f"  updating existing type {key!r} ({found['id']}) from bundle ...")
            payload = _create_type_payload(source, member_id_map)
            payload.pop("key", None)  # key is immutable; PATCH rejects unknown handling
            url = f"{client.api}/projects/{pid}/artifact-types/{found['id']}"
            resp = client.session.patch(url, json=payload, timeout=DEFAULT_TIMEOUT)
            if resp.status_code == 400 and "routine" in resp.text.lower():
                print(f"    unknown routine name(s) ({resp.text[:150]}); retrying with routine_names=[]")
                payload["routine_names"] = []
                resp = client.session.patch(url, json=payload, timeout=DEFAULT_TIMEOUT)
            if resp.status_code >= 400:
                print(f"    WARNING: type update failed ({resp.status_code}): {resp.text[:300]}")
            else:
                print("    type updated")
        else:
            print(f"  type {key!r} already exists in target ({found.get('scope')}), reusing id {found['id']}")
        return found["id"]
    print(f"  creating type {key!r} (render_mode={source.get('render_mode')}) ...")
    payload = _create_type_payload(source, member_id_map)
    resp = client.post_raw(f"/projects/{pid}/artifact-types", payload)
    if resp.status_code == 400 and "routine" in resp.text.lower():
        # 3.0.8 routine names may not exist here; they only matter for
        # regeneration, not viewing, so retry without them.
        print(f"    unknown routine name(s) in target ({resp.text[:200]}); retrying with routine_names=[]")
        payload["routine_names"] = []
        resp = client.post_raw(f"/projects/{pid}/artifact-types", payload)
    if resp.status_code >= 400:
        sys.exit(f"POST /projects/{pid}/artifact-types failed ({resp.status_code}): {resp.text[:500]}")
    created = resp.json()
    print(f"    created as {created['id']} (scope={created.get('scope')})")
    return created["id"]


def cmd_import(args: argparse.Namespace) -> None:
    client = RhinoClient(args.base_url, resolve_token(args), verify_tls=not args.insecure)
    pid = args.project_id

    with zipfile.ZipFile(args.bundle) as zf:
        names = set(zf.namelist())
        manifest = json.loads(zf.read(MANIFEST_JSON))
        artifact = json.loads(zf.read(ARTIFACT_JSON))
        artifact_type = json.loads(zf.read(TYPE_JSON)) if TYPE_JSON in names else None
        member_types = [
            json.loads(zf.read(n)) for n in sorted(names) if n.startswith(MEMBER_DIR) and n.endswith(".json")
        ]
        dos_envelope = json.loads(zf.read(DOS_JSON)) if DOS_JSON in names else None
        bundle_do_defs = json.loads(zf.read(DO_DEFS_JSON)) if DO_DEFS_JSON in names else None

    type_key = artifact.get("artifact_type") or manifest.get("artifact_type_key")
    print(f"Bundle: artifact {artifact.get('name')!r} type={type_key!r} "
          f"(exported {manifest.get('exported_at')} from {manifest.get('source_base_url')})")

    # 1. Ensure DO definitions exist BEFORE the DO import: the envelope rewrite
    #    needs the target's definition_group_ids, and the type validator needs
    #    the nav_section labels. Sourced from the bundle (do_definitions.json /
    #    DO envelope) or --definitions-file.
    listed = client.get(f"/projects/{pid}/artifact-types?include_drafts=true")
    existing_by_key = {t["key"]: t for t in listed}
    types_to_create = (
        [
            t for t in [*member_types, artifact_type]
            # --update-type PATCHes existing types too, so their labels also
            # need definitions ensured (and unresolved sections stripped).
            if t.get("key") not in existing_by_key or args.update_type
        ]
        if artifact_type is not None and artifact_type.get("scope") != "SYSTEM"
        else []
    )
    needed = _needed_labels(types_to_create)
    if args.import_dos and dos_envelope is not None:
        for do in dos_envelope.get("dynamic_objects") or []:
            label = (do.get("properties") or {}).get("node_label")
            if label:
                needed.add(label)
    if needed:
        extra_defs = None
        if args.definitions_file:
            with open(args.definitions_file, encoding="utf-8") as fh:
                extra_defs = json.load(fh)
        defs_by_label = _collect_bundle_definitions([bundle_do_defs, dos_envelope, extra_defs])
        unresolved = _ensure_do_definitions(client, pid, needed, defs_by_label)
        if unresolved:
            _strip_unknown_sections(types_to_create, unresolved)

    # 2. Import the bundled DOs, rewriting the envelope so every node lands
    #    with the TARGET project's identity (project_id + definition_group_id).
    #    This makes the post-import graph repair (--align) unnecessary --
    #    essential when the backend/DB cannot be reached outside the API.
    if args.import_dos:
        if dos_envelope is None:
            sys.exit("--import-dos given but the bundle has no dynamic_objects.json")
        if args.replace_dos:
            _delete_imported_dos(client, pid)
        label_map = _target_label_map(client, pid)
        rewritten = _rewrite_envelope_for_target(dos_envelope, pid, label_map)
        src_pid = dos_envelope.get("project_id") or manifest.get("source_project_id")
        if src_pid:
            normalized = _normalize_ids_to_project_prefix(dos_envelope, str(src_pid))
            print(f"Rewrote {rewritten} DO tree(s); normalised {normalized} run-prefixed id(s) "
                  "so imported items stay visible to the artifact viewer")
        else:
            print(f"Rewrote {rewritten} DO tree(s); WARNING: no source project id in envelope, "
                  "skipping id normalisation")
        print("Importing Dynamic Objects ...")
        summary = client.import_dynamic_objects(pid, dos_envelope)
        print(f"  import summary: {json.dumps(summary)[:300]}")
    elif dos_envelope is not None:
        print("Bundle contains Dynamic Objects; skipping (pass --import-dos to load them)")

    # 3. Ensure the artifact type exists in the target.
    if artifact_type is not None and artifact_type.get("scope") != "SYSTEM":
        member_id_map: dict[str, str] = {}
        for member in member_types:  # members first so the composite can reference them
            member_id_map[member["id"]] = _ensure_type(
                client, pid, member, existing_by_key, {}, update=args.update_type
            )
        _ensure_type(client, pid, artifact_type, existing_by_key, member_id_map, update=args.update_type)
    elif artifact_type is not None:
        if type_key not in existing_by_key:
            print(f"  WARNING: SYSTEM type {type_key!r} not seeded in target -- "
                  "artifact will render via the frontend fallback")
    elif type_key not in existing_by_key:
        print(f"  WARNING: bundle has no type definition and target has no type {type_key!r}")

    # 3. Create (or reuse) the artifact record and mark it complete. Reuse on
    #    an exact name+type match keeps repeated imports/updates idempotent.
    name = args.name or artifact.get("name") or type_key
    existing_artifacts = client.get(f"/projects/{pid}/artifacts")
    match = next(
        (a for a in existing_artifacts if a.get("name") == name and a.get("artifact_type") == type_key),
        None,
    )
    if match:
        artifact_id = match["id"]
        print(f"Artifact {name!r} already exists ({artifact_id}); reusing it")
    else:
        print(f"Creating artifact {name!r} ...")
        created = client.post(f"/projects/{pid}/artifacts", {"artifact_type": type_key, "name": name})
        artifact_id = created["id"]
    client.patch(f"/projects/{pid}/artifacts/{artifact_id}", {"status": "complete"})

    # 4. Graph repair -- only needed when the DOs were imported OUTSIDE this
    #    script (e.g. via the UI), which leaves source-env identities on the
    #    nodes. A script-side --import-dos already rewrote the envelope, so
    #    imported data lands correct and no repair is required.
    if args.align:
        if args.align_psql:
            _run_graph_alignment_sql(args.align_psql, pid, args.age_graph)
        else:
            _run_graph_alignment(args.align_exec, pid)
    elif not args.import_dos:
        print("\nNOTE: if the DOs were imported via the UI and the artifact renders empty, "
              "run again with --align (backend exec) or --align --align-psql (psql) to "
              "re-stamp the imported graph nodes.")

    ui_base = args.base_url.replace(":9080", ":3000") if args.ui_base_url is None else args.ui_base_url
    print(f"\nDone. Artifact created: {artifact_id}")
    print(f"  {ui_base.rstrip('/')}/digital-transformer/projects/{pid}/artifacts/{artifact_id}")


def cmd_poll(args: argparse.Namespace) -> None:
    """Re-attach to a graph I/O background task after a dropped connection."""
    client = RhinoClient(args.base_url, resolve_token(args), verify_tls=not args.insecure)
    result = client._wait_io_task(args.project_id, args.task_id, "task")  # noqa: SLF001
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(result, fh)
        print(f"Result written to {args.out}")
    else:
        print(f"Result: {json.dumps(result)[:1000]}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--base-url", required=True, help="e.g. http://localhost:9080 or https://<env>.rhinoapi.com")
    p.add_argument("--project-id", required=True)
    p.add_argument("--token", help="Bearer token (else RHINO_TOKEN, else username/password login)")
    p.add_argument("--username", help="Login username (else RHINO_USERNAME)")
    p.add_argument("--password", help="Login password (else RHINO_PASSWORD, else interactive prompt)")
    p.add_argument("--insecure", action="store_true", help="Skip TLS certificate verification")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export", help="Fetch artifact + type (+ DOs) from a source env into a zip bundle")
    _add_common(exp)
    exp.add_argument("--artifact-id", required=True)
    exp.add_argument("--out", default="artifact_bundle.zip", help="Output zip path")
    exp.add_argument("--export-dos", action="store_true",
                     help="Also run the Dynamic Object graph export via the API and bundle the envelope")
    exp.add_argument("--dos-file", help="Bundle an already-downloaded DO export JSON instead of exporting")
    exp.add_argument("--run-id", help="Limit --export-dos to one extraction run id")
    exp.add_argument("--do-ids", help="Comma-separated DO ids to limit --export-dos")
    exp.add_argument("--scope", help="DO export scope passthrough (e.g. extraction, full_graph)")
    exp.set_defaults(func=cmd_export)

    imp = sub.add_parser("import", help="Recreate artifact type + record (+ DOs) in a target env from a bundle")
    _add_common(imp)
    imp.add_argument("--bundle", default="artifact_bundle.zip", help="Bundle zip produced by 'export'")
    imp.add_argument("--import-dos", action="store_true", help="Also import the bundled Dynamic Objects")
    imp.add_argument("--replace-dos", action="store_true",
                     help="With --import-dos: delete previously imported DOs first so the re-import "
                          "replaces them (otherwise re-import merges in place and source-side "
                          "deletions linger)")
    imp.add_argument("--update-type", action="store_true",
                     help="PATCH an existing artifact type with the bundle's template/component/"
                          "nav_sections instead of reusing it unchanged (use when refreshing "
                          "an artifact to a newer version)")
    imp.add_argument("--definitions-file",
                     help="DO export JSON (e.g. downloaded from the Graph Explorer Export modal) to "
                          "source missing DO definitions from, in addition to the bundle")
    imp.add_argument("--name", help="Override the artifact display name")
    imp.add_argument("--align", action="store_true",
                     help="Re-stamp graph containers/items imported OUTSIDE this script (e.g. via "
                          "the UI) with the target's definition_group_id/project_id. Not needed "
                          "with --import-dos, which rewrites the payload before upload.")
    imp.add_argument("--align-exec", default="docker exec digitaltransformerbackend-api-1",
                     help="Exec prefix used with --align, e.g. 'docker exec <container>' or "
                          "'kubectl exec -n <ns> <pod> --' (default: %(default)s)")
    imp.add_argument("--align-psql",
                     help="Use SQL mode for --align instead of backend exec: a full psql invocation, "
                          "e.g. 'docker exec <pg-container> psql -U admin -d rhino' or "
                          "'kubectl exec -n <ns> <pg-pod> -- psql -U rhino_rw -d rhino'")
    imp.add_argument("--age-graph", default="rhino",
                     help="AGE graph name for --align-psql (default: %(default)s)")
    imp.add_argument("--ui-base-url", default=None,
                     help="Frontend base URL for the printed link (default: guess from --base-url)")
    imp.set_defaults(func=cmd_import)

    pol = sub.add_parser("poll", help="Re-attach to a graph I/O task (after a dropped connection)")
    _add_common(pol)
    pol.add_argument("--task-id", required=True, help="Task id printed when the task was enqueued")
    pol.add_argument("--out", help="Write the task result JSON to this file (useful for export tasks)")
    pol.set_defaults(func=cmd_poll)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
