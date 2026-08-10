#!/usr/bin/env python3
"""Import a Dynamic Objects export file back into a Rhino project.

Drives the Graph Explorer's async import machinery:

    POST /projects/<pid>/graph/import/dynamic-objects    (Accept: text/event-stream -> 202 {task_id})
    GET  /projects/<pid>/graph/io-tasks/<task_id>/result (202 while running, 200 with summary when done)

Accepts the envelope produced by ``GET /graph/export/dynamic-objects``
(e.g. ``DO_graph_export_<project>.json``): either the per-DO shape
(``{"dynamic_objects": [...]}``) or the full-graph shape
(``{"scope": "full_graph", "nodes": [...], "edges": [...]}``).

Batching: the enqueue path stores the payload in Redis and the worker
reads it back — very large single payloads can time out that read (seen
locally at ~300MB), and gateway deployments commonly cap request bodies
at 50M. Per-DO envelopes are therefore split by SERIALIZED SIZE
(``--max-batch-mb``, default 50): whole DOs are packed into batches, and
a DO bigger than the cap is itself split by chunking its ``items`` array
across several payloads (safe: the importer MERGEs nodes by id, so the
repeated DO container is idempotent). ``--batch-size`` additionally caps
the DO count per batch. Full-graph envelopes cannot be batched.

Auth: ``--token`` (browser localStorage ``accessToken``) or ``--username``
(password prompted; auto-refreshes on 401 — needed for 5-minute prod tokens).

Examples:
    python import_dynamic_objects.py DO_graph_export_<pid>.json \
        --base-url http://localhost:9080 --direct --username admin \
        --project-id <local-project-uuid> --exclude "App Overview" --verbose

    python import_dynamic_objects.py --cancel-task <task-id> \
        --base-url http://localhost:9080 --direct --username admin --project-id <uuid> dummy.json

Requires: pip install requests
"""
from __future__ import annotations

import argparse
import getpass
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

VERBOSE = False


def vlog(msg: str) -> None:
    if VERBOSE:
        print(f"[debug] {msg}")


class RhinoClient:
    """Minimal authenticated client with refresh renewal and safe retries.

    Connection-level failures (aborts, resets, timeouts) are retried with
    backoff for idempotent methods (GET/DELETE) only — an import-enqueue
    POST is never blind-retried because a retry after a post-processing
    abort would double-enqueue the task.
    """

    def __init__(self, base_url: str, api_prefix: str = "/api", verify: bool = True):
        self.base_url = base_url.rstrip("/") + api_prefix.rstrip("/")
        self.session = requests.Session()
        self.session.verify = verify
        self.access_token: str | None = None
        self.refresh_token: str | None = None

    def login(self, username: str, password: str) -> None:
        r = self.session.post(
            f"{self.base_url}/login",
            json={"username": username, "password": password},
            timeout=60,
        )
        r.raise_for_status()
        body = r.json()
        self.access_token = body["access_token"]
        self.refresh_token = body.get("refresh_token")
        print("[auth] logged in")

    def refresh(self) -> bool:
        if not self.refresh_token:
            return False
        r = self.session.post(
            f"{self.base_url}/login/refresh",
            headers={"Authorization": f"Bearer {self.refresh_token}"},
            timeout=60,
        )
        if r.status_code != 200:
            print(f"[auth] refresh failed: HTTP {r.status_code}")
            return False
        self.access_token = r.json()["access_token"]
        print("[auth] access token refreshed")
        return True

    def request(self, method: str, path: str, *, retry_auth: bool = True, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.access_token}"
        retriable = method.upper() in ("GET", "DELETE")
        attempts = 5 if retriable else 1
        last_exc: Exception | None = None
        for attempt in range(attempts):
            started = time.monotonic()
            try:
                r = self.session.request(
                    method, f"{self.base_url}{path}", headers=headers, timeout=600, **kwargs,
                )
                vlog(f"{method} {path} -> {r.status_code} in {time.monotonic() - started:.1f}s")
                if r.status_code == 401 and retry_auth and self.refresh():
                    return self.request(method, path, retry_auth=False, headers=headers, **kwargs)
                return r
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt == attempts - 1:
                    break
                wait = 3 * (attempt + 1)
                print(f"[net] {method} {path} connection error ({type(exc).__name__}); "
                      f"retry {attempt + 1}/{attempts - 1} in {wait}s...")
                time.sleep(wait)
        assert last_exc is not None
        raise last_exc


def enqueue_import(client: RhinoClient, project_id: str, payload_json: str) -> str:
    """POST the serialized import payload; returns the task id. Waits out 429s."""
    path = f"/projects/{project_id}/graph/import/dynamic-objects"
    size_mb = len(payload_json) / 1048576
    print(f"[import] POSTing payload ({size_mb:.1f} MB)...")
    while True:
        r = client.request(
            "POST", path,
            headers={"Accept": "text/event-stream", "Content-Type": "application/json"},
            data=payload_json.encode("utf-8"),
        )
        if r.status_code == 202:
            task_id = r.json()["task_id"]
            print(f"[import] task enqueued: {task_id}")
            return task_id
        if r.status_code == 429:
            print("[import] concurrency limit — another I/O task is running; retrying in 15s...")
            time.sleep(15)
            continue
        if r.status_code == 413:
            sys.exit("[import] HTTP 413: payload too large for the deployment's upload limit. "
                     "Lower --max-batch-mb and re-run.")
        sys.exit(f"[import] enqueue failed: HTTP {r.status_code}: {r.text[:500]}")


def wait_for_result(client: RhinoClient, project_id: str, task_id: str, poll_seconds: float) -> dict[str, Any]:
    """Poll the result endpoint until the task completes; returns the summary."""
    path = f"/projects/{project_id}/graph/io-tasks/{task_id}/result"
    started = time.monotonic()
    while True:
        try:
            r = client.request("GET", path)
        except (requests.ConnectionError, requests.Timeout) as exc:
            print(f"\n[import] poll connection error after retries ({type(exc).__name__}); "
                  f"continuing to poll in {poll_seconds}s...")
            time.sleep(poll_seconds)
            continue
        if r.status_code == 200:
            print(f"\n[import] task {task_id} finished after {int(time.monotonic() - started)}s")
            return r.json()
        if r.status_code == 202:
            body = {}
            try:
                body = r.json()
            except ValueError:
                pass
            status = body.get("status", "running")
            elapsed = int(time.monotonic() - started)
            print(f"[import] {status}... ({elapsed}s)", end="\r", flush=True)
            time.sleep(poll_seconds)
            continue
        if r.status_code in (502, 503, 504):
            print(f"\n[import] transient gateway {r.status_code}; retrying...")
            time.sleep(poll_seconds)
            continue
        if r.status_code == 404:
            sys.exit(f"\n[import] task {task_id} not found — it may have failed or expired. {r.text[:300]}")
        sys.exit(f"\n[import] result poll failed: HTTP {r.status_code}: {r.text[:500]}")


def build_batches(envelope: dict[str, Any], max_mb: float, count_cap: int) -> list[str]:
    """Pack DOs into serialized payloads bounded by size (and optionally count).

    Returns pre-serialized JSON strings so sizes are exact. Oversized DOs
    are split by chunking their ``items`` array; the DO container fields
    repeat in every chunk, which the importer's MERGE-by-id semantics
    make idempotent.
    """
    dos = envelope.get("dynamic_objects")
    if not isinstance(dos, list):
        sys.exit("Batching requires a per-DO envelope with a 'dynamic_objects' array")
    shared = {k: v for k, v in envelope.items() if k != "dynamic_objects"}
    shared_json = json.dumps(shared)
    overhead = len(shared_json) + len(', "dynamic_objects": []')
    max_bytes = int(max_mb * 1024 * 1024)

    def serialize(batch_dos: list[str]) -> str:
        return shared_json[:-1] + ', "dynamic_objects": [' + ", ".join(batch_dos) + "]}"

    def do_display(do: dict[str, Any]) -> str:
        return str((do.get("properties") or {}).get("name") or do.get("id") or "?")

    payloads: list[str] = []
    current: list[str] = []
    current_bytes = overhead

    def flush() -> None:
        nonlocal current, current_bytes
        if current:
            payloads.append(serialize(current))
            current, current_bytes = [], overhead

    for do in dos:
        do_json = json.dumps(do)
        if overhead + len(do_json) > max_bytes:
            # Single DO exceeds the cap: emit current batch, then chunk this DO's items.
            flush()
            items = do.get("items") or []
            base = {k: v for k, v in do.items() if k != "items"}
            base_json_len = len(json.dumps({**base, "items": []}))
            chunk: list[dict[str, Any]] = []
            chunk_bytes = overhead + base_json_len
            n_chunks = 0
            for item in items:
                item_len = len(json.dumps(item))
                if chunk and chunk_bytes + item_len > max_bytes:
                    payloads.append(serialize([json.dumps({**base, "items": chunk})]))
                    n_chunks += 1
                    chunk, chunk_bytes = [], overhead + base_json_len
                chunk.append(item)
                chunk_bytes += item_len + 2
            if chunk:
                payloads.append(serialize([json.dumps({**base, "items": chunk})]))
                n_chunks += 1
            print(f"[batch] '{do_display(do)}' is {len(do_json) / 1048576:.1f} MB "
                  f"({len(items)} items) — split into {n_chunks} item-chunk payloads")
            continue
        if current and (current_bytes + len(do_json) > max_bytes or (count_cap and len(current) >= count_cap)):
            flush()
        current.append(do_json)
        current_bytes += len(do_json) + 2
    flush()
    return payloads


def main() -> None:
    global VERBOSE  # noqa: PLW0603
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("export_file", type=Path, help="DO export JSON (per-DO or full-graph envelope)")
    parser.add_argument("--base-url", required=True,
                        help="e.g. https://stable-max-claude-sonnet.stg.rhinoapi.com, or http://localhost:9080 "
                             "for a direct backend connection")
    parser.add_argument("--api-prefix", default="/api",
                        help="Default '/api' (through the frontend nginx); '' for direct backend access")
    parser.add_argument("--direct", action="store_true", help="Shortcut for --api-prefix ''")
    parser.add_argument("--project-id", help="Target project UUID (default: filename UUID, then envelope project_id)")
    parser.add_argument("--token", help="Bearer access token (browser localStorage 'accessToken')")
    parser.add_argument("--username", help="Log in with this username instead of --token (password prompted)")
    parser.add_argument("--max-batch-mb", type=float, default=50.0,
                        help="Max serialized payload size per import task in MB (default 50). "
                             "Oversized DOs are split by chunking their items")
    parser.add_argument("--batch-size", type=int, default=0,
                        help="Additional cap on DOs per batch (0 = size-based only)")
    parser.add_argument("--exclude", action="append", default=[],
                        help="DO name to skip (case-insensitive; repeatable or comma-separated)")
    parser.add_argument("--include", action="append", default=[],
                        help="Import ONLY these DO names (case-insensitive; repeatable or comma-separated)")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Result poll interval in seconds")
    parser.add_argument("--cancel-task", help="Cancel this io-task id and exit (export_file is ignored)")
    parser.add_argument("--verbose", action="store_true", help="Log every request with status and timing")
    parser.add_argument("--insecure", action="store_true", help="Skip TLS verification (intercepting proxies)")
    args = parser.parse_args()
    VERBOSE = args.verbose

    client = RhinoClient(args.base_url, api_prefix="" if args.direct else args.api_prefix,
                         verify=not args.insecure)
    if args.insecure:
        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
    if args.token:
        client.access_token = args.token
    elif args.username:
        client.login(args.username, getpass.getpass(f"Password for {args.username}: "))
    else:
        sys.exit("Provide --token or --username")

    if args.cancel_task:
        if not args.project_id:
            sys.exit("--cancel-task requires --project-id")
        r = client.request("DELETE", f"/projects/{args.project_id}/graph/io-tasks/{args.cancel_task}")
        print(f"[cancel] HTTP {r.status_code}: {r.text[:300]}")
        return

    project_id = args.project_id
    if not project_id:
        m = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", args.export_file.name)
        if m:
            project_id = m.group(0)
            print(f"[import] target project (from filename): {project_id}")

    print(f"[import] loading {args.export_file} ...")
    envelope = json.loads(args.export_file.read_text(encoding="utf-8"))
    if not project_id:
        project_id = envelope.get("project_id")
        if not project_id:
            sys.exit("--project-id is required (no UUID in the filename and none in the envelope)")
        print(f"[import] target project (from envelope): {project_id}")

    is_full_graph = envelope.get("scope") == "full_graph" and "nodes" in envelope and "edges" in envelope
    n_dos = len(envelope.get("dynamic_objects") or [])
    print(f"[import] envelope: {'full-graph' if is_full_graph else f'per-DO ({n_dos} dynamic_objects)'}, "
          f"{args.export_file.stat().st_size / 1048576:.1f} MB on disk")

    def _name_set(values: list[str]) -> set[str]:
        return {name.strip().lower() for v in values for name in v.split(",") if name.strip()}

    exclude, include = _name_set(args.exclude), _name_set(args.include)
    if (exclude or include) and is_full_graph:
        sys.exit("--exclude/--include require a per-DO envelope")
    if exclude or include:
        def do_name(do: dict[str, Any]) -> str:
            return str((do.get("properties") or {}).get("name") or "").lower()

        before = envelope["dynamic_objects"]
        kept = [do for do in before
                if (not include or do_name(do) in include) and do_name(do) not in exclude]
        dropped = sorted({(do.get("properties") or {}).get("name") or "?" for do in before} -
                         {(do.get("properties") or {}).get("name") or "?" for do in kept})
        unmatched = (exclude | include) - {do_name(do) for do in before}
        if unmatched:
            print(f"[import] WARNING: no DO matched these names: {sorted(unmatched)}")
        envelope["dynamic_objects"] = kept
        print(f"[import] filtered {len(before)} -> {len(kept)} dynamic_objects"
              + (f" (dropped: {dropped})" if dropped else ""))
        if not kept:
            sys.exit("[import] nothing left to import after filtering")

    if is_full_graph:
        payloads = [json.dumps(envelope)]
        print(f"[import] full-graph payload: {len(payloads[0]) / 1048576:.1f} MB (cannot be batched)")
    else:
        payloads = build_batches(envelope, args.max_batch_mb, args.batch_size)
        total_mb = sum(len(p) for p in payloads) / 1048576
        print(f"[import] {len(payloads)} payload(s), {total_mb:.1f} MB total, "
              f"max {max(len(p) for p in payloads) / 1048576:.1f} MB each (cap {args.max_batch_mb} MB)")

    summaries = []
    for i, payload_json in enumerate(payloads, 1):
        print(f"[import] === payload {i}/{len(payloads)} ===")
        task_id = enqueue_import(client, project_id, payload_json)
        summary = wait_for_result(client, project_id, task_id, args.poll_interval)
        print(f"[import] result: {json.dumps(summary, indent=2)[:1200]}")
        summaries.append(summary)

    print(f"\n[import] DONE — {len(summaries)} task(s) completed against project {project_id}")


if __name__ == "__main__":
    main()
