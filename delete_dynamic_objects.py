#!/usr/bin/env python3
"""List and bulk-delete Dynamic Object instances via the Graph Explorer API.

Backend surfaces (project_graph/graph_io_routes.py):

    POST   /projects/<pid>/graph/do-instances   -> per-run / per-type instance counts
    DELETE /projects/<pid>/graph/do-instances   -> {"spec": {...}, "expected_total": N}

The DELETE is guarded by a count check: if ``expected_total`` doesn't
match the number of instances the spec resolves to, the server answers
409 with the actual count. This script leans on that as a built-in dry
run: it first sends ``expected_total=0``, reads the actual count from
the 409, shows it, and only after confirmation re-sends the delete with
the right total. ``--yes`` skips the prompt.

Selection filters (combined additively — an instance matching ANY
filter is selected; see DOSelectionSpec):
    --run-id       extraction run id (repeatable)
    --node-label   graph node label, e.g. AppOverview_00000000 (repeatable)
    --name         instance name (repeatable)
    --search       free-text search query
    --exclude-id   instance id to spare from the selection (repeatable)

With no filters the script just lists what exists (safe default).

Requires the ``graph_db`` feature to be enabled on the target backend —
a 403 "write operations are disabled" means that flag is off.

Examples:
    # See what's there
    python delete_dynamic_objects.py --base-url http://localhost:9080 --direct \
        --project-id f91ba545-adbb-4b27-b34f-c9f2b17d5563 --username admin

    # Delete everything from one extraction run (confirmed interactively)
    python delete_dynamic_objects.py --base-url http://localhost:9080 --direct \
        --project-id f91ba545-... --username admin \
        --run-id "routine:sys-app-overview:...:2e6cf6141a12"

Requires: pip install requests
"""
from __future__ import annotations

import argparse
import getpass
import json
import sys
from typing import Any

import requests


class RhinoClient:
    """Minimal authenticated client with optional refresh-token renewal."""

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
            return False
        self.access_token = r.json()["access_token"]
        return True

    def request(self, method: str, path: str, *, retry_auth: bool = True, **kwargs) -> requests.Response:
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.access_token}"
        r = self.session.request(method, f"{self.base_url}{path}", headers=headers, timeout=600, **kwargs)
        if r.status_code == 401 and retry_auth and self.refresh():
            return self.request(method, path, retry_auth=False, headers=headers, **kwargs)
        return r


def list_instances(client: RhinoClient, project_id: str) -> None:
    r = client.request("POST", f"/projects/{project_id}/graph/do-instances", json={})
    if r.status_code != 200:
        sys.exit(f"[list] failed: HTTP {r.status_code}: {r.text[:500]}")
    body = r.json()
    print(json.dumps(body, indent=2)[:8000])
    print("\n[list] use --run-id / --node-label / --name / --search to select instances for deletion")


def _norm(value: str) -> str:
    """Lowercase and strip non-alphanumerics so 'App Overview' matches 'AppOverview_00000000'."""
    return "".join(ch for ch in value.lower() if ch.isalnum())


def build_all_except_pairs(client: RhinoClient, project_id: str, excludes: list[str]) -> list[dict[str, str]]:
    """Return (run_id, node_label) pairs for every DO type EXCEPT the excluded names.

    The selection spec has no label-level subtraction, so the complement
    is computed client-side from the instance listing: every grouping
    whose display name or node label matches an exclude token (normalized
    substring) is dropped, and the rest become ``run_type_pairs``.
    """
    r = client.request("POST", f"/projects/{project_id}/graph/do-instances", json={})
    if r.status_code != 200:
        sys.exit(f"[all-except] listing failed: HTTP {r.status_code}: {r.text[:400]}")
    tokens = [_norm(e) for e in excludes if e.strip()]
    pairs: list[dict[str, str]] = []
    kept_labels: set[str] = set()
    dropped_labels: set[str] = set()
    for extraction in r.json().get("extractions", []):
        run_id = extraction.get("run_id")
        groups = extraction.get("dynamic_objects") or {}
        entries = groups.values() if isinstance(groups, dict) else groups
        for group in entries:
            label = str(group.get("node_label") or "")
            display = str(group.get("do_name") or group.get("name") or label)
            if not run_id or not label:
                continue
            if any(t in _norm(display) or t in _norm(label) for t in tokens):
                dropped_labels.add(display)
                continue
            pairs.append({"run_id": run_id, "node_label": label})
            kept_labels.add(display)
    print(f"[all-except] excluded types: {sorted(dropped_labels) or '(none matched!)'}")
    print(f"[all-except] selecting {len(pairs)} (run, type) pairs across: {sorted(kept_labels)}")
    return pairs


def attempt_delete(client: RhinoClient, project_id: str, spec: dict[str, Any], expected_total: int) -> requests.Response:
    return client.request(
        "DELETE",
        f"/projects/{project_id}/graph/do-instances",
        json={"spec": spec, "expected_total": expected_total},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--api-prefix", default="/api",
                        help="Default '/api' (via frontend nginx); '' for direct backend access")
    parser.add_argument("--direct", action="store_true", help="Shortcut for --api-prefix ''")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--token", help="Bearer access token")
    parser.add_argument("--username", help="Log in with this username (password prompted)")
    parser.add_argument("--run-id", action="append", default=[], help="Select by extraction run id (repeatable)")
    parser.add_argument("--node-label", action="append", default=[], help="Select by node label (repeatable)")
    parser.add_argument("--name", action="append", default=[], help="Select by instance name (repeatable)")
    parser.add_argument("--search", help="Select by free-text search query")
    parser.add_argument("--exclude-id", action="append", default=[], help="Instance id to exclude (repeatable)")
    parser.add_argument("--all-except", action="append", default=[],
                        help="Select EVERY instance except DO types matching this name/label "
                             "(case-insensitive, repeatable), e.g. --all-except \"App Overview\". "
                             "Standalone mode — not combinable with the other selection filters")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    parser.add_argument("--insecure", action="store_true", help="Skip TLS verification")
    args = parser.parse_args()

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

    spec: dict[str, Any] = {}
    if args.run_id:
        spec["run_ids"] = args.run_id
    if args.node_label:
        spec["node_labels"] = args.node_label
    if args.name:
        spec["instance_names"] = args.name
    if args.search:
        spec["search_query"] = args.search
    if args.exclude_id:
        spec["exclude_instance_ids"] = args.exclude_id

    if args.all_except:
        if any(k in spec for k in ("run_ids", "node_labels", "instance_names", "search_query")):
            sys.exit("--all-except is a standalone selection mode; don't combine it with other filters")
        spec["run_type_pairs"] = build_all_except_pairs(client, args.project_id, args.all_except)
        if not spec["run_type_pairs"]:
            print("[delete] nothing left to select after exclusions")
            return

    if not any(k in spec for k in ("run_ids", "node_labels", "instance_names", "search_query", "run_type_pairs")):
        list_instances(client, args.project_id)
        return

    # Dry run: expected_total=0 makes the server report the real count via 409.
    probe = attempt_delete(client, args.project_id, spec, 0)
    if probe.status_code == 200:
        print("[delete] selection matched 0 instances — nothing to delete")
        print(json.dumps(probe.json(), indent=2)[:1000])
        return
    if probe.status_code == 403:
        sys.exit("[delete] HTTP 403 — Graph Explorer write operations are disabled on this backend "
                 "(feature flag 'graph_db'). " + probe.text[:300])
    if probe.status_code != 409:
        sys.exit(f"[delete] probe failed: HTTP {probe.status_code}: {probe.text[:500]}")

    actual = probe.json().get("actual")
    if not isinstance(actual, int) or actual <= 0:
        sys.exit(f"[delete] unexpected probe response: {probe.text[:500]}")

    print(f"[delete] selection spec: {json.dumps(spec)}")
    print(f"[delete] matches {actual} instance(s) in project {args.project_id}")
    if not args.yes:
        answer = input(f"Delete {actual} instance(s)? This cannot be undone. [y/N] ").strip().lower()
        if answer != "y":
            print("[delete] aborted")
            return

    result = attempt_delete(client, args.project_id, spec, actual)
    if result.status_code == 409:
        body = result.json()
        sys.exit(f"[delete] count changed between probe and delete "
                 f"(expected {body.get('expected')}, now {body.get('actual')}) — re-run to retry")
    if result.status_code != 200:
        sys.exit(f"[delete] failed: HTTP {result.status_code}: {result.text[:500]}")
    print(f"[delete] done: {json.dumps(result.json(), indent=2)[:2000]}")


if __name__ == "__main__":
    main()
