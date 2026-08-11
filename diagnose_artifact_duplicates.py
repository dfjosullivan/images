#!/usr/bin/env python3
"""Diagnose duplicate / missing artifact cards for a Rhino project — via the API.

Read-only. Uses the *same* endpoints the artifact viewer uses, so it sees
exactly what the UI sees and can classify why cards duplicate (or vanish)
without touching the database:

    GET /projects/<pid>/graph/nodes                        -> root containers (one per DO type)
    GET /projects/<pid>/graph/nodes/<id>/children          -> the items (cards) in a container

For every root container with instances it pulls the children (paginated),
then reports, for the project:

  * Names that appear on more than one node — the driver of "2 cards for X".
    Each member is shown with its container/DO type, source routine, and
    ``rhino_deleted`` flag, and classified:
        - same container type      -> duplicate instances within one DO (data)
        - different container types -> cross-DO name collision (e.g. a Use Case
                                       and a User Journey sharing a name) —
                                       genuine distinct data; duplicate CARDS are
                                       a viewer grouping/de-dup concern
        - some rhino_deleted        -> a client that filters rhino_deleted shows
                                       fewer cards than one that doesn't (explains
                                       "fixed on one system, duplicated on another")
  * Whether the viewer's own dedup (it drops rhino_deleted items) already
    collapses each collision, so you can tell if the fix is present/effective.

Auth: --token (browser localStorage accessToken) or --username (password
prompted). --direct for a port-forwarded / backend-direct base URL.

Examples:
    python diagnose_artifact_duplicates.py --base-url http://localhost:9080 --direct \
        --project-id 6bcbe161-...-0a4fe0d3e944 --username admin
    python diagnose_artifact_duplicates.py --base-url https://host --token <jwt> \
        --project-id <uuid> --name "Add or Remove an Automatic Cash Sweep from a Client Account"

Requires: pip install requests
"""
from __future__ import annotations

import argparse
import getpass
import sys
from collections import defaultdict
from typing import Any

import requests


class RhinoClient:
    def __init__(self, base_url: str, api_prefix: str = "/api", verify: bool = True):
        self.base_url = base_url.rstrip("/") + api_prefix.rstrip("/")
        self.session = requests.Session()
        self.session.verify = verify
        self.access_token: str | None = None
        self.refresh_token: str | None = None

    def login(self, username: str, password: str) -> None:
        r = self.session.post(f"{self.base_url}/login",
                              json={"username": username, "password": password}, timeout=60)
        r.raise_for_status()
        b = r.json()
        self.access_token, self.refresh_token = b["access_token"], b.get("refresh_token")
        print("[auth] logged in")

    def _refresh(self) -> bool:
        if not self.refresh_token:
            return False
        r = self.session.post(f"{self.base_url}/login/refresh",
                              headers={"Authorization": f"Bearer {self.refresh_token}"}, timeout=60)
        if r.status_code != 200:
            return False
        self.access_token = r.json()["access_token"]
        return True

    def get(self, path: str, *, retry_auth: bool = True) -> requests.Response:
        r = self.session.get(f"{self.base_url}{path}",
                             headers={"Authorization": f"Bearer {self.access_token}"}, timeout=120)
        if r.status_code == 401 and retry_auth and self._refresh():
            return self.get(path, retry_auth=False)
        return r


def fetch_roots(client: RhinoClient, pid: str) -> list[dict[str, Any]]:
    r = client.get(f"/projects/{pid}/graph/nodes")
    if r.status_code == 403:
        sys.exit("[diag] HTTP 403 — Graph Explorer feature disabled (graph_db).")
    if r.status_code != 200:
        sys.exit(f"[diag] roots fetch failed: HTTP {r.status_code}: {r.text[:300]}")
    return r.json().get("items", [])


def fetch_children(client: RhinoClient, pid: str, node_id: str) -> list[dict[str, Any]]:
    from urllib.parse import quote
    out: list[dict[str, Any]] = []
    offset, limit, guard = 0, 200, 0
    while guard < 1000:
        guard += 1
        r = client.get(f"/projects/{pid}/graph/nodes/{quote(node_id, safe='')}/children?limit={limit}&offset={offset}")
        if r.status_code != 200:
            print(f"[diag]   children fetch failed for {node_id[:60]}: HTTP {r.status_code}")
            break
        body = r.json()
        items = body.get("children", body.get("items", []))
        out.extend(items)
        if not body.get("has_more") or not items:
            break
        offset += limit
    return out


def routine_of(node_id: str) -> str:
    return node_id.split(":routine:", 1)[1].split(":")[0] if ":routine:" in node_id else ""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", required=True)
    p.add_argument("--api-prefix", default="/api", help="'/api' via frontend nginx; '' for direct backend")
    p.add_argument("--direct", action="store_true", help="Shortcut for --api-prefix ''")
    p.add_argument("--project-id", required=True)
    p.add_argument("--token", help="Bearer access token")
    p.add_argument("--username", help="Log in with this username (password prompted)")
    p.add_argument("--name", help="Focus the report on one card name (exact match)")
    p.add_argument("--insecure", action="store_true", help="Skip TLS verification")
    args = p.parse_args()

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

    pid = args.project_id
    print(f"\n=== Artifact-card diagnosis for project {pid} (via API) ===")
    roots = fetch_roots(client, pid)
    containers = [r for r in roots if r.get("label") and (r.get("element_count") or 0) > 0]
    print(f"root containers with instances: {len(containers)}")
    if not containers:
        print("No populated containers — nothing for the artifact viewer to render.")
        return

    # name -> list of (container_label, item_id, rhino_deleted)
    by_name: dict[str, list[tuple[str, str, bool]]] = defaultdict(list)
    for c in containers:
        label = c["label"]
        children = fetch_children(client, pid, c["id"])
        for it in children:
            nm = it.get("name")
            if nm is None:
                continue
            by_name[nm].append((label, it.get("id", ""), bool(it.get("rhino_deleted"))))
    total_cards = sum(len(v) for v in by_name.values())
    print(f"total item cards across containers: {total_cards}")

    names = [args.name] if args.name else sorted(by_name)
    collisions = [(nm, by_name[nm]) for nm in names if nm in by_name and len(by_name[nm]) > 1]

    print(f"\n[names on >1 card]: {len(collisions)}"
          + ("  (each renders as multiple cards)" if collisions else ""))
    fanout = cross_do = same_label = deleted_masks = 0
    for nm, members in collisions:
        labels = {m[0] for m in members}
        ids = [m[1] for m in members]
        distinct_ids = set(ids)
        visible = [m for m in members if not m[2]]
        # THE key discriminator: same id returned more than once == the children
        # query fanned out over multiple edges (read-layer bug, NOT real data);
        # all-distinct ids == genuinely separate nodes (data).
        if len(distinct_ids) < len(members):
            kind = "READ-LAYER FAN-OUT (same node returned repeatedly)"
            fanout += 1
        elif len(labels) > 1:
            kind = "cross-DO name collision (distinct types)"
            cross_do += 1
        else:
            kind = "same-type DISTINCT duplicate nodes (data)"
            same_label += 1
        note = ""
        if len(members) > 1 and len(visible) <= 1:
            note = "  [rhino_deleted-filtering client shows 1 -> fixed there]"
            deleted_masks += 1
        print(f'\n  "{nm}"  x{len(members)} cards, {len(distinct_ids)} distinct id(s), '
              f'{len(visible)} not-deleted  -> {kind}{note}')
        for label, mid, deleted in members:
            print(f"      {label:28} deleted={'yes' if deleted else 'no':3} "
                  f"routine={routine_of(mid):24} id=…{mid[-24:]}")

    print("\n=== Verdict ===")
    if not collisions:
        print("- No name appears on >1 card. If the UI still shows duplicates, look elsewhere.")
    else:
        if fanout:
            print(f"- {fanout} READ-LAYER FAN-OUT case(s): the SAME node id is returned multiple times")
            print("  by the children query (reachable via >1 structural edge, no DISTINCT). This is a")
            print("  VIEWER/QUERY bug — the graph holds one node; the API duplicates it. No data dup.")
        if same_label:
            print(f"- {same_label} SAME-TYPE DISTINCT DUPLICATE(S): multiple REAL nodes (different ids) of one")
            print("  DO type share a name, usually from one import/extraction routine — genuine DATA")
            print("  duplication written at extract/import time. Needs a data-side investigation/cleanup.")
        if cross_do:
            print(f"- {cross_do} CROSS-DO NAME COLLISION(S): same name on different DO types (e.g. Use Case +")
            print("  User Journey). Genuine distinct data; duplicate CARDS are a viewer grouping concern.")
        if deleted_masks:
            print(f"- {deleted_masks} collision(s) collapse under rhino_deleted filtering (the 'fixed on one")
            print("  system, duplicated on another' signature).")


if __name__ == "__main__":
    main()
