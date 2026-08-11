#!/usr/bin/env python3
"""Read-only, API-only support probe for the 3.04->3.07 artifact regressions.

Run this against ANY environment (local, staging, the "other machine") using
only the HTTP API — it never touches the database. It confirms and quantifies
the two upgrade regressions documented in
``docs_imp/artifact_307_upgrade_regression.md`` and prints a report you can
paste into a support ticket (optionally also a JSON summary via --json-out).

WHAT IT CHECKS
--------------
A. Stuck-generating artifacts (Bug 4). Lists artifacts whose status is
   ``generating`` and flags the "never generated" fingerprint
   (``run_ids == []`` and ``created == updated``). No writes.

B. DO-item pagination eid-precision bug (Bugs 1/3). This is the important one.
   The 3.07 viewer pages Dynamic Object items with a keyset cursor over the
   AGE graphid ``id(item)`` (``next_cursor``). That graphid is emitted as a
   bare JSON number and consumed by the browser as a JS ``number``. Once a
   graphid exceeds 2^53 (Number.MAX_SAFE_INTEGER) — which happens on mature
   graphs, where AGE label ids climb past ~32 — ``JSON.parse`` ROUNDS it, so
   the ``after_eid`` the browser echoes back no longer matches the true
   boundary. The keyset ``id(item) > after_eid`` then re-returns or skips
   boundary rows, and the viewer's no-dedup page-concat renders duplicates.

   For each definition (DO type) the probe runs a THREE-WAY test:
     1. BASELINE  - one big page (no cursor). Ground truth; no cursor round-trip.
     2. EXACT     - page with the exact integer cursor (what Python/curl send).
                    Should equal baseline -> proves the server SQL is correct.
     3. JS-SIM    - page but round each cursor through float64 first
                    (``int(float(n))``), exactly what a browser does. If this
                    shows duplicates/missing that EXACT does not, AND the
                    cursors exceeded 2^53, the browser precision bug is proven
                    for THIS project's data.

Endpoints used (all GET, all read-only):
    GET /projects/<pid>/artifacts
    GET /dynamic-objects/definitions?project_id=<pid>
    GET /dynamic-objects/definitions/<defId>/items?project_id=<pid>&limit=&after_eid=&after_value=&expand=none

Auth: --token (browser localStorage accessToken) or --username (password
prompted). --direct for a port-forwarded / backend-direct base URL (no /api).

Examples:
    # Everything, human report:
    python artifact_support_probe.py --base-url https://app.example --token <jwt> \
        --project-id 6bcbe161-...-0a4fe0d3e944

    # Direct backend via port-forward, small page size to stress pagination,
    # write a JSON bundle for the ticket:
    python artifact_support_probe.py --base-url http://localhost:9080 --direct \
        --project-id <uuid> --username admin --page-size 25 --json-out probe.json

Requires: pip install requests
"""
from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

JS_MAX_SAFE_INT = 2**53 - 1  # 9,007,199,254,740,991


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
        print(f"[auth] logged in as {username}")

    def _refresh(self) -> bool:
        if not self.refresh_token:
            return False
        r = self.session.post(f"{self.base_url}/login/refresh",
                              headers={"Authorization": f"Bearer {self.refresh_token}"}, timeout=60)
        if r.status_code != 200:
            return False
        self.access_token = r.json()["access_token"]
        return True

    def get(self, path: str, *, params: dict | None = None, retry_auth: bool = True) -> requests.Response:
        r = self.session.get(f"{self.base_url}{path}", params=params,
                             headers={"Authorization": f"Bearer {self.access_token}"}, timeout=120)
        if r.status_code == 401 and retry_auth and self._refresh():
            return self.get(path, params=params, retry_auth=False)
        return r


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def parse_dt(val: str | None) -> datetime | None:
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def item_identity(item: dict[str, Any]) -> str:
    """Stable per-item key. Prefer the DO item's own ``id`` (survives the API's
    property cleaning); else a hash of the whole item so identical rows collide."""
    for k in ("id", "Id", "ID"):
        if isinstance(item.get(k), (str, int)) and str(item.get(k)).strip():
            return f"id:{item[k]}"
    blob = json.dumps(item, sort_keys=True, default=str)
    return "sha1:" + hashlib.sha1(blob.encode("utf-8")).hexdigest()


def js_round(n: int | None) -> int | None:
    """Reproduce what a browser does to a JSON integer: hold it as a float64.
    For |n| > 2^53 this loses precision exactly as ``JSON.parse`` would."""
    if n is None:
        return None
    return int(float(n))


def _extract_list(body: Any, *keys: str) -> list[dict]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for k in keys:
            v = body.get(k)
            if isinstance(v, list):
                return v
    return []


# ---------------------------------------------------------------------------
# Section A - stuck-generating artifacts (Bug 4)
# ---------------------------------------------------------------------------

def probe_stuck_artifacts(client: RhinoClient, pid: str) -> dict[str, Any]:
    print("\n" + "=" * 72)
    print("A. STUCK-GENERATING ARTIFACTS (Bug 4)")
    print("=" * 72)
    r = client.get(f"/projects/{pid}/artifacts")
    if r.status_code != 200:
        print(f"  [skip] GET artifacts -> HTTP {r.status_code}: {r.text[:150]}")
        return {"available": False, "http_status": r.status_code}
    artifacts = r.json() if isinstance(r.json(), list) else _extract_list(r.json(), "items", "artifacts")
    status_counts = Counter(a.get("status") for a in artifacts)
    generating = [a for a in artifacts if a.get("status") == "generating"]
    now = datetime.now(timezone.utc)

    orphans = []
    for a in generating:
        run_ids = a.get("run_ids") or []
        created, updated = a.get("created"), a.get("updated")
        never = (not run_ids) and created is not None and created == updated
        upd = parse_dt(updated)
        age_h = (now - upd).total_seconds() / 3600 if upd else None
        rec = {
            "id": a.get("id"), "type": a.get("artifact_type"), "name": a.get("name"),
            "run_ids": len(run_ids), "never_generated": never,
            "age_hours": round(age_h, 1) if age_h is not None else None,
        }
        orphans.append(rec)

    print(f"  total artifacts: {len(artifacts)}")
    print(f"  status breakdown: {dict(status_counts)}")
    print(f"  stuck 'generating': {len(generating)}"
          f"  (never-generated fingerprint: {sum(1 for o in orphans if o['never_generated'])})")
    for o in sorted(orphans, key=lambda x: (not x["never_generated"], x["type"] or "")):
        flag = "NEVER-GENERATED" if o["never_generated"] else "started-then-stalled"
        age = f"{o['age_hours']}h" if o["age_hours"] is not None else "?"
        print(f"    - {o['id']}  type={o['type']:<22} run_ids={o['run_ids']} age={age:>7}  [{flag}]  {(o['name'] or '')[:40]!r}")
    return {
        "available": True,
        "total": len(artifacts),
        "status_counts": dict(status_counts),
        "stuck_generating": len(generating),
        "never_generated": sum(1 for o in orphans if o["never_generated"]),
        "stuck": orphans,
    }


# ---------------------------------------------------------------------------
# Section B - DO-item pagination eid-precision test (Bugs 1/3)
# ---------------------------------------------------------------------------

def fetch_page(client: RhinoClient, pid: str, def_id: str, *, limit: int,
               after_eid: int | None = None, after_value_json: str | None = None,
               sort_by: str | None = None, sort_order: str = "asc") -> dict[str, Any] | None:
    params: dict[str, Any] = {"project_id": pid, "limit": limit, "expand": "none"}
    if after_eid is not None:
        params["after_eid"] = after_eid
    if after_value_json is not None:
        params["after_value"] = after_value_json
    if sort_by:
        params["sort_by"] = sort_by
        params["sort_order"] = sort_order
    r = client.get(f"/dynamic-objects/definitions/{quote(def_id, safe='')}/items", params=params)
    if r.status_code != 200:
        print(f"      [items HTTP {r.status_code}] {r.text[:120]}")
        return None
    return r.json()


def page_through(client: RhinoClient, pid: str, def_id: str, *, page_size: int,
                 simulate_js: bool, sort_by: str | None, sort_order: str) -> dict[str, Any]:
    """Page a definition to exhaustion. Returns identities (with repeats),
    the max cursor seen, and whether any cursor lost float64 precision."""
    identities: list[str] = []
    max_cursor = 0
    precision_lost = False
    after_eid: int | None = None
    after_value_json: str | None = None
    guard = 0
    while guard < 100_000:
        guard += 1
        page = fetch_page(client, pid, def_id, limit=page_size, after_eid=after_eid,
                          after_value_json=after_value_json, sort_by=sort_by, sort_order=sort_order)
        if page is None:
            break
        for it in page.get("items", []):
            identities.append(item_identity(it))
        nxt = page.get("next_cursor")
        if page.get("has_more") and nxt is not None:
            max_cursor = max(max_cursor, int(nxt))
            if js_round(nxt) != int(nxt):
                precision_lost = True
            # advance the cursor. simulate_js reproduces the browser's rounding.
            after_eid = js_round(nxt) if simulate_js else int(nxt)
            if sort_by:
                after_value_json = json.dumps(page.get("next_cursor_value"))
        else:
            break
    return {"identities": identities, "max_cursor": max_cursor, "precision_lost": precision_lost}


def probe_pagination(client: RhinoClient, pid: str, *, page_size: int, big_limit: int,
                     max_defs: int | None, sort_by: str | None, sort_order: str) -> dict[str, Any]:
    print("\n" + "=" * 72)
    print("B. DO-ITEM PAGINATION eid-PRECISION TEST (Bugs 1/3)")
    print("=" * 72)
    r = client.get("/dynamic-objects/definitions", params={"project_id": pid})
    if r.status_code != 200:
        print(f"  [skip] GET definitions -> HTTP {r.status_code}: {r.text[:150]}")
        return {"available": False, "http_status": r.status_code}
    defs = _extract_list(r.json(), "items", "definitions", "data")
    print(f"  definitions (DO types): {len(defs)}"
          + (f"  (probing first {max_defs})" if max_defs else ""))
    if max_defs:
        defs = defs[:max_defs]

    results = []
    confirmed = 0
    for d in defs:
        def_id = d.get("id") or d.get("definition_group_id")
        name = d.get("name") or d.get("node_label") or def_id
        if not def_id:
            continue

        base = fetch_page(client, pid, def_id, limit=big_limit, sort_by=sort_by, sort_order=sort_order)
        if base is None:
            continue
        baseline_items = [item_identity(it) for it in base.get("items", [])]
        baseline_set = set(baseline_items)
        baseline_truncated = bool(base.get("has_more"))
        total_count = base.get("total_count")

        if len(baseline_items) <= page_size:
            # single page in the viewer too -> pagination never engages here.
            results.append({"definition": name, "id": str(def_id), "items": len(baseline_items),
                            "paginated": False, "verdict": "not-paginated"})
            continue

        exact = page_through(client, pid, def_id, page_size=page_size, simulate_js=False,
                             sort_by=sort_by, sort_order=sort_order)
        jssim = page_through(client, pid, def_id, page_size=page_size, simulate_js=True,
                             sort_by=sort_by, sort_order=sort_order)

        exact_counts = Counter(exact["identities"])
        js_counts = Counter(jssim["identities"])
        exact_dups = sum(c - 1 for c in exact_counts.values() if c > 1)
        js_dups = sum(c - 1 for c in js_counts.values() if c > 1)
        js_missing = len(baseline_set - set(js_counts)) if not baseline_truncated else None
        cursor_over_2_53 = jssim["max_cursor"] > JS_MAX_SAFE_INT
        precision_lost = jssim["precision_lost"]

        # Verdict
        if precision_lost and (js_dups > 0 or (js_missing or 0) > 0) and exact_dups == 0:
            verdict = "CONFIRMED eid-precision bug (JS cursor rounding dups/drops rows)"
            confirmed += 1
        elif js_dups > 0 and exact_dups == 0:
            verdict = "duplicates under JS paging (cursor round-trip) - likely eid/precision"
            confirmed += 1
        elif exact_dups > 0:
            verdict = "duplicates even with exact cursor - backend keyset/query issue"
        elif cursor_over_2_53:
            verdict = "cursors exceed 2^53 but no dup observed this run (data-dependent; monitor)"
        else:
            verdict = "clean"

        rec = {
            "definition": name, "id": str(def_id),
            "items_baseline": len(baseline_items), "total_count": total_count,
            "baseline_truncated_at_big_limit": baseline_truncated,
            "paginated": True, "page_size": page_size,
            "max_cursor": jssim["max_cursor"], "cursor_exceeds_2^53": cursor_over_2_53,
            "cursor_precision_lost": precision_lost,
            "dupes_exact_cursor": exact_dups, "dupes_js_cursor": js_dups,
            "missing_js_cursor": js_missing, "verdict": verdict,
        }
        results.append(rec)
        print(f"\n  DO type: {name}  ({len(baseline_items)} items, total_count={total_count})")
        print(f"    max cursor seen : {jssim['max_cursor']:,}   (2^53 = {JS_MAX_SAFE_INT:,})")
        print(f"    cursor > 2^53   : {cursor_over_2_53}   precision-lost in JS: {precision_lost}")
        print(f"    dup rows  exact-cursor paging : {exact_dups}   (expect 0 -> server SQL is correct)")
        print(f"    dup rows  JS-rounded  paging  : {js_dups}   (browser behaviour)")
        if js_missing is not None:
            print(f"    missing rows JS paging        : {js_missing}")
        print(f"    VERDICT: {verdict}")

    print(f"\n  -> definitions exhibiting the pagination bug: {confirmed} / "
          f"{sum(1 for x in results if x.get('paginated'))} paginated")
    return {"available": True, "definitions_total": len(defs),
            "confirmed_definitions": confirmed, "results": results}


# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", required=True)
    p.add_argument("--api-prefix", default="/api", help="'/api' via frontend nginx; '' for direct backend")
    p.add_argument("--direct", action="store_true", help="Shortcut for --api-prefix ''")
    p.add_argument("--project-id", required=True)
    p.add_argument("--token", help="Bearer access token")
    p.add_argument("--username", help="Log in with this username (password prompted)")
    p.add_argument("--page-size", type=int, default=50, help="Cursor page size for the stress test (default 50)")
    p.add_argument("--big-limit", type=int, default=2000, help="Single-fetch baseline page size (default 2000, server max)")
    p.add_argument("--max-defs", type=int, help="Only probe the first N definitions")
    p.add_argument("--sort-by", help="Sort field to page by (default: unsorted = pure eid cursor, the most direct test)")
    p.add_argument("--sort-order", default="asc", choices=["asc", "desc"])
    p.add_argument("--skip-artifacts", action="store_true", help="Skip Section A")
    p.add_argument("--skip-pagination", action="store_true", help="Skip Section B")
    p.add_argument("--json-out", help="Write the full machine-readable summary here")
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
    print(f"\n### Artifact support probe — project {pid}")
    print(f"### base={client.base_url}  page_size={args.page_size}  big_limit={args.big_limit}"
          f"  sort_by={args.sort_by or '(none/eid)'}")

    summary: dict[str, Any] = {"project_id": pid, "base_url": client.base_url,
                               "page_size": args.page_size, "sort_by": args.sort_by}
    if not args.skip_artifacts:
        summary["stuck_artifacts"] = probe_stuck_artifacts(client, pid)
    if not args.skip_pagination:
        summary["pagination"] = probe_pagination(
            client, pid, page_size=args.page_size, big_limit=args.big_limit,
            max_defs=args.max_defs, sort_by=args.sort_by, sort_order=args.sort_order)

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    sa = summary.get("stuck_artifacts", {})
    if sa.get("available"):
        print(f"  Bug 4: {sa['stuck_generating']} stuck 'generating' "
              f"({sa['never_generated']} never-generated) of {sa['total']} artifacts.")
    pg = summary.get("pagination", {})
    if pg.get("available"):
        print(f"  Bugs 1/3: {pg['confirmed_definitions']} DO type(s) reproduce the "
              f"pagination duplicate/precision bug.")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n  wrote JSON summary -> {args.json_out}")


if __name__ == "__main__":
    main()
