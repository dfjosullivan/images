#!/usr/bin/env python3
"""Compare IMPORTED vs NATIVE Dynamic Object items and decide if deleting the
imported set is SAFE (no data loss) — read-only, API-only.

Context. When DOs are imported cross-project, the graph can end up with two
copies of every item: the project's own ("native") and an imported copy from
another project. The imported copies carry a foreign id prefix and typically
``project_id: null``. Reading a definition then returns BOTH copies (e.g. an
artifact renders every card twice). Deleting the imported set fixes it — but
ONLY if the imported copy is a true redundant duplicate. If an imported copy
DIFFERS from its native twin (post-import edit, newer extraction) or has NO
native twin, a blind ``--imported`` delete would lose data.

This script pairs imported vs native per DO type and reports, per type and
overall, whether an imported-delete is safe.

How it pairs. A DO item id looks like
``<projectUuid>:dobj:<hash>:routine:<routine>:<routine-project>:<run>:item:<N>``.
Native vs imported copies of the same logical item are identical EXCEPT the
leading ``<projectUuid>``. So the pairing key is the id with the leading
project uuid stripped. An item is NATIVE when its leading uuid == the requested
project; otherwise FOREIGN/IMPORTED (usually ``project_id: null`` too).

How it compares. For a native/foreign pair it compares every field except
``id``/``project_id``, after replacing any project-uuid substrings with a
placeholder (so link fields that only differ by an embedded project prefix are
treated as equal, not a real difference).

Verdict per DO type:
  * SAFE          - every foreign item has an IDENTICAL native twin (pure dup).
  * UNSAFE-DIFF   - some foreign item differs in content from its native twin.
  * UNSAFE-ORPHAN - some foreign item has NO native twin (delete would lose it).
Overall SAFE only if every populated type is SAFE.

Endpoints (all GET, read-only):
    GET /dynamic-objects/definitions?project_id=<pid>
    GET /dynamic-objects/definitions/<defId>/items?project_id=<pid>&limit=&after_eid=&expand=none

Auth: --token (browser accessToken) or --username (prompted). --direct for a
port-forwarded / backend-direct base URL.

Example:
    python compare_imported_dos.py --base-url https://host --token <jwt> \
        --project-id f86fcb62-... --show-diffs --json-out compare.json

Requires: pip install requests
"""
from __future__ import annotations

import argparse
import getpass
import json
import re
import sys
from collections import defaultdict
from typing import Any
from urllib.parse import quote

import requests

# Bump on behavioural changes so log output is traceable.
#   1.0.0  initial: per-DO-type imported-vs-native compare + safe-delete verdict
#   1.1.0  add --strict (compare link uuids literally, not just project prefixes)
SCRIPT_VERSION = "1.1.0"

_UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_COMPARE_IGNORE = {"id", "project_id"}


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


def _extract_list(body: Any, *keys: str) -> list[dict]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for k in keys:
            v = body.get(k)
            if isinstance(v, list):
                return v
    return []


def fetch_all_items(client: RhinoClient, pid: str, def_id: str, page_size: int) -> list[dict[str, Any]]:
    """Page a definition to exhaustion (unsorted eid cursor, expand=none)."""
    out: list[dict[str, Any]] = []
    after_eid: Any = None
    guard = 0
    while guard < 100_000:
        guard += 1
        params: dict[str, Any] = {"project_id": pid, "limit": page_size, "expand": "none"}
        if after_eid is not None:
            params["after_eid"] = after_eid
        r = client.get(f"/dynamic-objects/definitions/{quote(def_id, safe='')}/items", params=params)
        if r.status_code != 200:
            print(f"    [items HTTP {r.status_code}] {r.text[:120]}")
            break
        body = r.json()
        out.extend(body.get("items", []))
        nxt = body.get("next_cursor")
        if body.get("has_more") and nxt is not None:
            after_eid = nxt  # keep whatever type the server sent (string cursor post-fix)
        else:
            break
    return out


def leading_project(item_id: str) -> str:
    return item_id.split(":", 1)[0] if ":" in item_id else item_id


def pair_key(item_id: str) -> str:
    """Id with the leading project uuid stripped — shared by native + imported twins."""
    return item_id.split(":", 1)[1] if ":" in item_id else item_id


def normalize_field(value: Any, projs: set[str], strict: bool = False) -> str:
    s = json.dumps(value, sort_keys=True, default=str)
    for p in projs:
        if p:
            s = s.replace(p, "<PROJ>")
    if strict:
        # strict: only the project prefixes are normalised. Any other uuid — e.g.
        # a link pointing at a genuinely different node — stays literal and WILL
        # be flagged as a difference.
        return s
    # lenient (default): also neutralise any remaining uuid so a link that only
    # differs by an embedded project prefix is not treated as a content change.
    return _UUID_RE.sub("<UUID>", s)


def diff_fields(native: dict, foreign: dict, projs: set[str], strict: bool = False) -> set[str]:
    keys = (set(native) | set(foreign)) - _COMPARE_IGNORE
    out = set()
    for k in keys:
        if normalize_field(native.get(k), projs, strict) != normalize_field(foreign.get(k), projs, strict):
            out.add(k)
    return out


def analyse_definition(client: RhinoClient, pid: str, definition: dict, page_size: int,
                       show_diffs: bool, strict: bool = False) -> dict[str, Any]:
    def_id = definition.get("id") or definition.get("definition_group_id")
    label = definition.get("node_label") or definition.get("name") or def_id
    items = fetch_all_items(client, pid, def_id, page_size)

    native: dict[str, dict] = {}
    foreign: dict[str, list[dict]] = defaultdict(list)
    foreign_projects: set[str] = set()
    for it in items:
        iid = it.get("id")
        if not iid:
            continue
        if leading_project(iid) == pid:
            native[pair_key(iid)] = it
        else:
            foreign[pair_key(iid)].append(it)
            foreign_projects.add(leading_project(iid))

    identical = 0
    differing: list[dict[str, Any]] = []
    orphans: list[str] = []
    for key, fcopies in foreign.items():
        nat = native.get(key)
        if nat is None:
            orphans.append(key)
            continue
        for fc in fcopies:
            d = diff_fields(nat, fc, foreign_projects | {pid}, strict)
            if d:
                rec = {"pair_key": key, "fields": sorted(d)}
                if show_diffs:
                    rec["native"] = {k: nat.get(k) for k in d}
                    rec["foreign"] = {k: fc.get(k) for k in d}
                differing.append(rec)
            else:
                identical += 1

    if orphans:
        verdict = "UNSAFE-ORPHAN"
    elif differing:
        verdict = "UNSAFE-DIFF"
    elif foreign:
        verdict = "SAFE"
    else:
        verdict = "no-foreign"

    return {
        "do_type": label, "id": str(def_id),
        "total_items": len(items), "native": len(native),
        "foreign": sum(len(v) for v in foreign.values()),
        "foreign_projects": sorted(foreign_projects),
        "identical_dups": identical, "differing": differing, "orphans": orphans,
        "verdict": verdict,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--version", action="version", version=f"compare_imported_dos {SCRIPT_VERSION}")
    p.add_argument("--base-url", required=True)
    p.add_argument("--api-prefix", default="/api", help="'/api' via frontend nginx; '' for direct backend")
    p.add_argument("--direct", action="store_true", help="Shortcut for --api-prefix ''")
    p.add_argument("--project-id", required=True, help="The project whose graph is being cleaned")
    p.add_argument("--token", help="Bearer access token")
    p.add_argument("--username", help="Log in with this username (password prompted)")
    p.add_argument("--page-size", type=int, default=2000, help="Item page size (default 2000)")
    p.add_argument("--max-defs", type=int, help="Only analyse the first N definitions")
    p.add_argument("--only-foreign", action="store_true", help="Skip DO types that have no imported items")
    p.add_argument("--show-diffs", action="store_true", help="Include the differing field values in output")
    p.add_argument("--strict", action="store_true",
                   help="Only normalise the native/foreign project prefixes; compare all other uuids "
                        "literally (flags links that point at genuinely different nodes). Default is "
                        "lenient (all uuids normalised, so only content changes are flagged).")
    p.add_argument("--json-out", help="Write the full machine-readable report here")
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
    print(f"\n### compare_imported_dos v{SCRIPT_VERSION} — project {pid}")
    print(f"### base={client.base_url}  mode={'STRICT (uuid-literal)' if args.strict else 'lenient (uuid-normalised)'}")

    r = client.get("/dynamic-objects/definitions", params={"project_id": pid})
    if r.status_code != 200:
        sys.exit(f"list definitions failed: HTTP {r.status_code}: {r.text[:200]}")
    defs = _extract_list(r.json(), "items", "definitions", "data")
    if args.max_defs:
        defs = defs[:args.max_defs]
    print(f"definitions (DO types): {len(defs)}\n")

    results = []
    for d in defs:
        if not (d.get("id") or d.get("definition_group_id")):
            continue
        res = analyse_definition(client, pid, d, args.page_size, args.show_diffs, args.strict)
        if args.only_foreign and res["foreign"] == 0:
            continue
        results.append(res)
        icon = {"SAFE": "OK ", "UNSAFE-DIFF": "!! ", "UNSAFE-ORPHAN": "XX ",
                "no-foreign": "-- "}.get(res["verdict"], "?? ")
        print(f"  [{icon}] {res['do_type']:<28} native={res['native']:<5} imported={res['foreign']:<5} "
              f"-> {res['verdict']}")
        if res["orphans"]:
            print(f"        {len(res['orphans'])} imported item(s) have NO native twin (would be lost)")
        for dd in res["differing"][:8]:
            print(f"        differs: {dd['pair_key'][-40:]}  fields={dd['fields']}")

    # ---- summary + overall verdict ----
    with_foreign = [r for r in results if r["foreign"] > 0]
    safe = [r for r in with_foreign if r["verdict"] == "SAFE"]
    unsafe = [r for r in with_foreign if r["verdict"].startswith("UNSAFE")]
    total_foreign = sum(r["foreign"] for r in with_foreign)
    all_foreign_projects = sorted({p for r in with_foreign for p in r["foreign_projects"]})

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print(f"  DO types with imported items : {len(with_foreign)}")
    print(f"  total imported items         : {total_foreign}")
    print(f"  imported from project(s)     : {all_foreign_projects or '(none)'}")
    print(f"  SAFE to delete (pure dup)    : {len(safe)} type(s)")
    print(f"  UNSAFE                       : {len(unsafe)} type(s)")
    for r in unsafe:
        why = f"{len(r['orphans'])} orphan(s)" if r["orphans"] else f"{len(r['differing'])} differ"
        print(f"      - {r['do_type']} [{r['verdict']}: {why}]")

    if not with_foreign:
        overall = "NOTHING-TO-DELETE (no imported items found)"
    elif unsafe:
        overall = "NOT SAFE for a blind --imported delete — resolve the UNSAFE types first"
    else:
        overall = "SAFE — every imported item is an identical duplicate of a native twin"
    print(f"\n  OVERALL: {overall}")
    if with_foreign and not unsafe:
        print("  Next: dry-run `delete_dynamic_objects.py --imported` and confirm it targets only")
        print(f"        the imported set, then --apply. Expect item counts to drop to the native total.")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump({"script_version": SCRIPT_VERSION, "project_id": pid,
                       "overall": overall, "results": results}, f, indent=2, default=str)
        print(f"\n  wrote report -> {args.json_out}")


if __name__ == "__main__":
    main()
