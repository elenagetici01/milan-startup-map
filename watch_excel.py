#!/usr/bin/env python3
"""
watch_excel.py — Syncs milan_startups.xlsx → startups.json on GitHub.

Excel is the single source of truth. On every sync:
  ADD    — row in Excel, not in JSON          → geocode + add pin
  UPDATE — row in both, but data changed      → update + re-geocode if address changed
  REMOVE — row in JSON, not in Excel          → remove pin
  KEEP   — row in both, data unchanged        → preserve as-is (id, lat, lng, logo, notes)

Usage:
    python watch_excel.py             # watch mode: auto-syncs on every Excel save
    python watch_excel.py --once      # run once and exit
    python watch_excel.py --excel path/to/file.xlsx

Required env vars:
    GITHUB_TOKEN   personal access token with repo Contents write access
    GITHUB_REPO    owner/repo slug  (default: elenagetici01/milan-startup-map)
    EXCEL_PATH     path to .xlsx    (default: milan_startups.xlsx)
"""

import os, sys, json, time, base64, argparse, requests, openpyxl
from pathlib import Path
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────
GITHUB_TOKEN  = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO   = os.environ.get('GITHUB_REPO', 'elenagetici01/milan-startup-map')
GITHUB_FILE   = 'startups.json'
GITHUB_BRANCH = 'main'
EXCEL_PATH    = os.environ.get('EXCEL_PATH', 'milan_startups.xlsx')

API_URL = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}'
GH_HEADERS = {
    'Authorization': f'Bearer {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
}

SKIP_HEADERS = {'name', 'startup', 'nome', 'company', 'startup name', 'n°', 'n', '#'}


# ── GitHub helpers ─────────────────────────────────────────────────────────────

def gh_fetch() -> tuple[list, str | None]:
    """Download startups.json. Returns (data_list, sha)."""
    r = requests.get(API_URL, headers=GH_HEADERS, timeout=15)
    if r.status_code == 404:
        return [], None
    r.raise_for_status()
    body = r.json()
    return json.loads(base64.b64decode(body['content']).decode()), body['sha']


def gh_push(data: list, sha: str | None, message: str) -> None:
    """Commit updated startups.json."""
    body = {
        'message': message,
        'branch':  GITHUB_BRANCH,
        'content': base64.b64encode(
            json.dumps(data, ensure_ascii=False, indent=2).encode()
        ).decode(),
    }
    if sha:
        body['sha'] = sha
    requests.put(API_URL, headers=GH_HEADERS, json=body, timeout=15).raise_for_status()


# ── Nominatim geocoding ────────────────────────────────────────────────────────

def geocode(address: str) -> tuple[float | None, float | None]:
    try:
        r = requests.get(
            'https://nominatim.openstreetmap.org/search',
            params={'q': address, 'format': 'json', 'limit': 1, 'countrycodes': 'it'},
            headers={'User-Agent': 'milan-startup-map/1.0'},
            timeout=10,
        )
        hits = r.json()
        if hits:
            return float(hits[0]['lat']), float(hits[0]['lon'])
    except Exception as e:
        print(f'    ⚠  geocode error: {e}')
    return None, None


# ── Excel reader ───────────────────────────────────────────────────────────────

def read_excel(path: str) -> list[dict]:
    """
    Returns list of {name, address} dicts.
    Col A = name, Col B = address. Skips blank rows and the header row.
    Supports any number of extra columns — only A and B are used.
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        name = str(row[0] or '').strip()
        addr = str(row[1] or '').strip() if len(row) > 1 else ''
        if not name or not addr:
            continue
        if i == 0 and name.lower() in SKIP_HEADERS:
            continue
        rows.append({'name': name, 'address': addr})
    wb.close()
    return rows


# ── Core sync ──────────────────────────────────────────────────────────────────

def sync(excel_path: str = EXCEL_PATH) -> None:
    print(f'\n[{datetime.now().strftime("%H:%M:%S")}] Starting sync…')

    path = Path(excel_path)
    if not path.exists():
        print(f'  ⚠  Excel not found: {path}')
        return

    # Read both sources
    excel_rows  = read_excel(str(path))
    json_data, sha = gh_fetch()

    excel_by_name = {r['name'].lower(): r for r in excel_rows}
    json_by_name  = {s['name'].lower(): s for s in json_data}

    print(f'  📊  Excel: {len(excel_rows)} rows   |   JSON: {len(json_data)} startups')

    to_add    = []
    to_update = []
    to_remove = []

    # What needs to change?
    for key, excel_row in excel_by_name.items():
        if key not in json_by_name:
            # New startup
            to_add.append(excel_row)
        else:
            existing = json_by_name[key]
            address_changed = existing.get('address', '').strip().lower() != excel_row['address'].strip().lower()
            if address_changed:
                to_update.append((existing, excel_row['address']))

    for key, entry in json_by_name.items():
        if key not in excel_by_name:
            to_remove.append(entry)

    if not to_add and not to_update and not to_remove:
        print('  ✓   Already in sync — nothing to do')
        return

    print(f'  ➕  Add: {len(to_add)}   ✏  Update: {len(to_update)}   🗑  Remove: {len(to_remove)}')

    # ── UPDATES: re-geocode changed addresses ──
    if to_update:
        print(f'  ✏   Updating {len(to_update)} address(es)…')
        for existing, new_addr in to_update:
            lat, lng = geocode(new_addr)
            coord = f'{lat:.5f}, {lng:.5f}' if lat else 'NOT FOUND'
            print(f'      {existing["name"]}: {new_addr}  →  {coord}')
            existing['address'] = new_addr
            existing['lat']     = lat
            existing['lng']     = lng
            time.sleep(1.1)

    # ── ADDS: geocode new startups ──
    if to_add:
        max_id = max((s.get('id', 0) for s in json_data), default=0)
        print(f'  ➕  Geocoding {len(to_add)} new startup(s)…')
        for i, row in enumerate(to_add):
            lat, lng = geocode(row['address'])
            coord = f'{lat:.5f}, {lng:.5f}' if lat else 'NOT FOUND'
            print(f'      + {row["name"]}: {coord}')
            row['id']  = max_id + i + 1
            row['lat'] = lat
            row['lng'] = lng
            if i < len(to_add) - 1:
                time.sleep(1.1)

    # ── REMOVES ──
    removed_keys = {s['name'].lower() for s in to_remove}
    for s in to_remove:
        print(f'  🗑   Removing: {s["name"]}')

    # Build final list: current JSON (minus removed) + new entries
    result = [s for s in json_data if s['name'].lower() not in removed_keys]
    result += to_add

    # Commit to GitHub
    parts = []
    if to_add:    parts.append(f'add {len(to_add)}')
    if to_update: parts.append(f'update {len(to_update)}')
    if to_remove: parts.append(f'remove {len(to_remove)}')
    commit_msg = 'data: ' + ', '.join(parts)

    gh_push(result, sha, commit_msg)
    print(f'  ✅  Done — {commit_msg}  ({len(result)} startups total)')


# ── Watch mode ─────────────────────────────────────────────────────────────────

def watch(excel_path: str) -> None:
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        sys.exit('ERROR: run  pip install watchdog  first')

    resolved = Path(excel_path).resolve()
    print(f'👁   Watching: {resolved}')
    print('     Syncs automatically on every save. Ctrl+C to stop.\n')

    sync(excel_path)          # sync once at startup
    last_run = [0.0]

    class Handler(FileSystemEventHandler):
        def on_modified(self, event):
            if Path(event.src_path).resolve() == resolved:
                now = time.time()
                if now - last_run[0] > 5:   # debounce Excel's multiple write events
                    last_run[0] = now
                    sync(excel_path)

    obs = Observer()
    obs.schedule(Handler(), str(resolved.parent), recursive=False)
    obs.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
        print('\nStopped.')
    obs.join()


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Sync Excel → startups.json on GitHub (add / update / remove)'
    )
    parser.add_argument('--once',  action='store_true', help='Run once and exit')
    parser.add_argument('--excel', default=EXCEL_PATH,  help='Path to .xlsx file')
    args = parser.parse_args()

    if not GITHUB_TOKEN:
        sys.exit(
            'ERROR: set GITHUB_TOKEN before running.\n'
            '  Windows : set GITHUB_TOKEN=ghp_yourtoken\n'
            '  Mac/Linux: export GITHUB_TOKEN=ghp_yourtoken'
        )

    if args.once:
        sync(args.excel)
    else:
        watch(args.excel)


if __name__ == '__main__':
    main()
