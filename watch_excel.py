#!/usr/bin/env python3
"""
watch_excel.py — Sync milan_startups.xlsx → startups.json on GitHub

Usage:
    python watch_excel.py           # watch mode: syncs on every Excel save
    python watch_excel.py --once    # run once and exit

Flow per sync:
    1. Read Excel (Col A = Name, Col B = Address)
    2. Fetch current startups.json from GitHub
    3. Compare by name (case-insensitive) — existing entries are NEVER modified
    4. Fill in lat/lng for existing entries that have null coordinates (one-time geocode)
    5. Geocode and append new entries
    6. Push updated startups.json to GitHub via API

Required env vars:
    GITHUB_TOKEN   — personal access token with repo Contents write access
    GITHUB_REPO    — owner/repo slug (default: elenagetici01/milan-startup-map)
    EXCEL_PATH     — path to .xlsx file (default: milan_startups.xlsx)
"""

import os
import sys
import json
import time
import base64
import argparse
import requests
import openpyxl
from pathlib import Path
from datetime import datetime

# ─── Config ────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO  = os.environ.get('GITHUB_REPO', 'elenagetici01/milan-startup-map')
GITHUB_FILE  = 'startups.json'
GITHUB_BRANCH = 'main'
EXCEL_PATH   = os.environ.get('EXCEL_PATH', 'milan_startups.xlsx')

API_BASE = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}'
HEADERS  = {
    'Authorization': f'Bearer {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
}

HEADER_NAMES = {'name', 'startup', 'nome', 'n°', 'n', '#', 'startup name'}


# ─── GitHub helpers ────────────────────────────────────────────

def fetch_current() -> tuple[list, str | None]:
    """Fetch startups.json from GitHub. Returns (data, sha)."""
    r = requests.get(API_BASE, headers=HEADERS, timeout=15)
    if r.status_code == 404:
        return [], None
    r.raise_for_status()
    resp    = r.json()
    content = base64.b64decode(resp['content']).decode('utf-8')
    return json.loads(content), resp['sha']


def push_updated(data: list, sha: str | None, message: str) -> None:
    """Push updated startups.json to GitHub."""
    content_b64 = base64.b64encode(
        json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    ).decode('utf-8')
    body = {'message': message, 'content': content_b64, 'branch': GITHUB_BRANCH}
    if sha:
        body['sha'] = sha
    r = requests.put(API_BASE, headers=HEADERS, json=body, timeout=15)
    r.raise_for_status()


# ─── Geocoding ─────────────────────────────────────────────────

def geocode(address: str) -> tuple[float | None, float | None]:
    """Geocode via Nominatim (Italy-scoped). Returns (lat, lng) or (None, None)."""
    url     = 'https://nominatim.openstreetmap.org/search'
    params  = {'q': address, 'format': 'json', 'limit': 1, 'countrycodes': 'it'}
    headers = {'User-Agent': 'milan-startup-map/1.0 (github.com/elenagetici01/milan-startup-map)'}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f'  ⚠  Geocode error for "{address}": {e}')
    return None, None


# ─── Excel reader ──────────────────────────────────────────────

def read_excel(path: str) -> list[tuple[str, str]]:
    """Read Excel. Returns list of (name, address) — skips header and blank rows."""
    wb   = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws   = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        name = str(row[0] or '').strip()
        addr = str(row[1] or '').strip() if len(row) > 1 else ''
        if not name or not addr:
            continue
        if i == 0 and name.lower() in HEADER_NAMES:
            continue
        rows.append((name, addr))
    wb.close()
    return rows


# ─── Core sync ─────────────────────────────────────────────────

def sync(excel_path: str = EXCEL_PATH) -> None:
    ts = datetime.now().strftime('%H:%M:%S')
    print(f'\n[{ts}] Starting sync…')

    # 1. Read Excel
    path = Path(excel_path)
    if not path.exists():
        print(f'  ⚠  Excel not found: {path}')
        return
    excel_rows = read_excel(str(path))
    print(f'  📊  {len(excel_rows)} rows in Excel')

    # 2. Fetch current JSON
    current_data, sha = fetch_current()
    print(f'  📡  {len(current_data)} startups in startups.json')

    existing_by_name = {s['name'].lower(): s for s in current_data}

    # 3. Find genuinely new entries
    new_entries = [
        {'name': name, 'address': addr}
        for name, addr in excel_rows
        if name.lower() not in existing_by_name
    ]

    # 4. Find existing entries missing coordinates (fill-in only, no overwrite)
    needs_geo = [s for s in current_data if s.get('lat') is None or s.get('lng') is None]

    if not new_entries and not needs_geo:
        print(f'  ✓   Nothing to update — {len(current_data)} startups already synced')
        return

    # 5. Geocode existing entries that lack coordinates
    if needs_geo:
        print(f'  🌍  Geocoding {len(needs_geo)} existing entry/entries missing coordinates…')
        for s in needs_geo:
            lat, lng = geocode(s['address'])
            s['lat'] = lat
            s['lng'] = lng
            coord_str = f'{lat:.5f}, {lng:.5f}' if lat is not None else 'NOT FOUND'
            print(f'      {s["name"]}: {coord_str}')
            time.sleep(1.1)

    # 6. Geocode new entries and assign IDs
    if new_entries:
        max_id = max((s.get('id', 0) for s in current_data), default=0)
        print(f'  🌍  Geocoding {len(new_entries)} new startup(s)…')
        for i, entry in enumerate(new_entries):
            lat, lng = geocode(entry['address'])
            entry['id']  = max_id + i + 1
            entry['lat'] = lat
            entry['lng'] = lng
            coord_str = f'{lat:.5f}, {lng:.5f}' if lat is not None else 'NOT FOUND'
            print(f'      + {entry["name"]}: {coord_str}')
            if i < len(new_entries) - 1:
                time.sleep(1.1)

    updated_data = current_data + new_entries

    # 7. Push to GitHub
    parts = []
    if new_entries:   parts.append(f'add {len(new_entries)} startup(s)')
    if needs_geo:     parts.append(f'geocode {len(needs_geo)} missing')
    commit_msg = 'data: ' + ', '.join(parts)

    push_updated(updated_data, sha, commit_msg)
    print(f'  ✓   Pushed — {commit_msg}')
    print(f'      Total startups: {len(updated_data)}')


# ─── Watch mode ────────────────────────────────────────────────

def watch(excel_path: str) -> None:
    """Watch the Excel file and sync on every change."""
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        print('ERROR: watchdog not installed. Run: pip install watchdog')
        sys.exit(1)

    resolved = Path(excel_path).resolve()
    print(f'👁  Watching {resolved}')
    print(f'    Press Ctrl+C to stop.\n')

    # Run once at startup
    sync(excel_path)

    last_run = [0.0]

    class Handler(FileSystemEventHandler):
        def on_modified(self, event):
            if Path(event.src_path).resolve() == resolved:
                now = time.time()
                if now - last_run[0] > 5:   # debounce — ignore rapid duplicate events
                    last_run[0] = now
                    sync(excel_path)

    observer = Observer()
    observer.schedule(Handler(), str(resolved.parent), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print('\nStopped.')
    observer.join()


# ─── Entry point ───────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='Sync milan_startups.xlsx → startups.json on GitHub')
    parser.add_argument('--once', action='store_true', help='Run once and exit (default: watch mode)')
    parser.add_argument('--excel', default=EXCEL_PATH, help=f'Path to Excel file (default: {EXCEL_PATH})')
    args = parser.parse_args()

    if not GITHUB_TOKEN:
        print('ERROR: GITHUB_TOKEN environment variable not set.')
        print('  Set it with: set GITHUB_TOKEN=ghp_yourtoken  (Windows)')
        print('           or: export GITHUB_TOKEN=ghp_yourtoken  (Unix)')
        sys.exit(1)

    if args.once:
        sync(args.excel)
    else:
        watch(args.excel)


if __name__ == '__main__':
    main()
