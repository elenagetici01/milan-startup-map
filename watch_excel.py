#!/usr/bin/env python3
"""
watch_excel.py — Daily sync: milan_startups.xlsx → startups.json (GitHub) → mappa del sito

Excel è l'unica fonte di verità. Ogni run confronta Excel con startups.json e applica:
  ADD    — riga in Excel (ID non presente in JSON)    → geocodifica + aggiunge al JSON
  UPDATE — riga in entrambi, ma dati cambiati          → aggiorna JSON (re-geocodifica se cambia indirizzo)
  REMOVE — riga in JSON (ID non presente in Excel)     → rimuove dal JSON
  KEEP   — riga in entrambi, dati identici             → preserva tutto (lat, lng, logo, notes)

Il sito legge startups.json al caricamento — le modifiche sono visibili subito per tutti gli utenti.

Struttura Excel (riga 1 = intestazione opzionale):
  Colonna A = ID numerico  (identificatore univoco)
  Colonna B = Nome startup
  Colonna C = Indirizzo

Usage:
    python watch_excel.py                      # modalità giornaliera (default: ore 08:00)
    python watch_excel.py --time 06:30         # orario personalizzato
    python watch_excel.py --once               # esegui subito una volta e chiudi
    python watch_excel.py --excel path/to/file.xlsx

Env vars richieste:
    GITHUB_TOKEN   token di accesso personale con scope "repo" (write)
    GITHUB_REPO    owner/repo  (default: elenagetici01/milan-startup-map)
    EXCEL_PATH     percorso .xlsx  (default: milan_startups.xlsx)
"""

import os, sys, json, time, base64, argparse, requests, openpyxl
from pathlib import Path
from datetime import datetime, timedelta

# Forza UTF-8 su Windows (necessario per emoji nei print)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# ── Configurazione ─────────────────────────────────────────────────────────────
GITHUB_TOKEN  = os.environ.get('GITHUB_TOKEN', '')
GITHUB_REPO   = os.environ.get('GITHUB_REPO', 'elenagetici01/milan-startup-map')
GITHUB_FILE   = 'startups.json'
GITHUB_BRANCH = 'main'
EXCEL_PATH    = os.environ.get('EXCEL_PATH', 'milan_startups.xlsx')
DEFAULT_TIME  = '08:00'   # orario sync giornaliero

# Colonne Excel (0-indexed)
COL_ID      = 0   # A — ID numerico identificatore
COL_NAME    = 1   # B — nome startup
COL_ADDRESS = 2   # C — indirizzo

API_URL = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}'
GH_HEADERS = {
    'Authorization': f'Bearer {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
}


# ── GitHub API ─────────────────────────────────────────────────────────────────

def gh_fetch() -> tuple[list, str | None]:
    """Scarica startups.json da GitHub. Ritorna (lista, sha)."""
    r = requests.get(API_URL, headers=GH_HEADERS, timeout=15)
    if r.status_code == 404:
        return [], None
    r.raise_for_status()
    body = r.json()
    return json.loads(base64.b64decode(body['content']).decode('utf-8')), body['sha']


def gh_push(data: list, sha: str | None, message: str) -> None:
    """Committa startups.json aggiornato su GitHub."""
    body = {
        'message': message,
        'branch':  GITHUB_BRANCH,
        'content': base64.b64encode(
            json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        ).decode('utf-8'),
    }
    if sha:
        body['sha'] = sha
    r = requests.put(API_URL, headers=GH_HEADERS, json=body, timeout=15)
    r.raise_for_status()


# ── Geocoding ──────────────────────────────────────────────────────────────────

def geocode(address: str) -> tuple[float | None, float | None]:
    """Geocodifica via Nominatim (scope Italia). Ritorna (lat, lng) o (None, None)."""
    try:
        r = requests.get(
            'https://nominatim.openstreetmap.org/search',
            params={'q': address, 'format': 'json', 'limit': 1, 'countrycodes': 'it'},
            headers={'User-Agent': 'milan-startup-map/1.0 (github.com/elenagetici01/milan-startup-map)'},
            timeout=10,
        )
        hits = r.json()
        if hits:
            return float(hits[0]['lat']), float(hits[0]['lon'])
    except Exception as e:
        print(f'    ⚠  Geocode error "{address}": {e}')
    return None, None


# ── Lettura Excel ──────────────────────────────────────────────────────────────

def read_excel(path: str) -> list[dict]:
    """
    Legge il foglio attivo. Ritorna lista di {id, name, address}.
    Col A = ID numerico, Col B = nome, Col C = indirizzo.
    Salta righe vuote e riga di intestazione (se la cella A1 non è un numero).
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        raw_id   = row[COL_ID]   if len(row) > COL_ID   else None
        raw_name = row[COL_NAME] if len(row) > COL_NAME else None
        raw_addr = row[COL_ADDRESS] if len(row) > COL_ADDRESS else None

        # Salta righe senza dati essenziali
        if raw_id is None or raw_name is None or raw_addr is None:
            continue

        # Salta intestazione: riga 0 dove l'ID non è un numero
        if i == 0:
            try:
                int(str(raw_id).strip())
            except ValueError:
                continue

        try:
            startup_id = int(str(raw_id).strip())
        except ValueError:
            print(f'  ⚠  Riga {i+1}: ID "{raw_id}" non è un numero — riga saltata')
            continue

        name = str(raw_name).strip()
        addr = str(raw_addr).strip()
        if not name or not addr:
            continue

        rows.append({'id': startup_id, 'name': name, 'address': addr})

    wb.close()
    return rows


# ── Sync principale ────────────────────────────────────────────────────────────

def sync(excel_path: str = EXCEL_PATH) -> None:
    print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Avvio sync…')

    # 1. Leggi Excel
    path = Path(excel_path)
    if not path.exists():
        print(f'  ⚠  File Excel non trovato: {path}')
        return

    excel_rows = read_excel(str(path))
    excel_by_id: dict[int, dict] = {r['id']: r for r in excel_rows}
    print(f'  📊  Excel: {len(excel_rows)} righe  (ID: {sorted(excel_by_id.keys())})')

    # 2. Scarica JSON attuale
    json_data, sha = gh_fetch()
    json_by_id: dict[int, dict] = {s['id']: s for s in json_data}
    print(f'  📡  JSON:  {len(json_data)} startup  (ID: {sorted(json_by_id.keys())})')

    # ── 2b. GEOCODIFICA COORDINATE MANCANTI ──────────────────────────────────
    # Geocodifica le entry già presenti nel JSON con lat/lng ancora null.
    # Succede quando una startup è inserita via API o manualmente senza coordinate.
    # → scrive lat e lng direttamente nell'oggetto JSON e fa push immediato.
    # Zero chiamate API se tutte le coordinate sono già valorizzate.
    missing_coords = [s for s in json_data if s.get('lat') is None or s.get('lng') is None]
    coords_fixed = False
    if missing_coords:
        print(f'  🌍  Coordinate mancanti ({len(missing_coords)} entry) — geocodifica in corso…')
        for i, s in enumerate(missing_coords):
            lat, lng = geocode(s['address'])
            s['lat'] = lat   # scritto direttamente nel dict → aggiorna json_data
            s['lng'] = lng
            coord = f'{lat:.5f}, {lng:.5f}' if lat else 'NOT FOUND'
            print(f'      ID {s["id"]} "{s["name"]}": {coord}')
            if i < len(missing_coords) - 1:
                time.sleep(1.1)   # rispetta rate limit Nominatim (1 req/s)
        coords_fixed = any(s['lat'] is not None for s in missing_coords)
    # ── FINE GEOCODIFICA COORDINATE MANCANTI ─────────────────────────────────

    excel_ids = set(excel_by_id.keys())
    json_ids  = set(json_by_id.keys())

    # 3. Classifica le differenze
    ids_to_add    = excel_ids - json_ids
    ids_to_remove = json_ids  - excel_ids
    ids_to_check  = excel_ids & json_ids   # presenti in entrambi → verifica modifiche

    to_add    = [excel_by_id[i] for i in sorted(ids_to_add)]
    to_remove = [json_by_id[i]  for i in sorted(ids_to_remove)]
    to_update = []

    for startup_id in sorted(ids_to_check):
        ex  = excel_by_id[startup_id]
        jsn = json_by_id[startup_id]
        name_changed    = ex['name'].strip()    != jsn.get('name', '').strip()
        address_changed = ex['address'].strip() != jsn.get('address', '').strip()
        if name_changed or address_changed:
            to_update.append((jsn, ex, address_changed))

    print(f'  ➕  Da aggiungere : {len(to_add)}')
    print(f'  ✏   Da aggiornare : {len(to_update)}')
    print(f'  🗑   Da rimuovere  : {len(to_remove)}')
    print(f'  ✓   Invariati     : {len(ids_to_check) - len(to_update)}')

    if not to_add and not to_update and not to_remove and not coords_fixed:
        print('  ✓   JSON già sincronizzato — nessuna modifica necessaria')
        return

    # 4. Applica UPDATES (modifica dati, re-geocodifica se indirizzo cambiato)
    if to_update:
        print(f'\n  ✏   Aggiornamento {len(to_update)} startup…')
        for jsn_entry, excel_entry, addr_changed in to_update:
            old_name = jsn_entry.get('name')
            new_name = excel_entry['name']
            new_addr = excel_entry['address']

            jsn_entry['name']    = new_name
            jsn_entry['address'] = new_addr

            if addr_changed:
                lat, lng = geocode(new_addr)
                jsn_entry['lat'] = lat
                jsn_entry['lng'] = lng
                coord = f'{lat:.5f}, {lng:.5f}' if lat else 'NOT FOUND'
                print(f'      ID {jsn_entry["id"]} "{old_name}" → "{new_name}" | indirizzo → {coord}')
                time.sleep(1.1)
            else:
                print(f'      ID {jsn_entry["id"]} "{old_name}" → "{new_name}" (solo nome)')

    # 5. Geocodifica e aggiungi nuove startup
    if to_add:
        print(f'\n  ➕  Geocodifica {len(to_add)} nuova/e startup…')
        for i, row in enumerate(to_add):
            lat, lng = geocode(row['address'])
            row['lat'] = lat
            row['lng'] = lng
            coord = f'{lat:.5f}, {lng:.5f}' if lat else 'NOT FOUND'
            print(f'      + ID {row["id"]} "{row["name"]}": {coord}')
            if i < len(to_add) - 1:
                time.sleep(1.1)

    # 6. Log rimozioni
    if to_remove:
        print(f'\n  🗑   Rimozione {len(to_remove)} startup…')
        for s in to_remove:
            print(f'      - ID {s["id"]} "{s["name"]}"')

    # 7. Costruisci lista finale: JSON attuale (senza rimossi) + nuovi
    removed_ids = {s['id'] for s in to_remove}
    result = [s for s in json_data if s['id'] not in removed_ids]
    result += to_add
    result.sort(key=lambda s: s['id'])   # ordine per ID

    # 8. Push su GitHub
    parts = []
    if to_add:       parts.append(f'add {len(to_add)}')
    if to_update:    parts.append(f'update {len(to_update)}')
    if to_remove:    parts.append(f'remove {len(to_remove)}')
    if coords_fixed: parts.append(f'geocode {len(missing_coords)}')
    commit_msg = 'data: ' + ', '.join(parts)

    gh_push(result, sha, commit_msg)
    print(f'\n  ✅  Push completato — {commit_msg}  ({len(result)} startup totali)')
    print(f'      Il sito aggiornerà i pin al prossimo caricamento della pagina.')


# ── Scheduler giornaliero ──────────────────────────────────────────────────────

def next_run_at(hhmm: str) -> datetime:
    """Calcola il prossimo datetime per l'orario HH:MM dato."""
    h, m = map(int, hhmm.split(':'))
    now  = datetime.now()
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def run_daily(excel_path: str, run_time: str) -> None:
    """
    Esegue sync() ogni giorno all'orario specificato (formato HH:MM).
    Esegue subito alla prima avvio, poi rispetta l'orario configurato.
    """
    print(f'⏰  Scheduler giornaliero attivo — sync ogni giorno alle {run_time}')
    print(f'    Excel: {Path(excel_path).resolve()}')
    print(f'    Ctrl+C per fermare.\n')

    # Prima sync immediata
    sync(excel_path)

    while True:
        target = next_run_at(run_time)
        wait_s = (target - datetime.now()).total_seconds()
        print(f'\n  💤  Prossima sync: {target.strftime("%Y-%m-%d %H:%M:%S")} '
              f'(tra {wait_s/3600:.1f} ore)')
        try:
            time.sleep(wait_s)
        except KeyboardInterrupt:
            print('\nScheduler fermato.')
            sys.exit(0)
        sync(excel_path)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Sync Excel → startups.json su GitHub (add / update / remove) — run giornaliero'
    )
    parser.add_argument('--once',  action='store_true',
                        help='Esegui subito una volta e chiudi')
    parser.add_argument('--time',  default=DEFAULT_TIME,
                        metavar='HH:MM',
                        help=f'Orario sync giornaliero (default: {DEFAULT_TIME})')
    parser.add_argument('--excel', default=EXCEL_PATH,
                        help=f'Percorso file Excel (default: {EXCEL_PATH})')
    args = parser.parse_args()

    if not GITHUB_TOKEN:
        sys.exit(
            'ERRORE: variabile GITHUB_TOKEN non impostata.\n'
            '  Windows : set GITHUB_TOKEN=ghp_iltuotoken\n'
            '  Mac/Linux: export GITHUB_TOKEN=ghp_iltuotoken'
        )

    if args.once:
        sync(args.excel)
    else:
        run_daily(args.excel, args.time)


if __name__ == '__main__':
    main()
