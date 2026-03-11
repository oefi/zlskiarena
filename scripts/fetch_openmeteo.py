#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher

KEY FIX: The Open-Meteo archive API requires `daily` as REPEATED query params:
  daily=var1&daily=var2  (pass a list to requests)
NOT a comma-joined string:
  daily=var1,var2        (this causes 400 "Cannot initialize ForecastVariableDaily")

Requirements: pip install requests
Run:          python fetch_openmeteo.py [--end-date YYYY-MM-DD] [--probe]
"""

import requests, json, time, sys, argparse
from pathlib import Path
from datetime import date, timedelta

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL   = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2019-12-10"

# ── Variables — must be a LIST, not a joined string ───────────────────────────
DAILY_VARS = [
    "temperature_2m_max",       # °C
    "temperature_2m_min",       # °C
    "snowfall_sum",             # cm
    "snow_depth",               # m
    "precipitation_sum",        # mm
    "shortwave_radiation_sum",  # MJ/m² (sunshine proxy; sunshine_duration not in ERA5-Land)
    "windspeed_10m_max",        # km/h
    "weathercode",              # WMO code
    # REMOVED — not available in ERA5-Land archive:
    #   uv_index_max     (forecast-only endpoint)
    #   rain_sum         (not in ERA5-Land)
    #   sunshine_duration (ERA5-Land uses shortwave_radiation_sum)
]

RESORTS = [
    ("nauders",    46.8897, 10.5042, 1400, 2750),
    ("schoeneben", 46.8067, 10.5233, 1460, 2390),
    ("watles",     46.6869, 10.5517, 1500, 2550),
    ("sulden",     46.5260, 10.5752, 1900, 3250),
    ("trafoi",     46.5581, 10.6008, 1540, 2800),
]

def default_end_date():
    today = date.today()
    m, d  = today.month, today.day
    in_season = (m == 12 and d >= 10) or (1 <= m <= 3) or (m == 4 and d <= 10)
    if in_season:
        return str(today - timedelta(days=7))
    yr = today.year if today > date(today.year, 4, 10) else today.year - 1
    return f"{yr}-04-10"

# ── Variable probe: test each var individually ────────────────────────────────

def probe_variables(lat, lon, elevation):
    """Hit the API with each variable alone on a 3-day window. Returns working list."""
    test_start, test_end = "2024-01-15", "2024-01-17"
    working, failing = [], []
    print(f"\n  Probing {len(DAILY_VARS)} variables ({test_start} → {test_end}) ...")
    print(f"  {'Variable':<35} Status")
    print(f"  {'-'*50}")
    for var in DAILY_VARS:
        try:
            r = requests.get(BASE_URL, params={
                "latitude": lat, "longitude": lon, "elevation": elevation,
                "start_date": test_start, "end_date": test_end,
                "daily": [var], "timezone": "UTC",
            }, timeout=15)
            if r.status_code == 200:
                working.append(var)
                print(f"  {var:<35} ✓ OK")
            else:
                failing.append(var)
                reason = r.json().get("reason", r.text[:80]) if r.content else r.status_code
                print(f"  {var:<35} ✗ {r.status_code}: {reason}")
        except Exception as ex:
            failing.append(var)
            print(f"  {var:<35} ✗ {ex}")
        time.sleep(0.3)
    print(f"\n  OK: {len(working)}  FAIL: {len(failing)}")
    if failing:
        print(f"  Failing vars: {failing}")
    return working

# ── Single resort×elevation fetch ─────────────────────────────────────────────

def fetch_one(resort, lat, lon, elevation, label, vars_list, start, end):
    params = {
        "latitude":        lat,
        "longitude":       lon,
        "elevation":       elevation,
        "start_date":      start,
        "end_date":        end,
        "daily":           vars_list,   # LIST → daily=v1&daily=v2&...
        "timezone":        "UTC",
        "wind_speed_unit": "kmh",
    }
    tag = f"{resort} ({label}, {elevation}m)"
    print(f"  {tag} ...", end=" ", flush=True)

    r = requests.get(BASE_URL, params=params, timeout=60)
    if r.status_code != 200:
        try:
            body = r.json()
            reason = body.get("reason", str(body))
        except Exception:
            reason = r.text[:300]
        print(f"\n  ✗ HTTP {r.status_code}: {reason}")
        r.raise_for_status()

    data   = r.json()
    n_days = len(data["daily"]["time"])
    got    = [k for k in data["daily"] if k != "time"]
    print(f"{n_days} days  [{len(got)} vars]")

    missing = [v for v in vars_list if v not in got]
    if missing:
        print(f"    ⚠ Requested but not returned: {missing}")

    out = OUTPUT_DIR / f"{resort}_{label}_raw.json"
    out.write_text(json.dumps(data))
    return n_days

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--end-date", default=None)
    p.add_argument("--probe", action="store_true",
                   help="Test each variable against the real API before fetching")
    args = p.parse_args()

    end_date    = args.end_date or default_end_date()
    vars_to_use = DAILY_VARS

    print("=" * 60)
    print("Zwei Länder Skiarena — Open-Meteo Data Fetch")
    print(f"Period:  {START_DATE} → {end_date}")
    print(f"Resorts: {len(RESORTS)} × 2 = {len(RESORTS)*2} calls")
    print(f"Vars ({len(vars_to_use)}): {', '.join(vars_to_use)}")
    print("=" * 60)

    if args.probe:
        name, lat, lon, base_m, _ = RESORTS[0]
        print(f"\n[PROBE on {name} base {base_m}m]")
        vars_to_use = probe_variables(lat, lon, base_m)
        if not vars_to_use:
            sys.exit("No variables worked — check network/API status.")
        print(f"\nProceeding with {len(vars_to_use)} confirmed variables.")

    summary = []
    for name, lat, lon, base_m, summit_m in RESORTS:
        print(f"\n[{name.upper()}]")
        n = fetch_one(name, lat, lon, base_m,   "base",   vars_to_use, START_DATE, end_date)
        summary.append((name, "base",   base_m,   n))
        time.sleep(0.6)
        n = fetch_one(name, lat, lon, summit_m, "summit", vars_to_use, START_DATE, end_date)
        summary.append((name, "summit", summit_m, n))
        time.sleep(0.6)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"{'Resort':<12} {'Role':<8} {'Elev m':>8} {'Days':>6}")
    print("-" * 38)
    for nm, role, elev, days in summary:
        print(f"{nm:<12} {role:<8} {elev:>8} {days:>6}")
    print("=" * 60)
    print(f"Output: {OUTPUT_DIR}  →  run scripts/clean_normalize.py next")

if __name__ == "__main__":
    main()
