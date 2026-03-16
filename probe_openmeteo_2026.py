#!/usr/bin/env python3
"""
probe_openmeteo_2026.py — One-shot API probe to answer two questions:

  1. Does era5_seamless return all ERA5_VARS we currently fetch?
     (If yes → switch Call A model to era5_seamless, drop lag-handling machinery)

  2. Does the archive API return non-null sunshine_duration for alpine coords?
     (If yes → re-add to ERA5_VARS and delete Angstrom-Prescott derivation)
     Also compares the native value against our Angstrom-Prescott estimate.

  3. What is era5_seamless's actual data lag today?

Run:
    pip install requests
    python3 probe_openmeteo_2026.py

Expected runtime: ~5 seconds, 3 API calls.
"""

import math, requests
from datetime import date, timedelta

BASE     = "https://archive-api.open-meteo.com/v1/archive"
LAT, LON = 46.88, 10.50   # Nauders
ELEV     = 2750            # summit

# All variables Call A currently fetches (sunshine_duration excluded by design)
ERA5_VARS = [
    "temperature_2m_max", "temperature_2m_min", "apparent_temperature_min",
    "snowfall_sum", "precipitation_sum", "precipitation_hours",
    "shortwave_radiation_sum", "wind_speed_10m_max", "wind_gusts_10m_max",
    "weather_code",
]

TEST_DATE = "2024-01-15"   # mid-season, well within archive


def _get(params):
    r = requests.get(BASE, params=params, timeout=20)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}: {r.json().get('reason', r.text[:200])}"
    return r.json(), None


def angstrom_prescott_sunshine(srad_mj, doy, lat_deg=47.0):
    """Replicate clean_normalize.py derivation for comparison."""
    if not srad_mj or srad_mj <= 0:
        return 0.0
    dr   = 1 + 0.033 * math.cos(2 * math.pi * doy / 365)
    decl = 0.409 * math.sin(2 * math.pi * doy / 365 - 1.39)
    lat  = math.radians(lat_deg)
    ws   = math.acos(max(-1.0, min(1.0, -math.tan(lat) * math.tan(decl))))
    Ra   = 37.6 * dr * (ws*math.sin(lat)*math.sin(decl) + math.cos(lat)*math.cos(decl)*math.sin(ws))
    N    = 24 * ws / math.pi
    if Ra <= 0:
        return 0.0
    n    = N * (srad_mj / Ra - 0.25) / 0.50
    return max(0.0, min(N, n)) * 3600   # seconds


def probe_A():
    """era5_seamless: does it return all ERA5_VARS + sunshine_duration?"""
    print("=" * 60)
    print("PROBE A  era5_seamless — variable availability")
    print("=" * 60)
    data, err = _get({
        "latitude": LAT, "longitude": LON, "elevation": ELEV,
        "start_date": TEST_DATE, "end_date": TEST_DATE,
        "daily": ",".join(ERA5_VARS + ["sunshine_duration"]),
        "models": "era5_seamless",
        "timezone": "Europe/Berlin",
    })
    if err:
        print(f"  ERROR: {err}")
        return False, False

    daily = data.get("daily", {})
    all_vars_ok = True
    sun_ok      = False

    for var in ERA5_VARS + ["sunshine_duration"]:
        vals = daily.get(var, "MISSING")
        if vals == "MISSING":
            status, ok = "❌ NOT IN RESPONSE", False
        elif vals is None or (isinstance(vals, list) and all(v is None for v in vals)):
            status, ok = "⚠  NULL", False
        else:
            v      = vals[0] if isinstance(vals, list) else vals
            status = f"✓  {v}"
            ok     = True

        if var == "sunshine_duration":
            sun_ok = ok
            marker = " ← KEY QUESTION"
        else:
            if not ok:
                all_vars_ok = False
            marker = ""

        print(f"  {var:<35} {status}{marker}")

    print(f"\n  → All ERA5_VARS available under era5_seamless:  {'YES ✓' if all_vars_ok else 'NO ✗'}")
    print(f"  → sunshine_duration non-null in era5_seamless:  {'YES ✓' if sun_ok else 'NO ✗ (keep Angstrom-Prescott)'}")
    return all_vars_ok, sun_ok


def probe_B(sun_from_A):
    """Compare native sunshine_duration vs Angstrom-Prescott on best_match."""
    print("\n" + "=" * 60)
    print("PROBE B  sunshine_duration accuracy — native vs Angstrom-Prescott")
    print("=" * 60)

    data, err = _get({
        "latitude": LAT, "longitude": LON, "elevation": ELEV,
        "start_date": TEST_DATE, "end_date": TEST_DATE,
        "daily": "sunshine_duration,shortwave_radiation_sum",
        "models": "best_match",
        "timezone": "Europe/Berlin",
    })
    if err:
        print(f"  ERROR: {err}")
        return

    daily      = data.get("daily", {})
    sun_native = (daily.get("sunshine_duration") or [None])[0]
    srad       = (daily.get("shortwave_radiation_sum") or [None])[0]

    if sun_native:
        print(f"  sunshine_duration (native API):    {sun_native:.0f} s  ({sun_native/3600:.2f} h)")
    else:
        print(f"  sunshine_duration (native API):    NULL — API does not supply it for this location/model")

    if srad:
        ap_sec = angstrom_prescott_sunshine(srad, doy=15)
        print(f"  shortwave_radiation_sum:           {srad} MJ/m²")
        print(f"  Angstrom-Prescott derived:         {ap_sec:.0f} s  ({ap_sec/3600:.2f} h)")
        if sun_native and sun_native > 0:
            diff = abs(sun_native - ap_sec) / sun_native * 100
            verdict = "CLOSE — derivation acceptable" if diff < 15 else "DIVERGES — native preferred"
            print(f"  Difference:                        {diff:.1f}%  → {verdict}")
    else:
        print(f"  shortwave_radiation_sum:           NULL")

    if not sun_native and not sun_from_A:
        print("\n  CONCLUSION: Neither model returns sunshine_duration reliably.")
        print("  Keep Angstrom-Prescott derivation from shortwave_radiation_sum.")


def probe_C():
    """era5_seamless recency — what is the effective lag today?"""
    print("\n" + "=" * 60)
    print("PROBE C  era5_seamless data lag — how recent is the data today?")
    print("=" * 60)

    today = date.today()
    data, err = _get({
        "latitude": LAT, "longitude": LON, "elevation": ELEV,
        "start_date": str(today - timedelta(days=10)),
        "end_date":   str(today),
        "daily": "temperature_2m_max",
        "models": "era5_seamless",
        "timezone": "Europe/Berlin",
    })
    if err:
        print(f"  ERROR: {err}")
        return

    times = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    pairs = list(zip(times, temps))

    print(f"  Requested: {today - timedelta(days=10)} → {today}")
    for t, v in pairs:
        flag = "✓" if v is not None else "✗ null"
        print(f"    {t}  {flag}  {f'{v}°C' if v is not None else ''}")

    non_null = [(t, v) for t, v in pairs if v is not None]
    if non_null:
        newest = date.fromisoformat(non_null[-1][0])
        lag    = (today - newest).days
        print(f"\n  Most recent non-null date: {newest}")
        print(f"  Effective lag:             {lag} day(s)")
        if lag <= 2:
            print("  → CONFIRMED: era5_seamless lag ≤ 2 days. Safe to remove ERA5LagError machinery.")
        elif lag <= 7:
            print("  → lag still present but reduced. Verify on multiple days before removing lag handling.")
        else:
            print("  → lag > 7 days. era5_seamless not yet an improvement over current best_match approach.")
    else:
        print("  → No non-null values in the last 10 days — something is wrong.")


def summary(all_vars_ok, sun_ok):
    print("\n" + "=" * 60)
    print("SUMMARY — What to change in the pipeline")
    print("=" * 60)

    if all_vars_ok:
        print("  ✓  Switch Call A `models` from `best_match` to `era5_seamless`")
        print("     → simplifies lag handling significantly")
    else:
        print("  ✗  era5_seamless missing some ERA5_VARS — keep `best_match` for Call A")

    if sun_ok:
        print("  ✓  Re-add `sunshine_duration` to ERA5_VARS")
        print("     → remove Angstrom-Prescott derivation from clean_normalize.py")
        print("     → simpler, API-native, consistent derivation method")
    else:
        print("  ✗  sunshine_duration not reliably populated — keep Angstrom-Prescott")

    print("\n  In both cases: Call B (hourly snow_depth via era5_land) stays unchanged.")
    print("  snow_depth has no daily aggregate and no era5_seamless coverage.")


if __name__ == "__main__":
    all_vars_ok, sun_ok = probe_A()
    probe_B(sun_ok)
    probe_C()
    summary(all_vars_ok, sun_ok)
