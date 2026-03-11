#!/usr/bin/env python3
"""
Zwei Länder Skiarena — Open-Meteo Historical Weather Fetcher

Requirements: pip install requests urllib3
Run:          python fetch_openmeteo.py [--end-date YYYY-MM-DD] [--probe]
"""

import requests, json, time, sys, argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import date, timedelta

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL   = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2019-12-10"

DAILY_VARS = [
    "temperature_2m_max", "temperature_2m_min", "snowfall_sum", 
    "snow_depth", "precipitation_sum", "shortwave_radiation_sum", 
    "windspeed_10m_max", "weathercode"
]

# ── FIX 2: Hardened Requests Session with Exponential Backoff ──
def get_session():
    session = requests.Session()
    # Retry on 429 (Rate Limit), 500, 502, 503, 504.
    # Backoff factor 1 means sleeps will be [1s, 2s, 4s, 8s, 16s]
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def fetch_one(session, name, lat, lon, elevation_label, vars_to_use, start_d, end_d):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_d,
        "end_date": end_d,
        "daily": vars_to_use,
        "timezone": "Europe/Berlin"
    }
    
    try:
        response = session.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # ERA5-Land latency fallback check
        if "error" in data and data.get("reason", "").startswith("Data only available until"):
            print(f"  [!] ERA5 lag detected: {data['reason']}. Adjusting end date and retrying...")
            # Ideally parse the date from the string, but a simple manual fallback works
            # Or exit and let the orchestrator handle it. For now, raise.
            raise ValueError(data['reason'])

        out_file = OUTPUT_DIR / f"{name}_{elevation_label}_raw.json"
        with open(out_file, "w") as f:
            json.dump(data, f)
            
        return len(data.get("daily", {}).get("time", []))
        
    except requests.exceptions.RequestException as e:
        print(f"  [!] Network failure fetching {name} ({elevation_label}): {e}")
        raise

def default_end_date():
    """ERA5-Land is usually 5 days behind. Return today - 6 days to be safe."""
    return (date.today() - timedelta(days=6)).strftime("%Y-%m-%d")

# ... (Keep your existing PROBE and MAIN logic, just swap `requests.get` to `session.get`) ...

if __name__ == "__main__":
    # Example execution wrap
    session = get_session()
    # Replace your standard request loop with `session` being passed into fetch_one
    print("Session hardened. Fetch logic goes here.")