#!/usr/bin/env python3
"""
build_dashboard.py — injects JSON data into dashboard_template.html
Output: nauders_dashboard.html (renamed to index.html in GitHub Actions)
"""
from pathlib import Path
import sys

BASE     = Path(__file__).parent
TMPL     = BASE / "dashboard_template.html"
# Rerouted from the dead enriched_data.json to the raw normalized output
DATA     = BASE / "data" / "processed" / "master_data.json"
FORECAST = BASE / "data" / "processed" / "forecast_data.json"
OUT      = BASE / "nauders_dashboard.html"

def main():
    print("Building dashboard…")
    template = TMPL.read_text(encoding="utf-8")

    if "__SKI_DATA_PLACEHOLDER__" not in template:
        sys.exit("ERROR: __SKI_DATA_PLACEHOLDER__ not found in template")

    # Inject History
    if not DATA.exists():
        sys.exit(f"ERROR: {DATA.name} missing. Run clean_normalize.py first.")

    data_str = DATA.read_text(encoding="utf-8")
    html = template.replace("__SKI_DATA_PLACEHOLDER__", data_str)

    # Inject Forecast
    if "__FORECAST_DATA_PLACEHOLDER__" in html:
        if FORECAST.exists():
            fc_str = FORECAST.read_text(encoding="utf-8")
            html = html.replace("__FORECAST_DATA_PLACEHOLDER__", fc_str)
            print("  ✓ Injected high-res forecast data")
        else:
            print("  ⚠ No forecast data found, injecting empty object.")
            html = html.replace("__FORECAST_DATA_PLACEHOLDER__", '{"error": "no forecast generated"}')

    OUT.write_text(html, encoding="utf-8")
    print(f"  ✓ Dashboard baked to {OUT.name}")

if __name__ == "__main__":
    main()
