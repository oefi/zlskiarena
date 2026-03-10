#!/usr/bin/env python3
"""
build_dashboard.py — injects enriched_data.json into dashboard_template.html
Output: nauders_dashboard.html (self-contained, offline-capable)
"""
from pathlib import Path
import json, sys

BASE    = Path(__file__).parent
TMPL    = BASE / "dashboard_template.html"
DATA    = BASE / "data" / "processed" / "enriched_data.json"
OUT     = BASE / "nauders_dashboard.html"

def main():
    print("Building dashboard…")
    template = TMPL.read_text(encoding="utf-8")
    if "__SKI_DATA_PLACEHOLDER__" not in template:
        sys.exit("ERROR: placeholder not found in template")

    data_str = DATA.read_text(encoding="utf-8")   # already minified
    html = template.replace("__SKI_DATA_PLACEHOLDER__", data_str)
    OUT.write_text(html, encoding="utf-8")

    size_kb = OUT.stat().st_size / 1024
    print(f"  → {OUT.name}  ({size_kb:.0f} KB)")
    print("Done.")

if __name__ == "__main__":
    main()
