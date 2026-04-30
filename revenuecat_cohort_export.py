"""
RevenueCat Cohort Explorer — Data Export Script
================================================
Fetches Realized LTV and Proceeds from the cohort_explorer chart
for daily cohorts across countries and platforms, then writes to CSV.

Requirements:
    pip install requests python-dotenv

Usage:
    1. Copy .env.example → .env and fill in your credentials
    2. python src/revenuecat_cohort_export.py

API KEY NOTE:
    This script uses the RevenueCat REST API v2.
    v1 keys (sk_...) do NOT work with v2 endpoints.
    Go to: RevenueCat Dashboard → Project Settings → API Keys → + New
    Create a v2 key with permission: charts_metrics:charts:read
"""

import json
import time
import csv
import sys
import os
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  (all secrets loaded from .env)
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ID     = os.getenv("RC_PROJECT_ID")
SECRET_API_KEY = os.getenv("RC_SECRET_API_KEY")

if not PROJECT_ID or not SECRET_API_KEY:
    sys.exit("[FATAL] RC_PROJECT_ID and RC_SECRET_API_KEY must be set in your .env file.")

START_DATE = os.getenv("RC_START_DATE", "2024-01-01")
END_DATE   = os.getenv("RC_END_DATE",   "2024-01-31")

TARGET_COUNTRIES = {
    "US": "United States",
    "GB": "United Kingdom",
    "AU": "Australia",
    "CA": "Canada",
}

TARGET_PLATFORMS = {
    "iOS":     "app_store",
    "Android": "play_store",
}

LTV_WINDOWS      = [0, 7, 30, 90, 180, 365]
PROCEEDS_WINDOWS = [0, 7, 30, 90, 180, 365]

RATE_LIMIT_SLEEP = 4.0   # RevenueCat v2: 15 req/min → 4s gap keeps us safe

BASE_URL   = "https://api.revenuecat.com/v2"
CHART_NAME = "cohort_explorer"

OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "cohort_export.csv"
)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def build_headers() -> dict:
    return {
        "Authorization": f"Bearer {SECRET_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def api_get(path: str, params: dict | None = None) -> dict:
    """GET request against the RevenueCat v2 API. Raises RuntimeError on failure."""
    url = f"{BASE_URL}{path}"
    response = requests.get(url, headers=build_headers(), params=params, timeout=30)
    if not response.ok:
        raise RuntimeError(
            f"API request failed\n"
            f"  URL    : {url}\n"
            f"  Params : {params}\n"
            f"  Status : {response.status_code}\n"
            f"  Body   : {response.text}"
        )
    return response.json()


def fetch_chart_options() -> dict:
    """Fetch available filters / selectors for cohort_explorer."""
    print(f"[INFO] Fetching chart options for '{CHART_NAME}' ...")
    return api_get(
        f"/projects/{PROJECT_ID}/charts/{CHART_NAME}/options",
        params={"realtime": "true"},
    )


def fetch_cohort_data(country: str, platform_store: str, measure_selector: str) -> dict:
    """
    Fetch cohort_explorer data for a single country + platform + measure.

    Args:
        country:           ISO country code (e.g. 'US')
        platform_store:    RevenueCat store value ('app_store' | 'play_store')
        measure_selector:  Measure key from user_selectors (e.g. 'realized_ltv')
    """
    filters   = json.dumps([
        {"name": "first_country", "values": [country]},
        {"name": "store",         "values": [platform_store]},
    ])
    selectors = json.dumps({"measure": measure_selector})

    params = {
        "realtime":   "true",
        "start_date": START_DATE,
        "end_date":   END_DATE,
        "resolution": "0",
        "filters":    filters,
        "selectors":  selectors,
    }
    return api_get(f"/projects/{PROJECT_ID}/charts/{CHART_NAME}", params=params)


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE PARSING
# ─────────────────────────────────────────────────────────────────────────────

def extract_window_value(
    cohort_data_point: dict | list,
    window_days: int,
    measures_meta: list,
) -> float | None:
    """
    Extract value for a specific time-window from a single cohort data point.

    Handles two response shapes from RevenueCat v2:
      Shape A: dict  → {"periods": {"P7D": 1.23}}
      Shape B: list  → [cohort_date, val_0d, val_7d, ...]
    """
    period_key = f"P{window_days}D" if window_days > 0 else "P0D"

    if isinstance(cohort_data_point, dict):
        periods = cohort_data_point.get("periods", {})
        if period_key in periods:
            return periods[period_key]
        return cohort_data_point.get(str(window_days)) or cohort_data_point.get(window_days)

    if isinstance(cohort_data_point, list):
        window_index = LTV_WINDOWS.index(window_days) if window_days in LTV_WINDOWS else None
        if window_index is not None and len(cohort_data_point) > window_index + 1:
            return cohort_data_point[window_index + 1]

    return None


def parse_cohort_response(response: dict, window_list: list[int]) -> dict:
    """
    Parse a full chart response into:
        { "YYYY-MM-DD": {0: 1.23, 7: 4.56, ...}, ... }
    """
    result: dict = {}
    values        = response.get("values", [])
    measures_meta = response.get("measures", [])

    for entry in values:
        if isinstance(entry, dict):
            cohort_ts = entry.get("cohort_date") or entry.get("date") or entry.get("timestamp")
        elif isinstance(entry, list) and len(entry) > 0:
            cohort_ts = entry[0]
        else:
            continue

        cohort_date_str = (
            date.fromtimestamp(cohort_ts / 1000).isoformat()
            if isinstance(cohort_ts, (int, float))
            else str(cohort_ts)[:10]
        )

        result[cohort_date_str] = {
            w: extract_window_value(entry, w, measures_meta) for w in window_list
        }

    return result


# ─────────────────────────────────────────────────────────────────────────────
# DATE RANGE
# ─────────────────────────────────────────────────────────────────────────────

def date_range(start: str, end: str) -> list[str]:
    s    = date.fromisoformat(start)
    e    = date.fromisoformat(end)
    days = (e - s).days + 1
    return [(s + timedelta(days=i)).isoformat() for i in range(days)]


# ─────────────────────────────────────────────────────────────────────────────
# CSV OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def build_csv_headers() -> list[str]:
    return (
        ["cohort_date", "country", "country_name", "platform"]
        + [f"realized_ltv_{w}d" for w in LTV_WINDOWS]
        + [f"proceeds_{w}d"     for w in PROCEEDS_WINDOWS]
    )


def write_csv(rows: list[dict]) -> None:
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=build_csv_headers(), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n[SUCCESS] CSV written → {OUTPUT_FILE}  ({len(rows)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65)
    print("  RevenueCat Cohort Explorer — Export Script")
    print("=" * 65)
    print(f"  Period   : {START_DATE} → {END_DATE}")
    print(f"  Countries: {', '.join(TARGET_COUNTRIES.keys())}")
    print(f"  Platforms: {', '.join(TARGET_PLATFORMS.keys())}")
    print("=" * 65)

    try:
        options = fetch_chart_options()
    except RuntimeError as exc:
        print(f"\n[FATAL] Could not fetch chart options.\n{exc}")
        print("\nHint: 401/403 usually means a v1 API key. Create a v2 key with "
              "'charts_metrics:charts:read' permission.")
        sys.exit(1)

    time.sleep(RATE_LIMIT_SLEEP)

    selectors_meta  = options.get("user_selectors", {})
    measure_options = {
        opt["display_name"].lower(): opt["id"]
        for opt in selectors_meta.get("measure", {}).get("options", [])
    }

    ltv_selector      = measure_options.get("realized ltv", "realized_ltv")
    proceeds_selector = measure_options.get("proceeds",      "proceeds")
    print(f"[INFO] LTV selector      : {ltv_selector!r}")
    print(f"[INFO] Proceeds selector : {proceeds_selector!r}")

    all_dates = date_range(START_DATE, END_DATE)
    all_rows:  list[dict] = []

    combos = [
        (cc, cn, pl, sv)
        for cc, cn in TARGET_COUNTRIES.items()
        for pl, sv in TARGET_PLATFORMS.items()
    ]
    total_calls = len(combos) * 2
    call_n      = 0

    for country_code, country_name, platform_label, store_value in combos:
        label = f"{country_code} / {platform_label}"

        print(f"\n[{call_n+1}/{total_calls}] Fetching LTV      {label}")
        ltv_resp    = fetch_cohort_data(country_code, store_value, ltv_selector)
        call_n     += 1
        ltv_by_date = parse_cohort_response(ltv_resp, LTV_WINDOWS)
        time.sleep(RATE_LIMIT_SLEEP)

        print(f"[{call_n+1}/{total_calls}] Fetching Proceeds {label}")
        proc_resp    = fetch_cohort_data(country_code, store_value, proceeds_selector)
        call_n      += 1
        proc_by_date = parse_cohort_response(proc_resp, PROCEEDS_WINDOWS)
        time.sleep(RATE_LIMIT_SLEEP)

        for cohort_date in all_dates:
            ltv_vals  = ltv_by_date.get(cohort_date,  {})
            proc_vals = proc_by_date.get(cohort_date, {})

            row: dict = {
                "cohort_date":  cohort_date,
                "country":      country_code,
                "country_name": country_name,
                "platform":     platform_label,
                **{f"realized_ltv_{w}d": ltv_vals.get(w,  "") for w in LTV_WINDOWS},
                **{f"proceeds_{w}d":     proc_vals.get(w, "") for w in PROCEEDS_WINDOWS},
            }
            all_rows.append(row)

    if not all_rows:
        print("\n[WARN] No data collected — CSV will not be written.")
        return

    write_csv(all_rows)
    print(f"\nTotal rows    : {len(all_rows)}")
    print(f"Expected rows : {len(all_dates)} dates × {len(combos)} combos = "
          f"{len(all_dates) * len(combos)}")


if __name__ == "__main__":
    main()
