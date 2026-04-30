"""
Adjust → BigQuery Pipeline
===========================
Fetches performance + cohort metrics from the Adjust Reporting API
and loads them into a BigQuery table.

Features:
  - Parallel chunked fetching (configurable workers & chunk size)
  - Automatic pagination with duplicate-page guard
  - Exponential backoff retry on 429 / 5xx
  - Hard row cap safety guard
  - Type casting + column renaming before BQ load

Requirements:
    pip install requests google-cloud-bigquery google-auth python-dotenv

Usage:
    1. Copy .env.example → .env and fill in your credentials
    2. Place your GCP service account JSON file at the path set in SERVICE_ACCOUNT_JSON_PATH
    3. python src/adjust_to_bigquery.py
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from threading import Lock
from typing import Any

import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG  (all secrets loaded from environment / .env)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    # ── Adjust API ────────────────────────────────────────────────────────────
    api_token:          str   = field(default_factory=lambda: os.getenv("ADJUST_API_TOKEN", ""))
    app_token:          str   = field(default_factory=lambda: os.getenv("ADJUST_APP_TOKEN", ""))
    start_date:         str   = field(default_factory=lambda: os.getenv("ADJUST_START_DATE", "2024-01-01"))
    end_date:           str   = field(default_factory=lambda: os.getenv("ADJUST_END_DATE",   "2024-12-31"))
    adjust_url:         str   = "https://dash.adjust.com/control-center/reports-service/report"
    utc_offset:         str   = "+00:00"
    attribution_type:   str   = "click"
    attribution_source: str   = "first"
    ad_spend_mode:      str   = "network"
    page_size:          int   = 50_000

    # ── Parallelism ───────────────────────────────────────────────────────────
    chunk_days:         int   = 7     # days per API call
    max_workers:        int   = 4     # parallel threads (keep ≤ 5 to respect rate limits)

    # ── Retry / timeout ───────────────────────────────────────────────────────
    max_retries:        int   = 5
    backoff_factor:     float = 1.5   # wait = backoff_factor ^ attempt (seconds)
    timeout:            int   = 120

    # ── Safety cap (per full run) ─────────────────────────────────────────────
    max_rows_hard_cap:  int   = 1_000_000

    # ── BigQuery ──────────────────────────────────────────────────────────────
    bq_project:         str   = field(default_factory=lambda: os.getenv("BQ_PROJECT_ID", ""))
    bq_dataset:         str   = field(default_factory=lambda: os.getenv("BQ_DATASET", "adjust_data"))
    bq_table:           str   = field(default_factory=lambda: os.getenv("BQ_TABLE", "adjust_report"))
    write_mode:         str   = "WRITE_TRUNCATE"   # WRITE_TRUNCATE | WRITE_APPEND

    # ── GCP Service Account ───────────────────────────────────────────────────
    service_account_json_path: str = field(
        default_factory=lambda: os.getenv("SERVICE_ACCOUNT_JSON_PATH", "service_account.json")
    )

    @property
    def bq_table_id(self) -> str:
        return f"{self.bq_project}.{self.bq_dataset}.{self.bq_table}"

    @property
    def adjust_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type":  "application/json",
        }


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN MAPPING  (Adjust API key → BigQuery column name)
# ══════════════════════════════════════════════════════════════════════════════

COLUMN_MAP: dict[str, str] = {
    "day":              "date",
    "campaign_id":      "campaign_id",
    "campaign":         "campaign_name",
    "adgroup_id":       "adgroup_id",
    "adgroup":          "adgroup_name",
    "creative_id":      "ad_id",
    "creative":         "ad_name",
    "country":          "country",
    "os_name":          "platform",
    "partner_name":     "data_source",
    "installs":         "installs",
    "paid_installs":    "installs_paid",
    "cost":             "ad_spend",
    "all_revenue":      "all_revenue",
    "revenue":          "all_revenue_cohort",
    "impressions":      "impressions",
    "clicks":           "clicks",
    "revenue_to_cost":  "revenue_to_cost",
    "sessions":         "sessions",
    "cohort_revenue":   "cohort_revenue",
}

# ── BigQuery schema ───────────────────────────────────────────────────────────

BQ_SCHEMA = [
    bigquery.SchemaField("date",                  "DATE"),
    bigquery.SchemaField("campaign_id",           "STRING"),
    bigquery.SchemaField("campaign_name",         "STRING"),
    bigquery.SchemaField("adgroup_id",            "STRING"),
    bigquery.SchemaField("adgroup_name",          "STRING"),
    bigquery.SchemaField("ad_id",                 "STRING"),
    bigquery.SchemaField("ad_name",               "STRING"),
    bigquery.SchemaField("country",               "STRING"),
    bigquery.SchemaField("platform",              "STRING"),
    bigquery.SchemaField("data_source",           "STRING"),
    bigquery.SchemaField("installs",              "FLOAT64"),
    bigquery.SchemaField("installs_paid",         "FLOAT64"),
    bigquery.SchemaField("ad_spend",              "FLOAT64"),
    bigquery.SchemaField("all_revenue",           "FLOAT64"),
    bigquery.SchemaField("all_revenue_cohort",    "FLOAT64"),
    bigquery.SchemaField("impressions",           "FLOAT64"),
    bigquery.SchemaField("clicks",                "FLOAT64"),
    bigquery.SchemaField("revenue_to_cost",       "FLOAT64"),
    bigquery.SchemaField("sessions",              "FLOAT64"),
    bigquery.SchemaField("cohort_revenue",        "FLOAT64"),
    # LTV cohort windows
    bigquery.SchemaField("lifetime_value_d0",     "FLOAT64"),
    bigquery.SchemaField("lifetime_value_d7",     "FLOAT64"),
    bigquery.SchemaField("lifetime_value_d15",    "FLOAT64"),
    bigquery.SchemaField("lifetime_value_d30",    "FLOAT64"),
    bigquery.SchemaField("lifetime_value_d60",    "FLOAT64"),
    bigquery.SchemaField("lifetime_value_d90",    "FLOAT64"),
    # Revenue cohort windows
    bigquery.SchemaField("all_revenue_total_d0",  "FLOAT64"),
    bigquery.SchemaField("all_revenue_total_d7",  "FLOAT64"),
    bigquery.SchemaField("all_revenue_total_d15", "FLOAT64"),
    bigquery.SchemaField("all_revenue_total_d30", "FLOAT64"),
    bigquery.SchemaField("all_revenue_total_d60", "FLOAT64"),
    bigquery.SchemaField("all_revenue_total_d90", "FLOAT64"),
    # Paying users cohort windows
    bigquery.SchemaField("paying_users_d0",       "FLOAT64"),
    bigquery.SchemaField("paying_users_d7",       "FLOAT64"),
    bigquery.SchemaField("paying_users_d15",      "FLOAT64"),
    bigquery.SchemaField("paying_users_d30",      "FLOAT64"),
    bigquery.SchemaField("paying_users_d60",      "FLOAT64"),
    bigquery.SchemaField("paying_users_d90",      "FLOAT64"),
    # Retained users cohort windows
    bigquery.SchemaField("retained_users_d0",     "FLOAT64"),
    bigquery.SchemaField("retained_users_d7",     "FLOAT64"),
    bigquery.SchemaField("retained_users_d15",    "FLOAT64"),
    bigquery.SchemaField("retained_users_d30",    "FLOAT64"),
    bigquery.SchemaField("retained_users_d60",    "FLOAT64"),
    bigquery.SchemaField("retained_users_d90",    "FLOAT64"),
]

STRING_FIELDS = {
    "date", "campaign_id", "campaign_name", "adgroup_id", "adgroup_name",
    "ad_id", "ad_name", "country", "platform", "data_source",
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def date_chunks(start: str, end: str, chunk_days: int):
    """Yield (chunk_start, chunk_end) pairs covering [start, end] in windows of chunk_days."""
    cur   = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_d:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end_d)
        yield cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        cur = chunk_end + timedelta(days=1)


def build_params(cfg: Config, chunk_start: str, chunk_end: str) -> dict:
    """Build Adjust query params for a specific date window."""
    return {
        "app_token__in":      cfg.app_token,
        "dimensions":         "day,campaign,adgroup,creative,country,os_name,partner_name",
        "metrics": ",".join([
            "installs", "paid_installs", "cost", "all_revenue", "revenue",
            "impressions", "clicks", "revenue_to_cost", "sessions", "cohort_revenue",
            "lifetime_value_d0",  "lifetime_value_d7",
            "lifetime_value_d15", "lifetime_value_d30",
            "lifetime_value_d60", "lifetime_value_d90",
            "all_revenue_total_d0",  "all_revenue_total_d7",
            "all_revenue_total_d15", "all_revenue_total_d30",
            "all_revenue_total_d60", "all_revenue_total_d90",
            "paying_users_d0",  "paying_users_d7",
            "paying_users_d15", "paying_users_d30",
            "paying_users_d60", "paying_users_d90",
            "retained_users_d0",  "retained_users_d7",
            "retained_users_d15", "retained_users_d30",
            "retained_users_d60", "retained_users_d90",
        ]),
        "date_period":        f"{chunk_start}:{chunk_end}",
        "utc_offset":         cfg.utc_offset,
        "attribution_type":   cfg.attribution_type,
        "attribution_source": cfg.attribution_source,
        "ad_spend_mode":      cfg.ad_spend_mode,
        "format":             "json",
    }


def _page_fingerprint(rows: list[dict]) -> str:
    sample = json.dumps(rows[0], sort_keys=True) + json.dumps(rows[-1], sort_keys=True)
    return hashlib.md5(sample.encode()).hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# FETCH ONE CHUNK  (single date window, with pagination + retry)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_chunk(
    cfg: Config,
    chunk_start: str,
    chunk_end: str,
    chunk_index: int,
    total_chunks: int,
    print_lock: Lock,
) -> list[dict]:
    """
    Fetch all rows for [chunk_start, chunk_end].
    Retries on transient errors with exponential backoff.
    """
    chunk_rows:         list[dict] = []
    page                           = 1
    seen_fingerprints:  set[str]   = set()

    while True:
        params            = build_params(cfg, chunk_start, chunk_end)
        params["limit"]   = cfg.page_size
        params["offset"]  = (page - 1) * cfg.page_size

        last_exc: Exception | None = None
        response                   = None

        for attempt in range(cfg.max_retries):
            try:
                response = requests.get(
                    cfg.adjust_url,
                    headers=cfg.adjust_headers,
                    params=params,
                    timeout=cfg.timeout,
                )
                if response.status_code == 429:
                    wait = cfg.backoff_factor ** (attempt + 2)
                    with print_lock:
                        print(f"   ⏳ [{chunk_start}] Rate limited. Waiting {wait:.1f}s...")
                    time.sleep(wait)
                    continue
                if response.status_code >= 500:
                    wait = cfg.backoff_factor ** attempt
                    with print_lock:
                        print(f"   ⚠️  [{chunk_start}] HTTP {response.status_code}. "
                              f"Retry {attempt+1}/{cfg.max_retries} in {wait:.1f}s")
                    time.sleep(wait)
                    continue
                break
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                wait = cfg.backoff_factor ** attempt
                with print_lock:
                    print(f"   ⚠️  [{chunk_start}] Request error: {exc}. "
                          f"Retry {attempt+1}/{cfg.max_retries} in {wait:.1f}s")
                time.sleep(wait)

        if response is None:
            raise RuntimeError(
                f"Chunk {chunk_start}:{chunk_end} failed after {cfg.max_retries} retries. "
                f"Last error: {last_exc}"
            )

        if response.status_code != 200:
            with print_lock:
                print(f"   ❌ [{chunk_start}] HTTP {response.status_code}: {response.text[:200]}")
            response.raise_for_status()

        data = response.json()
        rows = data.get("data", []) or data.get("rows", [])

        if not rows:
            break

        fp = _page_fingerprint(rows)
        if fp in seen_fingerprints:
            with print_lock:
                print(f"   🛑 [{chunk_start}] Duplicate page on p{page} — stopping chunk.")
            break
        seen_fingerprints.add(fp)

        chunk_rows.extend(rows)

        if len(rows) < cfg.page_size:
            break

        page += 1

    with print_lock:
        print(f"   ✅ [{chunk_start} → {chunk_end}]  {len(chunk_rows):,} rows  "
              f"(chunk {chunk_index}/{total_chunks})")

    return chunk_rows


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def fetch_adjust_data(cfg: Config) -> list[dict]:
    chunks       = list(date_chunks(cfg.start_date, cfg.end_date, cfg.chunk_days))
    total_chunks = len(chunks)

    print(f"\n🗓️  Date range : {cfg.start_date} → {cfg.end_date}")
    print(f"📦 Chunk size : {cfg.chunk_days} days → {total_chunks} chunks")
    print(f"⚡ Workers    : {cfg.max_workers} parallel threads")
    print(f"📄 Page size  : {cfg.page_size:,} rows/page\n")

    print_lock               = Lock()
    results: dict[int, list] = {}

    with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
        future_to_idx = {
            executor.submit(
                fetch_chunk, cfg,
                chunk_start, chunk_end,
                idx + 1, total_chunks, print_lock,
            ): idx
            for idx, (chunk_start, chunk_end) in enumerate(chunks)
        }

        completed    = 0
        total_so_far = 0

        for future in as_completed(future_to_idx):
            idx                     = future_to_idx[future]
            chunk_start, chunk_end  = chunks[idx]
            try:
                chunk_rows       = future.result()
                results[idx]     = chunk_rows
                completed       += 1
                total_so_far    += len(chunk_rows)
                with print_lock:
                    print(f"   📊 Progress: {completed}/{total_chunks} chunks | "
                          f"{total_so_far:,} rows so far")

                if total_so_far >= cfg.max_rows_hard_cap:
                    with print_lock:
                        print(f"\n🛑 Hard cap hit ({total_so_far:,}). Cancelling remaining chunks.")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

            except Exception as exc:
                with print_lock:
                    print(f"   ❌ Chunk [{chunk_start} → {chunk_end}] failed: {exc}")
                results[idx] = []

    all_rows: list[dict] = []
    for idx in sorted(results):
        all_rows.extend(results[idx])

    if not all_rows:
        print("⚠️  No data returned from Adjust.")
        return []

    print(f"\n✅ Fetch complete. {len(all_rows):,} total rows.")
    return all_rows


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

def safe_date_sort(row: dict):
    try:
        return (datetime.strptime(row.get("day", ""), "%m/%d/%Y"), row.get("campaign", ""), row.get("country", ""))
    except Exception:
        return (datetime.min, "", "")


def apply_column_map(rows: list[dict]) -> list[dict]:
    return [{COLUMN_MAP.get(k, k): v for k, v in row.items()} for row in rows]


def cast_types(rows: list[dict]) -> list[dict]:
    result = []
    for row in rows:
        clean: dict[str, Any] = {}
        for k, v in row.items():
            if k == "date":
                try:
                    clean[k] = datetime.strptime(str(v), "%m/%d/%Y").strftime("%Y-%m-%d")
                except Exception:
                    clean[k] = str(v)
            elif k in STRING_FIELDS:
                clean[k] = str(v) if v is not None else ""
            else:
                try:
                    clean[k] = float(v) if v not in (None, "", "null") else 0.0
                except (ValueError, TypeError):
                    clean[k] = 0.0
        result.append(clean)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# BIGQUERY LOAD
# ══════════════════════════════════════════════════════════════════════════════

def get_bq_client(cfg: Config) -> bigquery.Client:
    """Load GCP credentials from the service account JSON file path in .env."""
    if not os.path.exists(cfg.service_account_json_path):
        sys.exit(
            f"[FATAL] Service account JSON not found at: {cfg.service_account_json_path}\n"
            "Set SERVICE_ACCOUNT_JSON_PATH in your .env file."
        )
    credentials = service_account.Credentials.from_service_account_file(
        cfg.service_account_json_path,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=cfg.bq_project, credentials=credentials)


def ensure_dataset(client: bigquery.Client, cfg: Config) -> None:
    dataset_ref          = bigquery.Dataset(f"{cfg.bq_project}.{cfg.bq_dataset}")
    dataset_ref.location = "US"
    try:
        client.get_dataset(dataset_ref)
        print(f"📦 Dataset exists: {cfg.bq_dataset}")
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        print(f"📦 Created dataset: {cfg.bq_dataset}")


def load_to_bigquery(rows: list[dict], cfg: Config) -> None:
    client = get_bq_client(cfg)
    ensure_dataset(client, cfg)

    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if cfg.write_mode == "WRITE_TRUNCATE"
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    print(f"📤 Loading {len(rows):,} rows → {cfg.bq_table_id}  [{cfg.write_mode}]")
    load_job = client.load_table_from_json(rows, cfg.bq_table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(cfg.bq_table_id)
    print(f"✅ BigQuery load complete. Total rows in table: {table.num_rows:,}")


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(rows: list[dict]) -> None:
    installs   = sum(int(float(r.get("installs", 0) or 0)) for r in rows)
    cost       = sum(float(r.get("cost", 0) or 0) for r in rows)
    avg_ltv_d7 = sum(float(r.get("lifetime_value_d7", 0) or 0) for r in rows) / max(len(rows), 1)

    print("\n========== SUMMARY ==========")
    print(f"Rows       : {len(rows):,}")
    print(f"Installs   : {installs:,}")
    print(f"Cost       : {round(cost, 2):,}")
    print(f"Avg LTV D7 : {round(avg_ltv_d7, 4)}")
    print("=============================\n")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    cfg = Config()

    if not cfg.api_token or not cfg.app_token:
        sys.exit("[FATAL] ADJUST_API_TOKEN and ADJUST_APP_TOKEN must be set in your .env file.")
    if not cfg.bq_project:
        sys.exit("[FATAL] BQ_PROJECT_ID must be set in your .env file.")

    raw_rows = fetch_adjust_data(cfg)

    if raw_rows:
        raw_rows.sort(key=safe_date_sort)
        print_summary(raw_rows)
        mapped_rows = apply_column_map(raw_rows)
        mapped_rows = cast_types(mapped_rows)
        load_to_bigquery(mapped_rows, cfg)
