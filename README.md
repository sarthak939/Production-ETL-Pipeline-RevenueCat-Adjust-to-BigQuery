# RevenueCat & Adjust → BigQuery Pipeline

A Python ETL pipeline that extracts subscription cohorts from **RevenueCat** and attribution metrics from **Adjust**, transforms and loads them into **Google BigQuery**, and runs automatically on a schedule via **GitHub Actions**.

---

## Architecture

```
RevenueCat API          Adjust Reporting API
      │                        │
      ▼                        ▼
revenuecat_cohort_export   adjust_to_bigquery
  (LTV + Proceeds)          (installs, spend,
  → CSV output               cohort metrics)
                                  │
                                  ▼
                           Google BigQuery
                                  ▲
                                  │
                          GitHub Actions
                      (scheduled orchestration)
```

---

## Features

- **RevenueCat Cohort Export** — Fetches Realized LTV and Proceeds across countries and platforms for configurable cohort time windows (D0, D7, D30, D90, D180, D365)
- **Adjust → BigQuery** — Parallel chunked fetching with pagination, exponential backoff retry, and a hard row-cap safety guard
- **GitHub Actions Orchestration** — Fully automated, scheduled pipeline execution with no manual intervention required
- **Rate-limit aware** — Respects RevenueCat v2 (15 req/min) and Adjust API limits
- **Clean schema** — Column renaming and type casting before BigQuery load
- **Zero hardcoded secrets** — All credentials stored as GitHub Actions secrets and loaded via environment variables

---

## Tech Stack

- Python 3.11+
- `requests` for API calls
- `google-cloud-bigquery` for BQ load
- `python-dotenv` for local credential management
- `concurrent.futures` for parallel chunk fetching
- GitHub Actions for pipeline scheduling and orchestration

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/your-username/revenuecat-adjust-bigquery-pipeline.git
cd revenuecat-adjust-bigquery-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure credentials (local)

```bash
cp .env.example .env
```

Fill in `.env` with your credentials (see table below). Place your GCP service account JSON file in the project root (it is gitignored).

### 4. Run locally

```bash
# Export RevenueCat cohort data to CSV
python src/revenuecat_cohort_export.py

# Fetch Adjust data and load to BigQuery
python src/adjust_to_bigquery.py
```

---

## GitHub Actions Orchestration

The pipeline is fully automated using GitHub Actions. It runs on a defined schedule and requires no manual execution.

### Setting up secrets

Add the following secrets in your GitHub repository under **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `RC_PROJECT_ID` | RevenueCat project ID |
| `RC_SECRET_API_KEY` | RevenueCat v2 API key |
| `ADJUST_API_TOKEN` | Adjust API bearer token |
| `ADJUST_APP_TOKEN` | Adjust app token |
| `BQ_PROJECT_ID` | Google Cloud project ID |
| `BQ_DATASET` | BigQuery dataset name |
| `BQ_TABLE` | BigQuery table name |
| `GCP_SERVICE_ACCOUNT_JSON` | Full contents of your GCP service account JSON |

### Workflow location

```
.github/
└── workflows/
    └── pipeline.yml
```

The workflow triggers on a cron schedule and can also be triggered manually via `workflow_dispatch`.

---

## Environment Variables (local)

| Variable | Description |
|---|---|
| `RC_PROJECT_ID` | RevenueCat project ID |
| `RC_SECRET_API_KEY` | RevenueCat **v2** API key (`charts_metrics:charts:read`) |
| `RC_START_DATE` | Export start date (`YYYY-MM-DD`) |
| `RC_END_DATE` | Export end date (`YYYY-MM-DD`) |
| `ADJUST_API_TOKEN` | Adjust API bearer token |
| `ADJUST_APP_TOKEN` | Adjust app token |
| `ADJUST_START_DATE` | Fetch start date |
| `ADJUST_END_DATE` | Fetch end date |
| `BQ_PROJECT_ID` | Google Cloud project ID |
| `BQ_DATASET` | BigQuery dataset name |
| `BQ_TABLE` | BigQuery table name |
| `SERVICE_ACCOUNT_JSON_PATH` | Path to GCP service account JSON |

> ⚠️ **Never commit `.env` or your service account JSON.** Both are in `.gitignore`.

---

## RevenueCat API Note

This script uses the **RevenueCat REST API v2**. v1 keys (`sk_...`) will return 401 errors.

Create a v2 key at:
`RevenueCat Dashboard → Project Settings → API Keys → + New`

Required permission: `charts_metrics:charts:read`

---

## BigQuery Schema

The Adjust pipeline loads the following cohort windows: **D0, D7, D15, D30, D60, D90** for:
- `lifetime_value`
- `all_revenue_total`
- `paying_users`
- `retained_users`

Plus standard campaign dimensions: campaign, adgroup, creative, country, platform, data source.

---

## License

MIT
