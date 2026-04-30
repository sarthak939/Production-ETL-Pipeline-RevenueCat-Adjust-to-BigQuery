# Data-Engineering-paid-project-
Python ETL pipeline that fetches subscription cohorts from RevenueCat and attribution metrics from Adjust, transforming and loading them into Google BigQuery.
RevenueCat & Adjust → BigQuery Pipeline
A Python ETL pipeline that fetches subscription cohort data from RevenueCat and performance metrics from Adjust, transforming and loading them into Google BigQuery for unified attribution and monetization analytics.

Architecture
RevenueCat API          Adjust Reporting API
      │                        │
      ▼                        ▼
revenuecat_cohort_export   adjust_to_bigquery
  (LTV + Proceeds)          (installs, spend,
  → CSV output               cohort metrics)
                                  │
                                  ▼
                           Google BigQuery

Features

RevenueCat Cohort Export — Fetches Realized LTV and Proceeds across countries and platforms for configurable cohort time windows (D0, D7, D30, D90, D180, D365)
Adjust → BigQuery — Parallel chunked fetching with pagination, exponential backoff retry, and a hard row-cap safety guard
Rate-limit aware — Respects RevenueCat v2 (15 req/min) and Adjust API limits
Clean schema — Column renaming and type casting before BigQuery load
Zero hardcoded secrets — All credentials loaded from .env


Tech Stack

Python 3.11+
requests for API calls
google-cloud-bigquery for BQ load
python-dotenv for credential management
concurrent.futures for parallel chunk fetching


Setup
1. Clone the repo
bashgit clone https://github.com/your-username/revenuecat-adjust-bigquery-pipeline.git
cd revenuecat-adjust-bigquery-pipeline
2. Install dependencies
bashpip install -r requirements.txt
3. Configure credentials
bashcp .env.example .env
Fill in .env with your credentials (see table below). Place your GCP service account JSON file in the project root (it is gitignored).
4. Run
bash# Export RevenueCat cohort data to CSV
python src/revenuecat_cohort_export.py

# Fetch Adjust data and load to BigQuery
python src/adjust_to_bigquery.py

Environment Variables
VariableDescriptionRC_PROJECT_IDRevenueCat project IDRC_SECRET_API_KEYRevenueCat v2 API key (charts_metrics:charts:read)RC_START_DATEExport start date (YYYY-MM-DD)RC_END_DATEExport end date (YYYY-MM-DD)ADJUST_API_TOKENAdjust API bearer tokenADJUST_APP_TOKENAdjust app tokenADJUST_START_DATEFetch start dateADJUST_END_DATEFetch end dateBQ_PROJECT_IDGoogle Cloud project IDBQ_DATASETBigQuery dataset nameBQ_TABLEBigQuery table nameSERVICE_ACCOUNT_JSON_PATHPath to GCP service account JSON

⚠️ Never commit .env or your service account JSON. Both are in .gitignore.


RevenueCat API Note
This script uses the RevenueCat REST API v2. v1 keys (sk_...) will return 401 errors.
Create a v2 key at:
RevenueCat Dashboard → Project Settings → API Keys → + New
Required permission: charts_metrics:charts:read

BigQuery Schema
The Adjust pipeline loads the following cohort windows: D0, D7, D15, D30, D60, D90 for:

lifetime_value
all_revenue_total
paying_users
retained_users

Plus standard campaign dimensions: campaign, adgroup, creative, country, platform, data source.

License
MIT
