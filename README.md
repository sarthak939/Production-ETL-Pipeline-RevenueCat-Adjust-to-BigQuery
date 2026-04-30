# 🚀 RevenueCat & Adjust → BigQuery Pipeline

A scalable **Python ETL pipeline** that extracts subscription cohorts from **RevenueCat** and attribution metrics from **Adjust**, transforms them, and loads into **Google BigQuery** — fully automated via **GitHub Actions**.

---

## 🧠 Architecture

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

## ✨ Features

- 📊 **RevenueCat Cohort Export**  
  Extracts *Realized LTV* and *Proceeds* across countries & platforms  
  Supports cohort windows: **D0, D7, D30, D90, D180, D365**

- ⚡ **Adjust → BigQuery Pipeline**  
  - Parallel chunk processing  
  - Pagination handling  
  - Exponential backoff retry  
  - Row-cap safety guard  

- 🤖 **Automated Orchestration**  
  Runs via GitHub Actions — no manual execution required  

- ⏱️ **Rate-Limit Aware**  
  - RevenueCat v2 → 15 req/min  
  - Handles Adjust API limits  

- 🧹 **Clean Schema**  
  Column renaming + type casting before BigQuery load  

- 🔐 **Secure Setup**  
  No hardcoded secrets — uses env variables & GitHub Secrets  

---

## 🧰 Tech Stack

- Python 3.11+
- requests  
- google-cloud-bigquery  
- python-dotenv  
- concurrent.futures  
- GitHub Actions  

---

## ⚙️ Setup

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/revenuecat-adjust-bigquery-pipeline.git
cd revenuecat-adjust-bigquery-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
cp .env.example .env
```

- Fill in credentials in `.env`  
- Add GCP service account JSON in root (gitignored)  

### 4. Run Locally

```bash
# Export RevenueCat cohort data
python src/revenuecat_cohort_export.py

# Fetch Adjust data → Load into BigQuery
python src/adjust_to_bigquery.py
```

---

## 🤖 GitHub Actions

Workflow path:

```
.github/workflows/pipeline.yml
```

### Required Secrets

Add in **Settings → Secrets and variables → Actions**

| Secret | Description |
|------|-------------|
| RC_PROJECT_ID | RevenueCat project ID |
| RC_SECRET_API_KEY | RevenueCat v2 API key |
| ADJUST_API_TOKEN | Adjust API token |
| ADJUST_APP_TOKEN | Adjust app token |
| BQ_PROJECT_ID | GCP project ID |
| BQ_DATASET | BigQuery dataset |
| BQ_TABLE | BigQuery table |
| GCP_SERVICE_ACCOUNT_JSON | Service account JSON |

Supports:
- Cron scheduling  
- Manual trigger (`workflow_dispatch`)  

---

## 🌍 Environment Variables

| Variable | Description |
|----------|------------|
| RC_PROJECT_ID | RevenueCat project ID |
| RC_SECRET_API_KEY | RevenueCat v2 API key |
| RC_START_DATE | Start date (YYYY-MM-DD) |
| RC_END_DATE | End date |
| ADJUST_API_TOKEN | Adjust API token |
| ADJUST_APP_TOKEN | Adjust app token |
| ADJUST_START_DATE | Start date |
| ADJUST_END_DATE | End date |
| BQ_PROJECT_ID | GCP project ID |
| BQ_DATASET | Dataset |
| BQ_TABLE | Table |
| SERVICE_ACCOUNT_JSON_PATH | Path to JSON |

> ⚠️ Never commit `.env` or service account JSON files.

---

## 📡 RevenueCat API Note

- Uses **REST API v2**
- v1 keys (`sk_...`) → 401 error  
- Required permission:  
  `charts_metrics:charts:read`

Create via:  
Dashboard → Project Settings → API Keys → + New  

---

## 🧱 BigQuery Schema

### Cohort Windows
**D0, D7, D15, D30, D60, D90**

### Metrics
- lifetime_value  
- all_revenue_total  
- paying_users  
- retained_users  

### Dimensions
- campaign  
- adgroup  
- creative  
- country  
- platform  
- data_source  

---

## 📜 License

MIT License
