# End-to-End ISP Customer Analytics & Churn Prediction Platform

> A production-grade **Azure Data Lakehouse** pipeline that unifies customer, network, support, and billing data from a large Australian ISP into a governed analytics platform — delivering descriptive dashboards and ML-driven churn prediction through a full medallion architecture.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Pipeline Walkthrough](#pipeline-walkthrough)
  - [Phase 1 — Data Ingestion (GitHub → Bronze via ADF)](#phase-1--data-ingestion-github--bronze-via-adf)
  - [Phase 2 — Bronze to Silver (Databricks)](#phase-2--bronze-to-silver-databricks)
  - [Phase 3 — Silver to Gold (Databricks)](#phase-3--silver-to-gold-databricks)
  - [Phase 4 — ML Feature Engineering & Churn Modelling (Databricks)](#phase-4--ml-feature-engineering--churn-modelling-databricks)
  - [Phase 5 — Power BI Analytics](#phase-5--power-bi-analytics)
- [Data Model (Star Schema)](#data-model-star-schema)
- [Dashboards](#dashboards)
- [Machine Learning — Churn Prediction](#machine-learning--churn-prediction)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Screenshots](#screenshots)
- [Key Business Questions Answered](#key-business-questions-answered)

---

## Project Overview

Telecommunications businesses operate in a **high-volume, high-velocity, multi-domain** data environment. Customer activity, network telemetry, support operations, and billing events generate continuous streams of data that — in isolation — tell only part of the story.

This project simulates how a modern telecom data team at a large Australian ISP (modelled on Superloop) would build a **centralised analytics foundation** capable of:

- Unifying fragmented operational data across **customer, network, support, and billing** domains
- Supporting **descriptive analytics** and executive reporting for operational decision-making
- Powering **predictive churn modelling** to identify at-risk customers before they leave

The platform is built entirely on **Azure** using industry-standard tools: ADF for orchestration, Databricks for transformation and ML, ADLS Gen2 as the data lake, and Power BI for analytics consumption.

---

## Architecture

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add architecture diagram here** — the full end-to-end architecture diagram showing GitHub → ADF → ADLS Gen2 (Bronze/Silver/Gold) → Databricks → Power BI flow.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                         │
│   GitHub Repository (CSV: customers, network_telemetry,                     │
│                       support_tickets, billing_events)                      │
└───────────────────────────┬─────────────────────────────────────────────────┘
                            │  HTTP Linked Service
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  AZURE DATA FACTORY (Orchestration)                         │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  Pipeline: network-analytics-de                                      │  │
│   │  ├── [PARALLEL] Copy: Ingest Customers Data      → Bronze/customers  │  │
│   │  ├── [PARALLEL] Copy: Ingest Telemetry Data      → Bronze/telemetry  │  │
│   │  ├── [PARALLEL] Copy: Ingest Support Tickets     → Bronze/tickets    │  │
│   │  ├── [PARALLEL] Copy: Ingest Billing Events      → Bronze/billing    │  │
│   │  ├── [SEQ]      Notebook: Bronze to Silver                           │  │
│   │  ├── [SEQ]      Notebook: Silver to Gold                             │  │
│   │  ├── [SEQ]      Notebook: Build Churn Features                       │  │
│   │  └── [SEQ]      Notebook: Build Churn Model                          │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────┬─────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│             AZURE DATA LAKE STORAGE GEN2 — Medallion Architecture           │
│                                                                             │
│  ┌─────────────┐    ┌─────────────────┐    ┌─────────────────────────────┐ │
│  │   BRONZE    │    │     SILVER      │    │           GOLD              │ │
│  │  (Raw Data) │───▶│ (Clean & Typed) │───▶│  (Star Schema + ML Output)  │ │
│  │  [Parquet]  │    │   [Parquet]     │    │     [Delta Lake]             │ │
│  └─────────────┘    └─────────────────┘    └─────────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────────────────────┘
                            │
          ┌─────────────────┴──────────────────┐
          ▼                                     ▼
┌──────────────────────┐            ┌───────────────────────────┐
│  DATABRICKS SQL      │            │  DATABRICKS ML (PySpark + │
│  WAREHOUSE           │            │  Scikit-learn)            │
│  (isp_gold schema)   │            │  (isp_ml.churn_scores)    │
└──────────┬───────────┘            └───────────────────────────┘
           │                                │
           ▼                                ▼                                                                      
┌──────────────────────────────────────────────────────────────┐
│                      POWER BI                                │
│  ├── Dashboard 1: Customer Health & Churn Intelligence       │
│  ├── Dashboard 2: Network Performance & SLA                  │
│  ├── Dashboard 3: Support Experience & Revenue Risk          │
│  └── Dashboard 4: ML Churn Probability Scores               │
└──────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Orchestration** | Azure Data Factory (ADF) | Pipeline scheduling, dependency management, monitoring |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) | Scalable lakehouse storage (Bronze / Silver / Gold containers) |
| **Transformation** | Azure Databricks (PySpark) | ETL, dimensional modelling, feature engineering |
| **ML** | Databricks + Scikit-learn | Logistic Regression, Gradient Boosting, Random Forest churn models |
| **Serving** | Databricks SQL Warehouse | SQL-queryable Delta tables via `isp_gold` and `isp_ml` schemas |
| **BI / Reporting** | Power BI (`.pbix`) | Interactive dashboards connected to Databricks SQL endpoint |
| **Data Generation** | Python (`faker`, `numpy`, `pandas`) | Synthetic ISP dataset generation |
| **Source Format** | CSV → Parquet → Delta | Efficient columnar storage across all medallion layers |
| **Authentication** | Azure Service Principal (OAuth) | Secure Databricks ↔ ADLS Gen2 access via Client Credentials |

---

## Data Sources

The platform ingests four datasets that represent different business processes and grains. The raw CSV files are hosted on this GitHub repository and pulled by ADF via HTTP.

### Dataset Summary

| Dataset | File | Rows | Grain | Key Fields |
|---|---|---|---|---|
| **Customer Master** | `customers.csv` | ~2,000 | 1 row per customer | `customer_id`, `customer_type`, `region`, `plan_type`, `contract_type`, `monthly_charge_aud`, `tenure_months`, `churn_flag` |
| **Network Telemetry** | `network_telemetry.csv` | ~50,000 | 1 row per customer per day | `telemetry_id`, `customer_id`, `date`, `daily_bandwidth_gb`, `peak_hour_latency_ms`, `packet_loss_percent`, `sla_breach`, `outage_flag` |
| **Support Tickets** | `support_tickets.csv` | ~3,000 | 1 row per ticket | `ticket_id`, `customer_id`, `issue_category`, `open_date`, `close_date`, `resolved`, `satisfaction_score` |
| **Billing Events** | `billing_events.csv` | ~15,000 | 1 row per customer per month | `billing_id`, `customer_id`, `billing_month`, `amount_charged_aud`, `payment_status`, `payment_date` |

### Customer Segments

| Segment | Share | Plan Types | Characteristics |
|---|---|---|---|
| **Residential** | 65% | NBN50, NBN100, NBN250, NBN1000 | Higher churn propensity on month-to-month contracts |
| **Business** | 20% | NBN100–1000, Fibre, Business VoIP | Lower churn, cybersecurity/VoIP add-ons, 1–2yr contracts |
| **Wholesale** | 15% | Wholesale Dark Fibre, Fibre, NBN1000 | Lowest churn, predominantly long-term (2yr) contracts |

### Australian Regions Covered

`Sydney` · `Melbourne` · `Brisbane` · `Adelaide` · `Perth` · `Canberra`

---

## Pipeline Walkthrough

### Phase 1 — Data Ingestion (GitHub → Bronze via ADF)

**Tool:** Azure Data Factory

ADF orchestrates the full end-to-end pipeline from a single pipeline resource: **`network-analytics-de`**.

#### Linked Services

| Service | Type | Purpose |
|---|---|---|
| `HttpServer1` | HTTP (Anonymous) | Connects to GitHub raw content URL to pull CSV files |
| `AzureDataLakeStorage1` | ADLS Gen2 (Account Key) | Landing zone for all Bronze, Silver, Gold data |
| `ls_azuredatabricks_isp` | Azure Databricks (Access Token) | Compute target for Databricks notebook activities |

#### Copy Activities (run in parallel)

Four `Copy Data` activities run simultaneously — one per dataset — pulling CSVs from GitHub and landing them as **Parquet** files in the Bronze container of ADLS Gen2:

```
bronze/customers/         ← customers.csv
bronze/network_telemetry/ ← network_telemetry.csv
bronze/support_tickets/   ← support_tickets.csv
bronze/billing_events/    ← billing_events.csv
```

> **Why Parquet for Bronze?** Storing raw data as Parquet (rather than CSV) improves read performance for Spark-based workloads downstream and reduces storage overhead while maintaining source fidelity.

#### Pipeline Execution Results

| Activity | Type | Duration | Status |
|---|---|---|---|
| Ingest Customers Data | Copy | 24s | ✅ Succeeded |
| Ingest Telemetry Data | Copy | 22s | ✅ Succeeded |
| Ingest Support Tickets | Copy | 24s | ✅ Succeeded |
| Ingest Billing Events | Copy | 31s | ✅ Succeeded |
| Bronze to Silver | DatabricksNotebook | 6m 42s | ✅ Succeeded |
| Silver to Gold | DatabricksNotebook | 2m 11s | ✅ Succeeded |
| Build Churn Features | DatabricksNotebook | 39s | ✅ Succeeded |
| Build Churn Model | DatabricksNotebook | 38s | ✅ Succeeded |

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add ADF pipeline screenshot here** — the ADF canvas showing the full pipeline with Copy and Notebook activities and their dependency arrows.

---

### Phase 2 — Bronze to Silver (Databricks)

**Notebook:** [`databricks_notebooks/data_engineering/Bronze to Silver.py`](databricks_notebooks/data_engineering/Bronze%20to%20Silver.py)

The Bronze-to-Silver notebook reads raw Parquet files from ADLS Gen2, applies data quality rules, and produces **clean, typed, deduplicated** Silver tables.

#### Access Pattern

Databricks authenticates to ADLS Gen2 via an **Azure Service Principal** (OAuth 2.0 Client Credentials). The service principal is assigned the `Storage Blob Data Contributor` role on the storage account, enabling both read and write access.

#### Transformations Applied

| Table | Transformations |
|---|---|
| **customers** | Deduplicate on `customer_id` · Drop null `customer_id` · Cast `onboarded_date`/`churn_date` to `DateType` · Cast `churn_flag`, `has_cybersecurity_addon`, `has_voip_addon` to `Boolean` · Cast `monthly_charge_aud` to `Double` · Cast `tenure_months` to `Integer` |
| **network_telemetry** | Deduplicate on `telemetry_id` · Drop null `customer_id` · Cast `date` to `DateType` · Cast `sla_breach`, `outage_flag` to `Boolean` · Cast latency, packet loss, connection drop metrics to numeric types |
| **support_tickets** | Deduplicate on `ticket_id` · Drop null `customer_id` · Cast `open_date`/`close_date` to `DateType` · Cast `resolved` to `Boolean` · Cast `satisfaction_score` to `Integer` |
| **billing_events** | Deduplicate on `billing_id` · Drop null `customer_id` · Cast `billing_month`/`payment_date` to `DateType` · Cast `amount_charged_aud` to `Double` |

**Output:** Silver Parquet files written to `silver/` container in ADLS Gen2.

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add Bronze-to-Silver notebook screenshot here** — showing the PySpark transformation cells and output DataFrames.

---

### Phase 3 — Silver to Gold (Databricks)

**Notebook:** [`databricks_notebooks/data_engineering/Silver to Gold.py`](databricks_notebooks/data_engineering/Silver%20to%20Gold.py)

The Silver-to-Gold notebook transforms clean Silver tables into a **dimensional star schema** — business-ready for SQL warehouse querying, Power BI dashboards, and ML feature engineering.

#### Gold Dimension Tables

| Table | Description | Key Fields |
|---|---|---|
| `DimDate` | Full calendar from 2020–2026 | `date_key` (YYYYMMDD), `year`, `month`, `day`, `day_of_week`, `is_weekend` |
| `DimRegion` | Distinct customer regions | `region_key` (surrogate), `region` |
| `DimCustomer` | Customer attributes joined to region | `customer_key` (surrogate), `customer_id`, `customer_type`, `plan_type`, `contract_type`, `monthly_charge_aud`, `tenure_months`, `churn_flag`, `churn_date` |
| `DimIssueCategory` | Support ticket categories | `issue_category_key` (surrogate), `issue_category` |
| `DimPaymentStatus` | Billing payment outcomes | `payment_status_key` (surrogate), `payment_status` |

#### Gold Fact Tables

| Table | Description | Foreign Keys | Measures |
|---|---|---|---|
| `FactTelemetry` | Daily network performance per customer | `customer_key`, `date_key` | `daily_bandwidth_gb`, `peak_hour_latency_ms`, `off_peak_latency_ms`, `packet_loss_percent`, `connection_drops`, `sla_breach`, `outage_flag` |
| `FactBilling` | Monthly billing events | `customer_key`, `payment_status_key`, `billing_month_key`, `payment_date_key` | `amount_charged_aud`, `payment_method` |
| `FactTickets` | Customer support interactions | `customer_key`, `issue_category_key`, `open_date_key`, `close_date_key` | `channel`, `resolved`, `satisfaction_score` |

**Output:** Gold Delta tables written to `gold/` container in ADLS Gen2 and registered as managed tables in the `isp_gold` schema on the Databricks SQL Warehouse.

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add Silver-to-Gold schema diagram here** — the star schema ERD showing all dimension and fact tables with their relationships.

---

### Phase 4 — ML Feature Engineering & Churn Modelling (Databricks)

#### Step 4a — Feature Engineering

**Notebook:** [`databricks_notebooks/machine learning/ML_Build_Churn_Features.py`](databricks_notebooks/machine%20learning/ML_Build_Churn_Features.py)

Reads Gold Delta tables and aggregates them at the **customer level** to build a flat feature table for the churn model.

| Feature Domain | Features Engineered |
|---|---|
| **Customer / Commercial** | `customer_type`, `plan_type`, `contract_type`, `monthly_charge_aud`, `tenure_months`, `has_cybersecurity_addon`, `has_voip_addon` |
| **Network Telemetry** | `avg_daily_bandwidth_gb`, `avg_peak_latency_ms`, `avg_offpeak_latency_ms`, `avg_packet_loss_pct`, `avg_connection_drops`, `outage_days`, `sla_breach_days` |
| **Support** | `ticket_count`, `ticket_resolved_rate`, `avg_ticket_satisfaction`, `avg_resolution_days` |
| **Billing** | `billing_cycles`, `avg_amount_charged`, `late_rate`, `failed_rate` |

**Output:** Saved as a Delta table at `churnml/churn_feature_table` in ADLS Gen2.

#### Step 4b — Churn Prediction Model

**Notebook:** [`databricks_notebooks/machine learning/Churn_Prediction_Model.py`](databricks_notebooks/machine%20learning/Churn_Prediction_Model.py)

Trains and evaluates multiple classification models on the churn feature table.

**Models Trained:**

| Model | Algorithm | Configuration |
|---|---|---|
| Logistic Regression | `sklearn.linear_model.LogisticRegression` | `max_iter=2000`, `solver=lbfgs`, `C=1.0` (L2 regularisation) |
| Gradient Boosting | `sklearn.ensemble.GradientBoostingClassifier` | `n_estimators=200`, `learning_rate=0.1`, `max_depth=3` |
| Random Forest | `sklearn.ensemble.RandomForestClassifier` | `n_estimators=200`, `n_jobs=-1` |

**Evaluation:** ROC-AUC score on a stratified 80/20 train-test split.

**Feature Preprocessing:**
- Categorical features (`customer_type`, `plan_type`, `contract_type`, `region`) → one-hot encoded via `pd.get_dummies`
- Missing numeric values → median imputation via `SimpleImputer`

**Output:** Churn probability scores (`churn_probability` column) for all customers saved as:
- Delta table at `churnml/ml/churn_scores` in ADLS Gen2
- Managed table `isp_ml.churn_scores` in Databricks SQL Warehouse

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add churn model output screenshot here** — the Power BI churn probability page or the Databricks notebook showing model AUC scores.

---

### Phase 5 — Power BI Analytics

**File:** [`powerbi/Network Analytics.pbix`](powerbi/Network%20Analytics.pbix)
**Connection:** [`powerbi/databricks-network-analytics.pbids`](powerbi/databricks-network-analytics.pbids)

Power BI connects directly to the **Databricks SQL Warehouse** (`isp_gold` and `isp_ml` schemas) using the `.pbids` connection file. All visuals are built on top of the governed Gold star schema — not raw source files.

---

## Data Model (Star Schema)

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add schema diagram here** — the `schema/network_analytics_schema.png` showing the full star schema with all tables and relationships.

The Gold layer implements a **star schema** with the following relationships:

```
                    ┌──────────────┐
                    │   DimDate    │
                    └──────┬───────┘
                           │ date_key
          ┌────────────────┼────────────────┐
          │                │                │
   ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
   │FactTelemetry│  │ FactBilling │  │ FactTickets │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                │
          └────────────────┼────────────────┘
                     customer_key
                    ┌──────▼───────┐
                    │ DimCustomer  │
                    └──────┬───────┘
                     region_key
                    ┌──────▼───────┐
                    │  DimRegion   │
                    └──────────────┘

          FactBilling ──────────────▶ DimPaymentStatus
          FactTickets ─────────────▶ DimIssueCategory
```

---

## Dashboards

The Power BI report contains four pages, each targeting a specific business audience.

### Dashboard 1 — Customer Health & Churn Intelligence

**Audience:** Commercial leadership, customer retention teams

**KPIs:** Total Customers · Churned Customers · Churn Rate % · Avg Tenure (Months)

**Visuals:**
- Churn Rate % by Plan Type (clustered bar chart)
- Churn by Region / City (bar chart)
- Churn by Tenure Bucket — `0–6m / 7–12m / 13–24m / 24+m` (column chart)
- SLA Breach Rate % vs Churn Rate % scatter (plan-level)

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add Dashboard 1 screenshot here** — `powerbi/screenshots/DA Page 1-Customer Health and Churn Intellgence.png`

---

### Dashboard 2 — Network Performance & SLA

**Audience:** Network operations, NOC leads, infrastructure teams

**KPIs:** Avg Peak Latency (ms) · Avg Packet Loss % · SLA Breach Rate % · Outages Last 30 Days

**Visuals:**
- Network Health Over Time — SLA Breach Rate + Avg Latency trend (line chart)
- Avg Peak Latency by Region (clustered bar chart)
- Packet Loss vs Bandwidth Scatter by Plan Type (QoS analysis)
- Outage Incidents by Region / Over Time

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add Dashboard 2 screenshot here** — `powerbi/screenshots/DA Page 2-Network Performance and SLA.png`

---

### Dashboard 3 — Support Experience & Revenue Risk

**Audience:** Support operations managers, finance teams

**KPIs:** Ticket Count · Resolved Ticket Count · Resolved Rate % · Payment Failure Rate %

**Visuals:**
- Ticket Counts by Issue Category (horizontal bar)
- Resolved Ticket Count by Issue Category (vertical bar)
- Resolved Rate % and Ticket Count by Region (combo line + column chart)
- Payment Failure Rate % and Churn Rate % by Region (scatter / combo chart)

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add Dashboard 3 screenshot here** — `powerbi/screenshots/DA Page 3-Support Experience and Revenue Risk.png`

---

### Dashboard 4 — ML Churn Probability

**Audience:** Retention managers, account management, CRM teams

Displays per-customer `churn_probability` scores from the logistic regression model alongside segment filters, enabling targeted intervention for high-risk customers.

**Key Fields Surfaced:** `customer_id`, `region`, `plan_type`, `contract_type`, `tenure_months`, `churn_probability`, `sla_breach_days`, `failed_rate`, `ticket_count`

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 **Add Dashboard 4 screenshot here** — `powerbi/screenshots/ML-Churn Probablitiy.png`

---

## Machine Learning — Churn Prediction

### Problem Statement

Binary classification: predict whether a customer will churn (`churn_flag = 1`) based on their commercial profile, network experience, support history, and billing behaviour.

### Feature Set (19 input features)

```
Customer:   customer_type, plan_type, contract_type, region,
            monthly_charge_aud, tenure_months,
            has_cybersecurity_addon, has_voip_addon

Network:    avg_daily_bandwidth_gb, avg_peak_latency_ms,
            avg_offpeak_latency_ms, avg_packet_loss_pct,
            avg_connection_drops, outage_days, sla_breach_days

Support:    ticket_count, ticket_resolved_rate,
            avg_ticket_satisfaction, avg_resolution_days

Billing:    billing_cycles, avg_amount_charged,
            late_rate, failed_rate
```

### Model Comparison

| Model | AUC-ROC |
|---|---|
| Logistic Regression | _(see notebook output)_ |
| Gradient Boosting | _(see notebook output)_ |
| Random Forest | _(see notebook output)_ |

### Output Table Schema (`isp_ml.churn_scores`)

| Column | Type | Description |
|---|---|---|
| `customer_id` | String | Customer identifier |
| `customer_type` | String | residential / business / wholesale |
| `plan_type` | String | NBN50 / NBN100 / Fibre / etc. |
| `contract_type` | String | month-to-month / 1-year / 2-year |
| `region` | String | Australian city |
| `tenure_months` | Integer | Customer lifetime in months |
| `churn_probability` | Double | Model-predicted churn probability (0–1) |
| `churn_flag` | Integer | Actual churn label (0 or 1) |
| ... | ... | All input features included for drill-down |

---

## Project Structure

```
network-analytics/
│
├── data_generation/
│   └── generate_isp_datasets.py         # Synthetic ISP dataset generator (Faker + NumPy)
│
├── adf/
│   ├── datasets/                         # ADF dataset definitions (CSV source, Parquet sink)
│   ├── linked_services/                  # Screenshots of HTTP, ADLS Gen2, Databricks linked services
│   ├── pipelines/                        # ADF pipeline run history screenshot
│   └── docs/
│       └── Pipeline Overview.md          # Full ADF pipeline documentation
│
├── databricks_notebooks/
│   ├── data_engineering/
│   │   ├── Bronze to Silver.py           # ETL: raw Parquet → clean Silver tables
│   │   ├── Bronze to Silver.ipynb        # Notebook with cell outputs
│   │   ├── Silver to Gold.py             # ETL: Silver → dimensional star schema (Gold)
│   │   └── Silver to Gold.ipynb          # Notebook with cell outputs
│   └── machine learning/
│       ├── ML_Build_Churn_Features.py    # Feature engineering from Gold tables
│       ├── ML_Build_Churn_Features.ipynb # Notebook with cell outputs
│       ├── Churn_Prediction_Model.py     # LR / GBT / RF churn model training & scoring
│       └── Churn_Prediction_Model.ipynb  # Notebook with cell outputs
│
├── powerbi/
│   ├── Network Analytics.pbix            # Power BI report file
│   ├── databricks-network-analytics.pbids # Databricks SQL Warehouse connection file
│   ├── Dashboard Overview.md             # Dashboard documentation (KPIs, DAX measures, visuals)
│   └── screenshots/
│       ├── DA Page 1-Customer Health and Churn Intellgence.png
│       ├── DA Page 2-Network Performance and SLA.png
│       ├── DA Page 3-Support Experience and Revenue Risk.png
│       └── ML-Churn Probablitiy.png
│
├── schema/
│   ├── network_analytics_schema.drawio   # Editable star schema diagram
│   └── network_analytics_schema.png      # Exported schema diagram
│
├── sql/
│   └── All tables.csv                    # Databricks SQL Warehouse table listing
│
├── docs/
│   └── End-to-end ISP Customer Analytics & Churn Prediction Platform.pdf  # Full project report
│
├── overview.md                           # Project narrative and phase-by-phase walkthrough
└── README.md                             # This file
```

---

## Getting Started

### Prerequisites

| Requirement | Details |
|---|---|
| Azure Subscription | Active subscription with Contributor access |
| Azure Data Lake Storage Gen2 | Storage account with hierarchical namespace enabled |
| Azure Data Factory | ADF instance in the same resource group |
| Azure Databricks | Workspace with an interactive cluster |
| Power BI Desktop | For opening and refreshing the `.pbix` report |
| Python 3.8+ | For local dataset generation only |

### Step 1 — Generate Datasets (Optional)

The datasets are already committed to this repository under `adf/datasets/`. To regenerate them locally:

```bash
pip install pandas numpy faker
python data_generation/generate_isp_datasets.py
```

This will produce `customers.csv`, `network_telemetry.csv`, `support_tickets.csv`, and `billing_events.csv`.

### Step 2 — Set Up Azure Infrastructure

1. **Create a Resource Group** in your preferred Azure region.
2. **Create an ADLS Gen2 Storage Account** — enable the **hierarchical namespace** to convert it to a data lake.
3. **Create three containers** in the storage account: `bronze`, `silver`, `gold` (and optionally `churnml` for ML outputs).
4. **Create an Azure Databricks workspace** and provision an interactive cluster.
5. **Create an Azure Data Factory** instance.

### Step 3 — Configure Azure Service Principal

Databricks needs a service principal to authenticate to ADLS Gen2:

1. Register an app in **Azure Active Directory** → note the `Application (Client) ID` and `Tenant (Directory) ID`.
2. Create a **client secret** and note the value.
3. Assign the service principal the **`Storage Blob Data Contributor`** role on the ADLS Gen2 storage account.
4. In your Databricks notebooks, replace the placeholder values:

```python
secret = "<YOUR_AZURE_CLIENT_SECRET>"
app_id = "<YOUR_AZURE_CLIENT_ID>"
dir_id = "<YOUR_AZURE_TENANT_ID>"
```

### Step 4 — Configure ADF Linked Services

Set up three linked services in ADF:

| Linked Service | Type | Key Setting |
|---|---|---|
| `HttpServer1` | HTTP | Base URL: `https://raw.githubusercontent.com/<user>/<repo>/main/adf/datasets/` · Auth: Anonymous |
| `AzureDataLakeStorage1` | ADLS Gen2 | DFS endpoint URL of your storage account · Auth: Account Key |
| `ls_azuredatabricks_isp` | Azure Databricks | Workspace URL · Auth: Access Token · Cluster: your cluster ID |

### Step 5 — Run the ADF Pipeline

Trigger the `network-analytics-de` pipeline manually (or set a schedule trigger). Monitor activity runs in the ADF monitoring pane. The full pipeline takes approximately **10–12 minutes** end-to-end.

### Step 6 — Connect Power BI

1. Open `powerbi/Network Analytics.pbix` in Power BI Desktop.
2. Update the data source connection using the Databricks SQL Warehouse JDBC endpoint from `powerbi/databricks-network-analytics.pbids`.
3. Refresh the dataset — all four dashboards will populate from the `isp_gold` and `isp_ml` schemas.

---

## Screenshots

### ADF Pipeline

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `adf/pipelines/Screenshot.png` — ADF pipeline canvas showing all Copy and Notebook activities

### ADF Linked Services

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `adf/linked_services/LinkedServices.png` — Overview of all three linked services

### Data Schema

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `schema/network_analytics_schema.png` — Full star schema diagram

### Power BI Dashboards

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `powerbi/screenshots/DA Page 1-Customer Health and Churn Intellgence.png`

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `powerbi/screenshots/DA Page 2-Network Performance and SLA.png`

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `powerbi/screenshots/DA Page 3-Support Experience and Revenue Risk.png`

<!-- SCREENSHOT PLACEHOLDER -->
> 📌 `powerbi/screenshots/ML-Churn Probablitiy.png`

---

## Key Business Questions Answered

| Business Question | Dashboard / Layer |
|---|---|
| Which customer segments have the highest churn rate? | Dashboard 1 |
| Which plan types and regions are driving churn? | Dashboard 1 |
| At what tenure stage do customers churn most? | Dashboard 1 |
| Do SLA breaches correlate with higher churn? | Dashboard 1 |
| Are we meeting network SLAs over time? | Dashboard 2 |
| Which regions have the worst latency and packet loss? | Dashboard 2 |
| Where and when are outage incidents occurring? | Dashboard 2 |
| Which issue categories generate the most support volume? | Dashboard 3 |
| Are support tickets being resolved effectively by region? | Dashboard 3 |
| Do payment failures predict churn? | Dashboard 3 |
| Which individual customers are most at risk of churning? | Dashboard 4 (ML) |
| What combination of factors drives the highest churn probability? | Dashboard 4 (ML) |

---

## Author

**Salitha Marasinghe**

[![GitHub](https://img.shields.io/badge/GitHub-SalithaMarasinghe-181717?logo=github)](https://github.com/SalithaMarasinghe/network-analytics-project)

---

*Built with Azure Data Factory · Azure Databricks · ADLS Gen2 · Power BI · PySpark · Scikit-learn*
