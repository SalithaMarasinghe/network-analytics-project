## Azure Data Factory Orchestration

This project uses **Azure Data Factory (ADF)** as the orchestration layer for the end‑to‑end ISP Customer Analytics & Churn Prediction Platform. ADF is responsible for:

- Ingesting four raw ISP datasets from a GitHub repo into **Azure Data Lake Gen2 (Bronze)**.

- Triggering a sequence of **Databricks notebooks** that implement Bronze/Silver, Silver/Gold transformations, churn feature engineering, and model training.

The result is a fully automated path from raw CSV files to curated Delta tables, ML churn scores, and downstream Power BI dashboards.

---

## Source Datasets

ADF ingests four logical source datasets from GitHub (via `HttpServer1`):

- `customers.csv` – customer master data (segment, plan, contract, tenure, churn flag, pricing).

- `networktelemetry.csv` – daily network performance metrics per customer (bandwidth, latency, packet loss, outages, SLA breaches).

- `supporttickets.csv` – support interactions (issue category, open/close dates, resolution status, CSAT).

- `billingevents.csv` – monthly billing events (amount charged, payment status, paid/late/failed).

Each dataset is mapped to a Copy activity that writes to an ADLS Gen2 container as Bronze Parquet files. 

[network-analytics-project/adf/datasets at main · SalithaMarasinghe/network-analytics-project](https://github.com/SalithaMarasinghe/network-analytics-project/tree/main/adf/datasets)

![](C:\Users\Salitha\AppData\Roaming\marktext\images\2026-06-01-12-39-35-image.png)

## Linked Services

**![](D:\Data%20Engineering\network%20analytics\adf\linked_services\LinkedServices.png)**

ADF is configured with three linked services that define external connections.

1. **HttpServer1** – HTTP source (GitHub raw content)
- Purpose: Source connection to the GitHub repository that hosts the four CSV datasets.

- Key settings:
  
  - Base URL: GitHub raw content URL for the repo (e.g., `https://raw.githubusercontent.com/<user>/<repo>/...`).
  
  - Authentication type: `Anonymous`.

- Used by: Copy activities that pull `customers.csv`, `networktelemetry.csv`, `supporttickets.csv`, and `billingevents.csv`.

![](D:\Data%20Engineering\network%20analytics\adf\linked_services\HTTP%20Service.png)

2. **AzureDataLakeStorage1** – Azure Data Lake Storage Gen2
- Purpose: Landing zone for Bronze, Silver, and Gold Delta/Parquet data.

- Key settings:
  
  - Integration runtime: `AutoResolveIntegrationRuntime`
  
  - Authentication: `Account key`
  
  - URL: your ADLS Gen2 DFS endpoint (e.g., `https://<storage-account>.dfs.core.windows.net/`).

- Used by: all Copy activities that write data into the lake.

![](D:\Data%20Engineering\network%20analytics\adf\linked_services\AzureDataLake%20Service.png)

3. **ls_azuredatabricks_isp** – Azure Databricks
- Purpose: Compute target for running Databricks notebooks from ADF.

- Key settings:
  
  - Workspace URL: your Azure Databricks workspace URL.
  
  - Authentication: `Access Token` (or Azure Key Vault).
  
  - Cluster: existing interactive cluster ID used for all notebook activities.

- Used by: Notebook activities for Bronze→Silver, Silver→Gold, churn feature engineering, and churn model training.

![](D:\Data%20Engineering\network%20analytics\adf\linked_services\DatabricksService.png)

---

## 

---

## Pipeline Overview

![](D:\Data%20Engineering\network%20analytics\adf\pipelines\Screenshot.png)

|                        |                 |       |                    |                       |          |                                             |                 |                                      |     |     |     |
| ---------------------- | --------------- | ----- | ------------------ | --------------------- | -------- | ------------------------------------------- | --------------- | ------------------------------------ | --- | --- | --- |
| Activity name          | Activity status | Error | Activity type      | Run start             | Duration | Integration runtime                         | User properties | Activity run ID                      | Log |     |     |
| Build Churn Model      | Succeeded       |       | DatabricksNotebook | 6/1/2026, 12:07:07 PM | 38s      | AutoResolveIntegrationRuntime (South India) | {}              | 7d36e9f1-4b71-43f7-84ad-5f506b086187 |     |     |     |
| build churn features   | Succeeded       |       | DatabricksNotebook | 6/1/2026, 12:06:27 PM | 39s      | AutoResolveIntegrationRuntime (South India) | {}              | a1e6a147-6c50-4d4e-beda-3eb46c7eac9e |     |     |     |
| Silver to Gold         | Succeeded       |       | DatabricksNotebook | 6/1/2026, 12:04:14 PM | 2m 11s   | AutoResolveIntegrationRuntime (South India) | {}              | 95b2c2cd-4952-4519-a390-8b45285a5b40 |     |     |     |
| Bronze to Silver       | Succeeded       |       | DatabricksNotebook | 6/1/2026, 11:57:31 AM | 6m 42s   | AutoResolveIntegrationRuntime (South India) | {}              | d7e79eaa-da52-493e-9623-992a178336bf |     |     |     |
| Ingest_Customers Data  | Succeeded       |       | Copy               | 6/1/2026, 11:56:59 AM | 24s      | AutoResolveIntegrationRuntime (South India) | {}              | 4176ba06-7091-4154-bcea-a5e73b0c2208 |     |     |     |
| Ingest_support_tickets | Succeeded       |       | Copy               | 6/1/2026, 11:56:59 AM | 24s      | AutoResolveIntegrationRuntime (South India) | {}              | 4d53ba94-30a9-4d6e-9836-f30c7e5924e0 |     |     |     |
| Ingest_Billing_events  | Succeeded       |       | Copy               | 6/1/2026, 11:56:59 AM | 31s      | AutoResolveIntegrationRuntime (South India) | {}              | 2b59ecbb-c533-409b-be4d-3c8962f2f11f |     |     |     |
| Ingest Telemetry_Data  | Succeeded       |       | Copy               | 6/1/2026, 11:56:59 AM | 22s      | AutoResolveIntegrationRuntime (South India) | {}              | 8cd3c48b-ac14-4396-9f16-0bcc2cdbd697 |     |     |     |

The main ADF pipeline orchestrates the complete medallion + ML flow.

1. **Copy data – Ingest_Customers_Data**

2. **Copy data – Ingest_Telemetry_Data**

3. **Copy data – Ingest_support_tickets**

4. **Copy data – Ingest_Billing_events**

These four Copy activities run in parallel, each using:

- Source: HTTP dataset (GitHub CSV) via `HttpServer1`.

- Sink: ADLS Gen2 folder path for Bronze (e.g., `bronze/customers`, `bronze/networktelemetry`, etc.) via `AzureDataLakeStorage1`.

Once all four copies succeed, ADF chains a series of Databricks notebook activities through `ls_azuredatabricks_isp`:

5. **Notebook – Bronze to Silver**
   
   - Reads Bronze Parquet files from ADLS.
   
   - Applies basic data quality rules (type casting, duplicates, null key filtering).
   
   - Writes clean, standardised Silver tables for each domain (customers, telemetry, tickets, billing).

6. **Notebook – Silver to Gold**
   
   - Implements the **dimensional star schema**:
     
     - Dimensions: `DimCustomer`, `DimRegion`, `DimDate`, `DimIssueCategory`, `DimPaymentStatus`.
     
     - Facts: `FactTelemetry`, `FactTickets`, `FactBilling`.
   
   - Joins Silver tables, assigns surrogate keys, builds conformed dimensions, and writes Gold Delta tables into ADLS Gen2.

7. **Notebook – build churn features**
   
   - Aggregates Gold facts (telemetry, tickets, billing) at the customer level.
   
   - Engineers churn‑driver features such as outage patterns, SLA breach rates, ticket volume and CSAT, payment behaviour, plan/contract attributes, and tenure.
   
   - Produces a feature table (e.g., `ml.churn_features`) in the ML/Gold zone.

8. **Notebook – Build Churn Model**
   
   - Trains a logistic regression model using the churn feature table.
   
   - Evaluates performance and then scores all active customers.
   
   - Writes final churn scores (e.g., `ml.churn_scores` with `churn_probability`) back to Delta tables for consumption by dashboards and further analysis.

Execution history for each activity (copy and notebook) is visible in the ADF monitoring view, confirming the full pipeline runs successfully end‑to‑end.

---

## End-to-End Dataflow Summary

End-to-end, the ADF pipeline coordinates:

1. **Ingestion** – four CSVs from GitHub → Bronze Parquet in ADLS via HTTP + ADLS linked services.

2. **Standardisation** – Databricks notebook cleans and types data into Silver tables.[]

3. **Dimensional Modelling** – Silver → Gold star schema for customer, telemetry, tickets, and billing.

4. **Feature Engineering** – Gold facts aggregated into a customer‑level churn feature dataset.

5. **ML Scoring** – logistic regression churn model trained and churn probabilities stored back into the lakehouse for BI

ADF acts as the **control plane**: it glues together storage (ADLS Gen2), compute (Databricks), and the external source (GitHub), providing a single place to schedule, rerun, and monitor the entire churn analytics platform.
