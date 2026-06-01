# ISP Customer Analytics & Churn Prediction – SQL Warehouse Documentation

## Overview

The `adb_network_analytics` SQL warehouse provides the curated analytical layer for the ISP Customer Analytics & Churn Prediction Platform. It exposes medallion-architecture data products (Gold and ML layers) as SQL tables, enabling:

- Dimensional reporting for customer, network, support, and billing analytics  
- Feature engineering and model scoring for churn prediction  
- Direct consumption by downstream BI tools (e.g., Power BI) and ad‑hoc analytics

All tables are stored as Delta Lake tables and are managed through Databricks SQL.

---

## Catalog and Schemas

![](C:\Users\Salitha\AppData\Roaming\marktext\images\2026-06-01-13-02-33-image.png)

- **Catalog:** `adb_network_analytics`
- **Schemas:**
  - `isp_gold` – dimensional warehouse (facts and dimensions for analytics)
  - `isp_ml` – machine learning feature and score tables

### isp_gold Schema: Dimensional Warehouse

The `isp_gold` schema implements a star-schema style dimensional warehouse designed to support cross-domain analysis across customers, network telemetry, support tickets, and billing.

#### Tables

- **`isp_gold.dimcustomer`**  
  
  - **Type:** Dimension  
  - **Grain:** One row per unique customer  
  - **Role:** Central customer master, joining commercial attributes (segment, plan, contract), geography (via `regionkey`), lifecycle dates, pricing, add‑on flags, and churn status.  
  - **Key relationships:**  
    - Referenced by `facttelemetry`, `facttickets`, and `factbilling` via `customerkey`.  
    - Joined to `dimregion` via `regionkey`.

- **`isp_gold.dimdate`**  
  
  - **Type:** Dimension  
  - **Grain:** One row per calendar date  
  - **Role:** Shared date dimension for time‑series analysis across all facts (onboarding, churn, telemetry events, ticket open/close, billing and payment dates).  
  - **Key attributes:** `datekey`, `date`, `year`, `month`, `day`, `dayofweek`, `isweekend`.

- **`isp_gold.dimissuecategory`**  
  
  - **Type:** Dimension  
  - **Grain:** One row per support issue category  
  - **Role:** Lookup for ticket categories (e.g., slow speed, outage report, connectivity drop) used by `facttickets` to group and filter support workloads.

- **`isp_gold.dimpaymentstatus`**  
  
  - **Type:** Dimension  
  - **Grain:** One row per payment status  
  - **Role:** Normalises billing outcomes (e.g., `paid`, `late`, `failed`) for use in revenue‑risk analysis in `factbilling`.

- **`isp_gold.dimregion`**  
  
  - **Type:** Dimension  
  - **Grain:** One row per geographic region  
  - **Role:** Geographic lookup for mapping customers and events to regions (e.g., Sydney, Melbourne, Brisbane), enabling regional segmentation across all facts.

- **`isp_gold.facttelemetry`**  
  
  - **Type:** Fact  
  - **Grain:** One row per customer per day of network telemetry  
  - **Role:** Captures daily network performance measures such as bandwidth, latency, packet loss, connection drops, SLA breaches, and outage flags.  
  - **Joins:**  
    - `customerkey` → `dimcustomer`  
    - `datekey` → `dimdate`

- **`isp_gold.facttickets`**  
  
  - **Type:** Fact  
  - **Grain:** One row per support ticket  
  - **Role:** Stores support interactions including issue category, open/close dates, resolution status, and satisfaction score for analysing operational performance and its impact on churn.  
  - **Joins:**  
    - `customerkey` → `dimcustomer`  
    - `issuecategorykey` → `dimissuecategory`  
    - `open_datekey` / `close_datekey` → `dimdate` (exact column naming may vary; see column list)

- **`isp_gold.factbilling`**  
  
  - **Type:** Fact  
  - **Grain:** One row per customer per billing period  
  - **Role:** Records billing events such as amount charged, billing month, and payment status to support analysis of revenue risk and payment behaviour as churn drivers.  
  - **Joins:**  
    - `customerkey` → `dimcustomer`  
    - `billing_datekey` / `payment_datekey` → `dimdate`  
    - `paymentstatuskey` → `dimpaymentstatus`

### isp_ml Schema: Machine Learning Layer

The `isp_ml` schema contains tables that support machine learning feature engineering and model outputs for churn prediction.

- **`isp_ml.churn_scores`**  
  - **Type:** ML score table  
  - **Grain:** One row per customer with latest churn score  
  - **Role:** Stores the output of the logistic regression churn model. Contains customer identifiers, selected engineered features, and the final `churn_probability` used by churn dashboards and downstream applications.  
  - **Joins:**  
    - `customer_id` / `customerkey` back to `isp_gold.dimcustomer`  
  - **Usage:**  
    - Power BI churn probability dashboards  
    - Segment‑level risk analysis (by plan type, region, contract type, customer segment)

---

## Data Model Summary

At a high level, the warehouse follows this pattern:

- **Conformed dimensions**  
  
  - `dimcustomer`, `dimdate`, `dimregion`, `dimissuecategory`, `dimpaymentstatus`  
  - Shared across multiple fact tables to support consistent slicing by customer, time, geography, support category, and payment status.

- **Operational facts**  
  
  - `facttelemetry` – network performance events  
  - `facttickets` – support ticket lifecycle  
  - `factbilling` – billing and payment events  

- **ML outputs**  
  
  - `churn_scores` – model predictions at the customer grain, derived from aggregated features built on top of Gold facts and dimensions.

This design allows:

- Standard BI querying using star‑schema joins  
- Efficient feature engineering for churn prediction  
- A single, governed source of truth for both analytics and ML workloads

---

## Querying the Warehouse

All tables are accessible through Databricks SQL using fully qualified names, for example:

```sql
SELECT *
FROM adb_network_analytics.isp_gold.facttelemetry
WHERE datekey BETWEEN 20250101 AND 20251231;
```

For ML outputs:

```sql
SELECT
  c.customerid,
  c.customertype,
  c.plantype,
  s.churn_probability
FROM adb_network_analytics.isp_ml.churn_scores AS s
JOIN adb_network_analytics.isp_gold.dimcustomer AS c
  ON s.customer_id = c.customerid;
```

---

## Metadata Exports

The following sections are reserved for automatically generated schema listings based on `INFORMATION_SCHEMA` queries. Populate these tables by pasting query results from Databricks.

### Table Inventory (from `INFORMATION_SCHEMA.TABLES`)

<style>
</style>

|                       |              |                  |            |                    |         |
| --------------------- | ------------ | ---------------- | ---------- | ------------------ | ------- |
| table_catalog         | table_schema | table_name       | table_type | is_insertable_into | comment |
| adb_network_analytics | isp_gold     | dimcustomer      | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | dimdate          | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | dimissuecategory | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | dimpaymentstatus | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | dimregion        | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | factbilling      | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | facttelemetry    | MANAGED    | YES                | null    |
| adb_network_analytics | isp_gold     | facttickets      | MANAGED    | YES                | null    |
| adb_network_analytics | isp_ml       | churn_scores     | MANAGED    | YES                | null    |

### Column-Level Schema (from `INFORMATION_SCHEMA.COLUMNS`)

<style>
</style>

|              |                  |                  |                         |           |             |         |
| ------------ | ---------------- | ---------------- | ----------------------- | --------- | ----------- | ------- |
| table_schema | table_name       | ordinal_position | column_name             | data_type | is_nullable | comment |
| isp_gold     | dimcustomer      | 0                | customer_key            | INT       | YES         | null    |
| isp_gold     | dimcustomer      | 1                | customer_id             | STRING    | YES         | null    |
| isp_gold     | dimcustomer      | 2                | customer_type           | STRING    | YES         | null    |
| isp_gold     | dimcustomer      | 3                | customer_id_str         | STRING    | YES         | null    |
| isp_gold     | dimcustomer      | 4                | region_key              | INT       | YES         | null    |
| isp_gold     | dimcustomer      | 5                | plan_type               | STRING    | YES         | null    |
| isp_gold     | dimcustomer      | 6                | contract_type           | STRING    | YES         | null    |
| isp_gold     | dimcustomer      | 7                | monthly_charge_aud      | DOUBLE    | YES         | null    |
| isp_gold     | dimcustomer      | 8                | tenure_months           | INT       | YES         | null    |
| isp_gold     | dimcustomer      | 9                | has_cybersecurity_addon | BOOLEAN   | YES         | null    |
| isp_gold     | dimcustomer      | 10               | has_voip_addon          | BOOLEAN   | YES         | null    |
| isp_gold     | dimcustomer      | 11               | onboarded_date          | DATE      | YES         | null    |
| isp_gold     | dimcustomer      | 12               | churn_flag              | BOOLEAN   | YES         | null    |
| isp_gold     | dimcustomer      | 13               | churn_date              | DATE      | YES         | null    |
| isp_gold     | dimdate          | 0                | date                    | DATE      | YES         | null    |
| isp_gold     | dimdate          | 1                | date_key                | INT       | YES         | null    |
| isp_gold     | dimdate          | 2                | year                    | INT       | YES         | null    |
| isp_gold     | dimdate          | 3                | month                   | INT       | YES         | null    |
| isp_gold     | dimdate          | 4                | day                     | INT       | YES         | null    |
| isp_gold     | dimdate          | 5                | day_of_week             | INT       | YES         | null    |
| isp_gold     | dimdate          | 6                | is_weekend              | BOOLEAN   | YES         | null    |
| isp_gold     | dimissuecategory | 0                | issue_category_key      | INT       | YES         | null    |
| isp_gold     | dimissuecategory | 1                | issue_category          | STRING    | YES         | null    |
| isp_gold     | dimpaymentstatus | 0                | payment_status_key      | INT       | YES         | null    |
| isp_gold     | dimpaymentstatus | 1                | payment_status          | STRING    | YES         | null    |
| isp_gold     | dimregion        | 0                | region_key              | INT       | YES         | null    |
| isp_gold     | dimregion        | 1                | region                  | STRING    | YES         | null    |
| isp_gold     | factbilling      | 0                | billing_id              | STRING    | YES         | null    |
| isp_gold     | factbilling      | 1                | billing_month           | DATE      | YES         | null    |
| isp_gold     | factbilling      | 2                | amount_charged_aud      | DOUBLE    | YES         | null    |
| isp_gold     | factbilling      | 3                | payment_method          | STRING    | YES         | null    |
| isp_gold     | factbilling      | 4                | customer_key            | INT       | YES         | null    |
| isp_gold     | factbilling      | 5                | payment_status_key      | INT       | YES         | null    |
| isp_gold     | factbilling      | 6                | billing_month_key       | INT       | YES         | null    |
| isp_gold     | factbilling      | 7                | payment_date_key        | INT       | YES         | null    |
| isp_gold     | facttelemetry    | 0                | telemetry_id            | STRING    | YES         | null    |
| isp_gold     | facttelemetry    | 1                | plan_type               | STRING    | YES         | null    |
| isp_gold     | facttelemetry    | 2                | daily_bandwidth_gb      | STRING    | YES         | null    |
| isp_gold     | facttelemetry    | 3                | peak_hour_latency_ms    | DOUBLE    | YES         | null    |
| isp_gold     | facttelemetry    | 4                | off_peak_latency_ms     | DOUBLE    | YES         | null    |
| isp_gold     | facttelemetry    | 5                | packet_loss_percent     | DOUBLE    | YES         | null    |
| isp_gold     | facttelemetry    | 6                | connection_drops        | INT       | YES         | null    |
| isp_gold     | facttelemetry    | 7                | sla_breach              | BOOLEAN   | YES         | null    |
| isp_gold     | facttelemetry    | 8                | outage_flag             | BOOLEAN   | YES         | null    |
| isp_gold     | facttelemetry    | 9                | node_id                 | STRING    | YES         | null    |
| isp_gold     | facttelemetry    | 10               | customer_key            | INT       | YES         | null    |
| isp_gold     | facttelemetry    | 11               | date_key                | INT       | YES         | null    |
| isp_gold     | facttickets      | 0                | ticket_id               | STRING    | YES         | null    |
| isp_gold     | facttickets      | 1                | channel                 | STRING    | YES         | null    |
| isp_gold     | facttickets      | 2                | resolved                | BOOLEAN   | YES         | null    |
| isp_gold     | facttickets      | 3                | satisfaction_score      | INT       | YES         | null    |
| isp_gold     | facttickets      | 4                | customer_key            | INT       | YES         | null    |
| isp_gold     | facttickets      | 5                | issue_category_key      | INT       | YES         | null    |
| isp_gold     | facttickets      | 6                | open_date_key           | INT       | YES         | null    |
| isp_gold     | facttickets      | 7                | close_date_key          | INT       | YES         | null    |
| isp_ml       | churn_scores     | 0                | customer_id             | STRING    | YES         | null    |
| isp_ml       | churn_scores     | 1                | customer_type           | STRING    | YES         | null    |
| isp_ml       | churn_scores     | 2                | plan_type               | STRING    | YES         | null    |
| isp_ml       | churn_scores     | 3                | contract_type           | STRING    | YES         | null    |
| isp_ml       | churn_scores     | 4                | region                  | STRING    | YES         | null    |
| isp_ml       | churn_scores     | 5                | tenure_months           | INT       | YES         | null    |
| isp_ml       | churn_scores     | 6                | monthly_charge_aud      | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 7                | avg_daily_bandwidth_gb  | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 8                | avg_peak_latency_ms     | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 9                | avg_offpeak_latency_ms  | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 10               | avg_packet_loss_pct     | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 11               | avg_connection_drops    | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 12               | outage_days             | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 13               | sla_breach_days         | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 14               | ticket_count            | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 15               | ticket_resolved_rate    | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 16               | avg_ticket_satisfaction | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 17               | billing_cycles          | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 18               | avg_amount_charged      | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 19               | late_rate               | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 20               | failed_rate             | DOUBLE    | YES         | null    |
| isp_ml       | churn_scores     | 21               | churn_flag              | INT       | YES         | null    |
| isp_ml       | churn_scores     | 22               | churn_probability       | DOUBLE    | YES         | null    |

## Notes and Conventions

- All dates are represented via surrogate integer keys in facts (e.g., `datekey`) and resolved to calendar attributes via `dimdate`.  
- Surrogate keys (e.g., `customerkey`, `regionkey`, `issuecategorykey`, `paymentstatuskey`) are generated in the Gold layer to decouple analytical joins from raw source identifiers.  
- All tables are implemented as Delta Lake tables to support ACID transactions and efficient incremental refresh.  
