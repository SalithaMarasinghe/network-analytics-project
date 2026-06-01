# **End-to-end ISP Customer Analytics & Churn Prediction Platform**



#### Project Context

- The telecommunications industry operates in a **high-volume, high-velocity, multi-domain** environment where customer activity, network telemetry, support operations, and billing events generate continuous streams of analytical data. In ISP businesses, these domains are tightly connected, because degraded service quality, support friction, and payment issues can all influence customer retention and commercial performance.

- A large Australian ISP typically serves multiple customer segments such as **residential, business, and wholesale**, each with different usage patterns, service expectations, pricing structures, and churn behaviour. This creates a need for a unified analytics platform that can support operational monitoring and customer intelligence across the full subscriber base rather than within isolated functional silos.

- In this project scenario, the business problem is not limited to reporting on one dataset. The challenge is to integrate **customer master data, daily network performance data, support ticket activity, and monthly billing records** into a single analytical foundation that can answer business-critical questions about service quality, revenue risk, and churn.

- The dataset was designed to reflect a realistic ISP operating model. It includes:
  
  - a **customer master table** with subscription, contract, pricing, tenure, and churn information,
  
  - a **network telemetry table** with daily bandwidth, latency, packet loss, connection drops, SLA breaches, and outage indicators,
  
  - a **support tickets table** with issue types, resolution status, channels, and satisfaction scores,
  
  - and a **billing events table** with invoice amounts, payment outcomes, and payment methods.

- These datasets represent different business processes and different grains:
  
  - one row per customer,
  
  - one row per customer per day,
  
  - one row per support ticket,
  
  - and one row per customer per billing month.
  
  - This makes the project a realistic data engineering problem involving ingestion, standardisation, modelling, and cross-domain joins rather than a simple dashboarding exercise.

- From a business perspective, the platform is intended to support questions such as:
  
  - Which customer segments are most likely to churn?
  
  - Are SLA breaches, latency spikes, and outages associated with higher churn risk?
  
  - Do unresolved tickets or lower customer satisfaction correlate with customer loss?
  
  - Are late and failed payments early indicators of revenue risk and retention issues?

- The broader context for this project is aligned with modern ISP analytics practices, where **data lakehouse architectures, distributed processing, and BI-driven decision-making** are essential for handling diverse operational data at scale. Telecommunications organisations increasingly rely on platforms built around Azure, Databricks, SQL, and Power BI to turn fragmented raw data into trusted analytical products for both operations and commercial teams

- Within that context, this project was framed as an **end-to-end ISP Customer Analytics & Churn Prediction Platform**. Its purpose was to simulate how a modern telecom data team could build a centralised analytics foundation that supports descriptive analytics, executive reporting, and predictive churn modelling on top of the same governed data platform. 



## Business Problem

- The business needed a single analytics platform to unify fragmented ISP data across **customer, network, support, and billing** domains. Each source answered only part of the story on its own, which made it difficult to understand why customers were churning or where operational issues were affecting retention.

- Customer churn was the central commercial risk. The ISP served multiple segments, including residential, business, and wholesale customers, and each segment had different pricing, contract structures, and service expectations. Without a joined view of the data, it was not possible to identify which combinations of plan type, contract type, region, SLA breach patterns, support issues, or payment problems were driving churn.[](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/113963402/0ab38fdb-7405-472b-be4d-476ce684d180/isp_dataset_descriptions-2.pdf)

- Network performance was another major problem area. The telemetry dataset contained daily latency, packet loss, connection drops, and outage indicators, which means service degradation could be measured at a granular level. The business problem was to connect those technical signals to customer outcomes so that network health could be analysed not just as an infrastructure metric, but as a factor influencing customer loss and satisfaction.[](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/113963402/0ab38fdb-7405-472b-be4d-476ce684d180/isp_dataset_descriptions-2.pdf)

- Support operations also needed to be analysed in context. Ticket volume, issue category, resolution status, and satisfaction score were all present in the dataset, but those metrics only become meaningful when linked back to customer behaviour and service events. The problem was to determine whether support friction, unresolved tickets, or lower CSAT scores were contributing to churn risk.[](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/113963402/0ab38fdb-7405-472b-be4d-476ce684d180/isp_dataset_descriptions-2.pdf)

- Billing behaviour added another layer of risk. Monthly billing records showed payment status, late payments, and failed payments, all of which can act as warning signals before churn. The challenge was to engineer a model and analytics layer that could surface those patterns early enough for retention or account-management action.[](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/113963402/0ab38fdb-7405-472b-be4d-476ce684d180/isp_dataset_descriptions-2.pdf)

- From a data engineering perspective, the underlying issue was not only business interpretation but **data integration**. The four tables had different grains and refresh characteristics, so they could not be used directly for analytics without a proper lakehouse design, cleansing, and dimensional modelling layer.[](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/113963402/0ab38fdb-7405-472b-be4d-476ce684d180/isp_dataset_descriptions-2.pdf)

- The project therefore had to solve two problems at once:
  
  - build a **reliable, governed data foundation** from raw source files,
  
  - and transform that foundation into **business-ready analytics and churn intelligence** for decision-making.

- In practical terms, success meant creating a platform where a user could answer questions such as:
  
  - Which customers are most at risk of churn?
  
  - Which service issues are most strongly associated with churn?
  
  - Which regions show the weakest network experience?
  
  - Which billing behaviours indicate financial risk?
  
  - Which combinations of these factors justify intervention?
  
  ## Proposed Solution
  
  ![](C:\Users\Salitha\Downloads\adsad.png)
  
  - The project was implemented as an **end-to-end telecom data lakehouse** on the Azure platform, designed to unify customer, network, support, and billing data into a single governed analytics environment.
  
  - The solution follows a **medallion architecture** pattern with three progressively refined layers:
    
    - **Bronze** for raw ingestion and archival of source files,
    
    - **Silver** for cleansing, standardisation, and conformance,
    
    - **Gold** for business-ready dimensional models, curated analytics tables, and ML feature outputs.
  
  - Raw CSV files from a public GitHub repository were ingested through **Azure Data Factory** into **Azure Data Lake Storage Gen2**, with the data stored in **Parquet** format to support efficient downstream Spark processing and scalable lakehouse workloads.
  
  - **Databricks PySpark** was used as the main transformation engine for the project. This layer handled cleansing, joins, aggregations, feature engineering, and dimensional modelling while preserving a separation between source data, refined data, and consumer-facing datasets.
  
  - The business data was organised so that the **Silver layer** could act as the conformed enterprise view, while the **Gold layer** could be optimised for consumption by SQL queries, Power BI dashboards, and churn modelling. This is consistent with the way Databricks describes the medallion model, where bronze is raw landing data, silver is cleansed and conformed data, and gold is consumption-ready data.
  
  - A **Databricks SQL warehouse** was created on top of the designed schema to expose curated tables through structured SQL endpoints. This enabled complex joins, window functions, and aggregations to be made available for reporting without directly querying raw files.
  
  - **Power BI** was used as the presentation layer for analytics dashboards and the churn intelligence page. The reporting layer was connected to the Databricks warehouse so that business users could consume governed tables rather than raw source extracts.
  
  - A dedicated **logistic regression churn model** was then built using engineered features derived from the Gold layer. The resulting churn scores were stored back into the analytics environment so they could be reused in dashboards and downstream analysis.
  
  - The overall design therefore solved three needs at once:
    
    - reliable ingestion and governance,
    
    - business analytics and reporting,
    
    - and predictive churn scoring from the same underlying data foundation.



## Phase A – Data Engineering

##

## Step O - Source Dataset



## Step 1 – Source Ingestion: GitHub Repository to Bronze Layer

- **Source systems**
  
  - Four raw datasets were used: `customers.csv`, `network_telemetry.csv`, `support_tickets.csv`, and `billing_events.csv`.
  
  - These files represent customer master data, daily network telemetry, support interactions, and monthly billing activity.[

- **Source hosting**
  
  - The raw CSV files were stored in a **public GitHub repository** and treated as the external landing source for ingestion.

- **Ingestion tool**
  
  - **Azure Data Factory (ADF)** was used to orchestrate source ingestion into the data lake.

- **Connectivity design**
  
  - An **HTTP Linked Service** was configured in ADF to access the GitHub-hosted CSV files.
  
  - This allowed each file to be retrieved directly from its public raw URL.

- **Pipeline design**
  
  - A **single ADF pipeline** was implemented.
  
  - The pipeline contained **multiple Copy Data activities**, with one activity assigned to each dataset.
  
  - This made the ingestion process modular and simplified monitoring, debugging, and reruns for individual files.

- **Bronze layer landing pattern**
  
  - Raw source files were landed in **Azure Data Lake Storage Gen2** as the **Bronze layer**.
  
  - Although the source format was CSV, the landed Bronze output was stored as **Parquet** to improve downstream query efficiency and compatibility with Spark-based processing.

- **Execution mode**
  
  - The ingestion pipeline supported both:
    
    - **Manual execution** for testing and development
    
    - **Scheduled execution** for repeatable refreshes in an automated workflow

- **Engineering rationale**
  
  - Separating ingestion from transformation established a clean lakehouse pattern.
  
  - Landing raw data in Bronze preserved source fidelity while preparing the datasets for Silver/Gold transformation stages.
  
  - Using Parquet reduced storage overhead and improved read performance for Databricks-based ETL workloads.

- **Outcome**
  
  - A reliable ingestion layer was created to bring multi-source ISP data from GitHub into ADLS Gen2.
  
  - This Bronze layer became the foundation for the medallion architecture implemented in Databricks.
  
  ## Step 2: Bronze to Silver
  
  ![](C:\Users\Salitha\Downloads\combined_img53_70.png)
  
  - The Bronze-to-Silver notebook was responsible for **reading raw Parquet files from ADLS Gen2 Bronze**, applying basic data quality rules, and producing standardized Silver tables for downstream modelling.
  
  - The Silver layer was designed to create **clean, typed, deduplicated datasets** for `customers`, `network_telemetry`, `support_tickets`, and `billing_events`.
  
  - Each Silver table was loaded separately so that the raw grain of each source could still be preserved while removing obvious issues such as duplicate records, null keys, and incorrect data types.
  
  ## Bronze to Silver transformations
  
  **Customer data**
  
  - - Removed duplicate customer rows using `customer_id`.
    
    - Filtered out records with null `customer_id`.
    
    - Converted `onboarded_date` and `churn_date` to date types.
    
    - Cast churn and add-on fields to boolean.
    
    - Cast `monthly_charge_aud` to double and `tenure_months` to integer.[]
  
  - **Network telemetry**
    
    - Removed duplicate telemetry rows using `telemetry_id`.
    
    - Filtered out records with null `customer_id`.
    
    - Converted `date` to date type.
    
    - Cast `sla_breach` and `outage_flag` to boolean.
    
    - Cast latency, packet loss, and drop metrics to numeric types.
  
  - **Support tickets**
    
    - Removed duplicate tickets using `ticket_id`.
    
    - Filtered out records with null `customer_id`.
    
    - Converted `open_date` and `close_date` to date types.
    
    - Cast `resolved` to boolean and `satisfaction_score` to integer.[]
  
  - **Billing events**
    
    - Removed duplicate billing rows using `billing_id`.
    
    - Filtered out records with null `customer_id`.
    
    - Converted `billing_month` and `payment_date` to date types.
    
    - Cast `amount_charged_aud` to double
  
  ## Silver layer purpose
  
  - The Silver layer acted as the **conformed operational layer** where all source tables were standardised into analysis-ready form.
  
  - This step reduced noise from the raw ingestion layer and made the datasets consistent enough to support joins, aggregations, and dimensional modelling in Gold.
  
  - The Silver outputs were written back to ADLS Gen2 as Parquet, keeping the storage efficient while remaining easy to read in Databricks.

## Step 3: Silver to Gold

The Silver-to-Gold notebook transformed the cleaned Silver tables into a **dimensional model** built for analytics and Power BI consumption.

- The Gold layer introduced both **dimension tables** and **fact tables**, which made the model more suitable for SQL warehouse querying and dashboard performance.

- This design also created a clear business semantic layer on top of the raw operational sources.

## Gold dimension tables

- **DimDate**
  
  - Generated a complete calendar using a date sequence.
  
  - Added `date_key`, `year`, `month`, `day`, `day_of_week`, and `is_weekend`.

- **DimRegion**
  
  - Built from distinct customer regions.
  
  - Assigned a surrogate `region_key` using row numbering.

- **DimIssueCategory**
  
  - Built from distinct support issue categories.
  
  - Assigned a surrogate `issue_category_key`.

- **DimPaymentStatus**
  
  - Built from distinct payment outcomes.
  
  - Assigned a surrogate `payment_status_key`.

- **DimCustomer**
  
  - Joined customers to region lookup data.
  
  - Generated a surrogate `customer_key`.
  
  - Kept customer attributes such as customer type, plan type, contract type, monthly charge, tenure, add-on flags, onboarded date, churn flag, and churn date.

## Gold fact tables

- **FactTelemetry**
  
  - Combined telemetry records with customer and date dimensions.
  
  - Stored measures such as bandwidth, latency, packet loss, connection drops, SLA breaches, and outage indicators.
  
  - Linked data through `customer_key` and `date_key`.

- **FactBilling**
  
  - Combined billing events with customer, payment status, and date dimensions.
  
  - Stored measures such as amount charged and payment method.
  
  - Added `billing_month_key` and `payment_date_key` for time analysis.

- **FactTickets**
  
  - Combined support tickets with customer, issue category, and date dimensions.
  
  - Stored support outcome fields such as channel, resolved status, and satisfaction score.
  
  - Added `open_date_key` and `close_date_key` for lifecycle analysis.

## Gold layer purpose

- The Gold layer was built to support **fast analytical queries, reusable business metrics, and dashboard-friendly structures**.

- The star-schema style model made it easier to analyse churn drivers across customer, network, support, and billing domains without repeatedly rebuilding joins in Power BI.

- The tables were written to Delta and also registered as warehouse tables, making them available both as lakehouse storage and as SQL-serving assets.

## Why this step mattered

- Bronze-to-Silver created **clean, standardised data**.

- Silver-to-Gold created **analytical structure and business meaning**.

- Together, these two notebooks formed the core transformation layer of the project and prepared the platform for the SQL warehouse, dashboards, and churn modelling phases.


