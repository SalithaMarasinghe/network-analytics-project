# Databricks notebook source
# ============================================================
# Notebook: ml_build_churn_features
# Purpose:  Build customer-level feature table for churn model
# Inputs:   Gold parquet tables in ADLS Gen2
# Output:   Parquet/Delta table: gold_ml/churn_feature_table
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

storage_account = "nastorage003"

adls_oauth_configs = {
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": "<YOUR_AZURE_CLIENT_ID>",
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": "<YOUR_AZURE_CLIENT_SECRET>",
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": "https://login.microsoftonline.com/<YOUR_AZURE_TENANT_ID>/oauth2/token",
}

# COMMAND ----------

for key, value in adls_oauth_configs.items():
    spark.conf.set(key, value)

dbutils.fs.ls("abfss://gold@nastorage003.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window


# COMMAND ----------

# -----------------------------
# 1. Configuration
# -----------------------------

# Base path to your Gold container in ADLS Gen2
# Adjust storage account / container names to match your environment
GOLD_BASE_PATH = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# Output path for ML features (you can also use Delta tables instead)
CHURN_FEATURE_PATH = f"{GOLD_BASE_PATH}/churn_feature_table"

# COMMAND ----------

#-----------------------------
# 2. Load Gold Dim/Fact tables
# -----------------------------
customers_df = (spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/DimCustomer"))

telemetry_df = (spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/FactTelemetry"))

tickets_df = (spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/FactTickets"))

billing_df = (spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/FactBilling"))

dim_date_df = (
    spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/DimDate"))

dim_payment_status_df = (
    spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/DimPaymentStatus"))

dim_region_df = (
    spark.read
    .format("delta")
    .load(f"{GOLD_BASE_PATH}/DimRegion")
)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the feature dataset

# COMMAND ----------

# -----------------------------
# 3. Customer base features
# -----------------------------

# First select from DimCustomer
cust_base = (
    customers_df
    .select(
        "customer_key",
        "customer_id",
        "customer_type",
        "region_key",
        "plan_type",
        "contract_type",
        "monthly_charge_aud",
        "tenure_months",
        "has_cybersecurity_addon",
        "has_voip_addon",
        "churn_flag",
        "churn_date"
    )
)

# Then join DimRegion to add the human-readable region label
cust_feats = (
    cust_base
    .join(
        dim_region_df.select("region_key", "region"),
        on="region_key",
        how="left"
    )
    .withColumnRenamed("region_enum", "region")
)

# -----------------------------
# 4. Network telemetry features
# -----------------------------

telemetry_feats = (
    telemetry_df
    .groupBy("customer_key")
    .agg(
        F.avg("daily_bandwidth_gb").alias("avg_daily_bandwidth_gb"),
        F.avg("peak_hour_latency_ms").alias("avg_peak_latency_ms"),
        F.avg("off_peak_latency_ms").alias("avg_offpeak_latency_ms"),
        F.avg("packet_loss_percent").alias("avg_packet_loss_pct"),
        F.avg("connection_drops").alias("avg_connection_drops"),
        F.sum(F.col("outage_flag").cast("int")).alias("outage_days"),
        F.sum(F.col("sla_breach").cast("int")).alias("sla_breach_days")
    )
)

# -----------------------------
# 5. Support ticket features (with resolution_days via DimDate)
# -----------------------------

# Prepare two views of DimDate for join: one for open, one for close
open_date_dim = dim_date_df.select(
    F.col("date_key").alias("open_date_key"),
    F.col("date").alias("open_date")
)

close_date_dim = dim_date_df.select(
    F.col("date_key").alias("close_date_key"),
    F.col("date").alias("close_date")
)

# Join support_tickets to DimDate to get real dates
tickets_with_dates = (
    tickets_df
    .join(open_date_dim, on="open_date_key", how="left")
    .join(close_date_dim, on="close_date_key", how="left")
    .withColumn(
        "resolution_days",
        F.when(
            (F.col("resolved") == True) & F.col("close_date").isNotNull(),
            F.datediff(F.col("close_date"), F.col("open_date"))
        )
    )
)

tickets_feats = (
    tickets_with_dates
    .groupBy("customer_key")
    .agg(
        F.count("*").alias("ticket_count"),
        F.avg(F.col("resolved").cast("int")).alias("ticket_resolved_rate"),
        F.avg("satisfaction_score").alias("avg_ticket_satisfaction"),
        F.avg("resolution_days").alias("avg_resolution_days")
    )
)

# -----------------------------
# 6. Billing features (with DimPaymentStatus)
# -----------------------------

# Join billing_events with DimPaymentStatus to get descriptive status
billing_with_status = (
    billing_df
    .join(
        dim_payment_status_df.select("payment_status_key", "payment_status"),
        on="payment_status_key",
        how="left"
    )
)

billing_feats = (
    billing_with_status
    .groupBy("customer_key")
    .agg(
        F.count("*").alias("billing_cycles"),
        F.avg("amount_charged_aud").alias("avg_amount_charged"),
        F.avg(F.when(F.col("payment_status") == "late",   1).otherwise(0)).alias("late_rate"),
        F.avg(F.when(F.col("payment_status") == "failed", 1).otherwise(0)).alias("failed_rate")
    )
)

# -----------------------------
# 7. Join all feature sets
# -----------------------------
# (Assuming you already defined cust_feats, telemetry_feats, tickets_feats above)

churn_features_df = (
    cust_feats
    .join(telemetry_feats, on="customer_key", how="left")
    .join(tickets_feats,   on="customer_key", how="left")
    .join(billing_feats,   on="customer_key", how="left")
)


# COMMAND ----------

churn_features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save the Dataset

# COMMAND ----------

# -----------------------------
# 8. Save the dataset
# -----------------------------

churn_features_final = churn_features_df.select(
    # IDs / labels for interpretation
    "customer_id",
    "region",
    
    # customer/commercial features
    "customer_type",
    "plan_type",
    "contract_type",
    "monthly_charge_aud",
    "tenure_months",
    "has_cybersecurity_addon",
    "has_voip_addon",
    
    # telemetry features
    "avg_daily_bandwidth_gb",
    "avg_peak_latency_ms",
    "avg_offpeak_latency_ms",
    "avg_packet_loss_pct",
    "avg_connection_drops",
    "outage_days",
    "sla_breach_days",
    
    # support features
    "ticket_count",
    "ticket_resolved_rate",
    "avg_ticket_satisfaction",
    "avg_resolution_days",
    
    # billing features
    "billing_cycles",
    "avg_amount_charged",
    "late_rate",
    "failed_rate",
    
    # label
    "churn_flag",
    "churn_date"
)

(
    churn_features_final
    .write
    .mode("overwrite")
    .format("delta")
    .save(CHURN_FEATURE_PATH)
)