# Databricks notebook source
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

dbutils.fs.ls("abfss://silver@nastorage003.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Liabraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Rading Customer Data**

# COMMAND ----------

df_customers = spark.read.format("parquet") \
    .load("abfss://silver@nastorage003.dfs.core.windows.net/customers/")

# COMMAND ----------

display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Network Telemetry Data**

# COMMAND ----------

df_network = spark.read.parquet("abfss://silver@nastorage003.dfs.core.windows.net/network_telemetry/")

# COMMAND ----------

display(df_network)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Support Tickets Data**

# COMMAND ----------

df_tickets = spark.read.parquet("abfss://silver@nastorage003.dfs.core.windows.net/support_tickets/")

# COMMAND ----------

display(df_tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Biiling Events Data**

# COMMAND ----------

df_billing = spark.read.parquet("abfss://silver@nastorage003.dfs.core.windows.net/billing_events/")

# COMMAND ----------

display(df_billing)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

# MAGIC %md
# MAGIC **DimDate**

# COMMAND ----------

df_customers.createOrReplaceTempView("customers")
df_network.createOrReplaceTempView("network_telemetry")
df_tickets.createOrReplaceTempView("support_tickets")
df_billing.createOrReplaceTempView("billing_events")

# COMMAND ----------

earliest_date_df = spark.sql("""
    SELECT MIN(the_date) AS earliest_date
    FROM (
        SELECT onboarded_date   AS the_date FROM customers
        UNION ALL
        SELECT churn_date       AS the_date FROM customers
        UNION ALL
        SELECT date             AS the_date FROM network_telemetry
        UNION ALL
        SELECT open_date        AS the_date FROM support_tickets
        UNION ALL
        SELECT close_date       AS the_date FROM support_tickets
        UNION ALL
        SELECT billing_month    AS the_date FROM billing_events
        UNION ALL
        SELECT payment_date     AS the_date FROM billing_events
    ) t
    WHERE the_date IS NOT NULL
""")

earliest_date_df.show()

# COMMAND ----------

latest_date_df = spark.sql("""
    SELECT MAX(the_date) AS latest_date
    FROM (
        SELECT onboarded_date   AS the_date FROM customers
        UNION ALL
        SELECT churn_date       AS the_date FROM customers
        UNION ALL
        SELECT date             AS the_date FROM network_telemetry
        UNION ALL
        SELECT open_date        AS the_date FROM support_tickets
        UNION ALL
        SELECT close_date       AS the_date FROM support_tickets
        UNION ALL
        SELECT billing_month    AS the_date FROM billing_events
        UNION ALL
        SELECT payment_date     AS the_date FROM billing_events
    ) t
    WHERE the_date IS NOT NULL
""")

latest_date_df.show()

# COMMAND ----------

from pyspark.sql.functions import (
    col, sequence, explode, year, month, dayofmonth, dayofweek, 
    date_format, when, lit, to_date
)
from pyspark.sql import functions as F

# 1. Define start and end dates for the calendar
start_date = "2020-01-01"
end_date   = "2026-12-31"   # or use today's date if you prefer

# 2. Generate one row per date
dates_df = (
    spark
    .range(1)  # dummy single row
    .select(
        explode(
            sequence(
                to_date(lit(start_date)),
                to_date(lit(end_date)),
                F.expr("interval 1 day")
            )
        ).alias("date")
    )
)

# 3. Add DimDate columns
dim_date_df = (
    dates_df
    .withColumn("date_key", year("date") * 10000 + month("date") * 100 + dayofmonth("date"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_of_week", dayofweek("date"))              # 1 = Sunday ... 7 = Saturday
    .withColumn("is_weekend", when(col("day_of_week").isin(1,7), True).otherwise(False))
)

# COMMAND ----------

dim_date_df.orderBy("date").show(5)
dim_date_df.orderBy(F.desc("date")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **DimRegion**

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# 1) Start from Silver customers and get unique region values
df_regions_raw = df_customers.select("region").distinct()
df_regions_raw.display()

# COMMAND ----------

w = Window.orderBy("region")

# COMMAND ----------

df_dim_region = df_regions_raw.withColumn(
    "region_key",
    row_number().over(w)
)

# COMMAND ----------

df_dim_region.display()

# COMMAND ----------

df_dim_region = df_dim_region.select("region_key", "region")

# COMMAND ----------

# MAGIC %md
# MAGIC **DimIssueCategory**

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_issue_raw = df_tickets.select("issue_category").distinct()

# COMMAND ----------

df_issue_raw.display()

# COMMAND ----------

w_issue = Window.orderBy("issue_category")

df_dim_issue_category = df_issue_raw.withColumn(
    "issue_category_key",
    row_number().over(w_issue)
)

# COMMAND ----------

df_dim_issue_category = df_dim_issue_category.select(
    "issue_category_key",
    "issue_category"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **DimPaymentStatus**

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_payment_raw = df_billing.select("payment_status").distinct()

# COMMAND ----------

df_payment_raw.display()

# COMMAND ----------

w_pay = Window.orderBy("payment_status")

df_dim_payment_status = df_payment_raw.withColumn(
    "payment_status_key",
    row_number().over(w_pay)
)

# COMMAND ----------

df_dim_payment_status = df_dim_payment_status.select(
    "payment_status_key",
    "payment_status"
)

# COMMAND ----------

df_dim_payment_status.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **DimCustomer**

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

df_cust_with_region = (
    df_customers.alias("c")
    .join(
        df_dim_region.alias("r"),
        on=col("c.region") == col("r.region"),
        how="left"
    )
)

# COMMAND ----------

df_cust_with_region.select(
    "c.customer_id", "c.region", "r.region_key"
).show(5)

# COMMAND ----------

w_cust = Window.orderBy("c.customer_id")

dim_customer_df = df_cust_with_region.withColumn(
    "customer_key",
    row_number().over(w_cust)
)

# COMMAND ----------

dim_customer_df.show(5)

# COMMAND ----------

dim_customer_df = dim_customer_df.select(
    "customer_key",          # new surrogate key
    col("c.customer_id").alias("customer_id"),
    col("c.customer_type").alias("customer_type"),
    col("c.customer_id").alias("customer_id_str"),  # only if you want a separate string field
    col("r.region_key").alias("region_key"),
    col("c.plan_type").alias("plan_type"),
    col("c.contract_type").alias("contract_type"),
    col("c.monthly_charge_aud").alias("monthly_charge_aud"),
    col("c.tenure_months").alias("tenure_months"),
    col("c.has_cybersecurity_addon").alias("has_cybersecurity_addon"),
    col("c.has_voip_addon").alias("has_voip_addon"),
    col("c.onboarded_date").alias("onboarded_date"),
    col("c.churn_flag").alias("churn_flag"),
    col("c.churn_date").alias("churn_date")
)

# COMMAND ----------

dim_customer_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **FactTelemetry**

# COMMAND ----------

telemetry_with_customer = (
    df_network.alias("t")
    .join(
        dim_customer_df.alias("c"),
        on=col("t.customer_id") == col("c.customer_id"),
        how="left"
    )
)

# COMMAND ----------

# Select only the columns needed for the fact
telemetry_with_customer = telemetry_with_customer.select(
    col("t.telemetry_id").alias("telemetry_id"),
    col("t.customer_id").alias("customer_id"),
    col("t.date").alias("date"),
    col("t.plan_type").alias("plan_type"),
    col("t.daily_bandwidth_gb").alias("daily_bandwidth_gb"),
    col("t.peak_hour_latency_ms").alias("peak_hour_latency_ms"),
    col("t.off_peak_latency_ms").alias("off_peak_latency_ms"),
    col("t.packet_loss_percent").alias("packet_loss_percent"),
    col("t.connection_drops").alias("connection_drops"),
    col("t.sla_breach").alias("sla_breach"),
    col("t.outage_flag").alias("outage_flag"),
    col("t.node_id").alias("node_id"),
    col("c.customer_key").alias("customer_key")
)

# COMMAND ----------

telemetry_with_date = (
    telemetry_with_customer.alias("t2")
    .join(
        dim_date_df.alias("d"),
        on=col("t2.date") == col("d.date"),
        how="left"
    )
)

# COMMAND ----------

telemetry_with_date.display()

# COMMAND ----------

fact_telemetry_df = telemetry_with_date.select(
    col("t2.telemetry_id").alias("telemetry_id"),
    col("t2.plan_type").alias("plan_type"),
    col("t2.daily_bandwidth_gb").alias("daily_bandwidth_gb"),
    col("t2.peak_hour_latency_ms").alias("peak_hour_latency_ms"),
    col("t2.off_peak_latency_ms").alias("off_peak_latency_ms"),
    col("t2.packet_loss_percent").alias("packet_loss_percent"),
    col("t2.connection_drops").alias("connection_drops"),
    col("t2.sla_breach").alias("sla_breach"),
    col("t2.outage_flag").alias("outage_flag"),
    col("t2.node_id").alias("node_id"),
    col("t2.customer_key").alias("customer_key"),
    col("d.date_key").alias("date_key")
)

# COMMAND ----------

fact_telemetry_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **FactBilling**

# COMMAND ----------

from pyspark.sql.functions import col

# 1) Start from Silver billing
b = df_billing.alias("b")

# 2) Join to DimCustomer → customer_key
b_with_cust = (
    b.join(
        dim_customer_df.alias("c"),
        on=col("b.customer_id") == col("c.customer_id"),
        how="left"
    )
)

# 3) Join to DimPaymentStatus → payment_status_key
b_with_status = (
    b_with_cust
    .join(
        df_dim_payment_status.alias("s"),
        on=col("b.payment_status") == col("s.payment_status"),
        how="left"
    )
)

# 4) Join to DimDate for billing_month_key
b_with_bill_date = (
    b_with_status.alias("b3")
    .join(
        dim_date_df.alias("d_bill"),
        on=col("b3.billing_month") == col("d_bill.date"),
        how="left"
    )
)

# 5) Join to DimDate for payment_date_key
b_with_all_dates = (
    b_with_bill_date
    .join(
        dim_date_df.alias("d_pay"),
        on=col("b3.payment_date") == col("d_pay.date"),
        how="left"
    )
)

# COMMAND ----------

fact_billing_df = b_with_all_dates.select(
    col("b3.billing_id").alias("billing_id"),
    col("b3.billing_month").alias("billing_month"),
    col("b3.amount_charged_aud").alias("amount_charged_aud"),
    col("b3.payment_method").alias("payment_method"),

    col("b3.customer_key").alias("customer_key"),           # from DimCustomer join
    col("b3.payment_status_key").alias("payment_status_key"),  # from DimPaymentStatus join
    col("d_bill.date_key").alias("billing_month_key"),
    col("d_pay.date_key").alias("payment_date_key")
)

# COMMAND ----------

fact_billing_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **FactTickets**

# COMMAND ----------

from pyspark.sql.functions import col

t = df_tickets.alias("t")

# Join to DimCustomer → customer_key
tickets_with_customer = (
    t.join(
        dim_customer_df.alias("c"),
        on=col("t.customer_id") == col("c.customer_id"),
        how="left"
    )
)

# Join to DimIssueCategory → issue_category_key
tickets_with_issue = (
    tickets_with_customer
    .join(
        df_dim_issue_category.alias("ic"),
        on=col("t.issue_category") == col("ic.issue_category"),
        how="left"
    )
)

# Join to DimDate for open_date_key
tickets_with_open_date = (
    tickets_with_issue.alias("t3")
    .join(
        dim_date_df.alias("d_open"),
        on=col("t3.open_date") == col("d_open.date"),
        how="left"
    )
)

# Join to DimDate for close_date_key
tickets_with_all_dates = (
    tickets_with_open_date
    .join(
        dim_date_df.alias("d_close"),
        on=col("t3.close_date") == col("d_close.date"),
        how="left"
    )
)

# COMMAND ----------

fact_tickets_df = tickets_with_all_dates.select(
    col("t3.ticket_id").alias("ticket_id"),
    col("t3.channel").alias("channel"),
    col("t3.resolved").alias("resolved"),
    col("t3.satisfaction_score").alias("satisfaction_score"),

    col("t3.customer_key").alias("customer_key"),              # from customer join
    col("t3.issue_category_key").alias("issue_category_key"),  # from issue category join
    col("d_open.date_key").alias("open_date_key"),
    col("d_close.date_key").alias("close_date_key")
)

# COMMAND ----------

fact_tickets_df = fact_tickets_df.filter(
    col("customer_key").isNotNull()
).filter(
    col("open_date_key").isNotNull()
)


# COMMAND ----------

fact_tickets_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Load

# COMMAND ----------

# --- LOAD: Dimensions to Gold ---

# DimDate
dim_date_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/DimDate/")

# DimRegion
df_dim_region.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/DimRegion/")

# DimIssueCategory
df_dim_issue_category.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/DimIssueCategory/")

# DimPaymentStatus
df_dim_payment_status.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/DimPaymentStatus/")

# DimCustomer
dim_customer_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/DimCustomer/")

# COMMAND ----------

# --- LOAD: Facts to Gold ---

# FactTelemetry
fact_telemetry_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/FactTelemetry/")

# FactBilling
fact_billing_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/FactBilling/")

# FactTickets
fact_tickets_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://gold@nastorage003.dfs.core.windows.net/FactTickets/")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS isp_gold")

dim_date_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.DimDate")

df_dim_region.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.DimRegion")

df_dim_issue_category.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.DimIssueCategory")

df_dim_payment_status.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.DimPaymentStatus")

dim_customer_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.DimCustomer")

fact_telemetry_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.FactTelemetry")

fact_billing_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.FactBilling")

fact_tickets_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("isp_gold.FactTickets")