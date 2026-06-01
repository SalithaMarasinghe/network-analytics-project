# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access
# MAGIC

# COMMAND ----------

secret = "<YOUR_AZURE_CLIENT_SECRET>"
app_id = "<YOUR_AZURE_CLIENT_ID>"
dir_id = "<YOUR_AZURE_TENANT_ID>"

# COMMAND ----------

# DBTITLE 1,Untitled
storage_account = "nastorage003"

adls_oauth_configs = {
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": "<YOUR_AZURE_CLIENT_ID>",
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": "<YOUR_AZURE_CLIENT_SECRET>",
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": "https://login.microsoftonline.com/<YOUR_AZURE_TENANT_ID>/oauth2/token",
}

# COMMAND ----------

# DBTITLE 1,Cell 4
for key, value in adls_oauth_configs.items():
    spark.conf.set(key, value)

dbutils.fs.ls("abfss://bronze@nastorage003.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract customer data**

# COMMAND ----------

df_customers = spark.read.format("parquet") \
    .load("abfss://bronze@nastorage003.dfs.core.windows.net/customers/")

# COMMAND ----------

display(df_customers)


# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Network Telemetry Data**

# COMMAND ----------

df_network = spark.read.parquet("abfss://bronze@nastorage003.dfs.core.windows.net/network_telemetry/")


# COMMAND ----------

display(df_network)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Support Tickets Data**

# COMMAND ----------

df_tickets = spark.read.parquet("abfss://bronze@nastorage003.dfs.core.windows.net/support_tickets/")

# COMMAND ----------

display(df_tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract Biiling Events Data**

# COMMAND ----------

df_billing = spark.read.parquet("abfss://bronze@nastorage003.dfs.core.windows.net/billing_events/")

# COMMAND ----------

display(df_billing)

# COMMAND ----------

df_customers.printSchema()
df_customers.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

# MAGIC %md
# MAGIC **customers**

# COMMAND ----------

from pyspark.sql.functions import col, to_date, when

df_customers_silver = df_customers \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("onboarded_date", to_date("onboarded_date")) \
    .withColumn("churn_date", to_date("churn_date")) \
    .withColumn("churn_flag", col("churn_flag").cast("boolean")) \
    .withColumn("has_cybersecurity_addon", col("has_cybersecurity_addon").cast("boolean")) \
    .withColumn("has_voip_addon", col("has_voip_addon").cast("boolean")) \
    .withColumn("monthly_charge_aud", col("monthly_charge_aud").cast("double")) \
    .withColumn("tenure_months", col("tenure_months").cast("integer"))

# COMMAND ----------

# MAGIC %md
# MAGIC **network_telemetry**

# COMMAND ----------

df_network_silver = df_network \
    .dropDuplicates(["telemetry_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("date", to_date("date")) \
    .withColumn("sla_breach", col("sla_breach").cast("boolean")) \
    .withColumn("outage_flag", col("outage_flag").cast("boolean")) \
    .withColumn("packet_loss_percent", col("packet_loss_percent").cast("double")) \
    .withColumn("peak_hour_latency_ms", col("peak_hour_latency_ms").cast("double")) \
    .withColumn("off_peak_latency_ms", col("off_peak_latency_ms").cast("double")) \
    .withColumn("connection_drops", col("connection_drops").cast("integer"))

# COMMAND ----------

# MAGIC %md
# MAGIC **support_tickets**

# COMMAND ----------

df_tickets_silver = df_tickets \
    .dropDuplicates(["ticket_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("open_date", to_date("open_date")) \
    .withColumn("close_date", to_date("close_date")) \
    .withColumn("resolved", col("resolved").cast("boolean")) \
    .withColumn("satisfaction_score", col("satisfaction_score").cast("integer"))

# COMMAND ----------

# MAGIC %md
# MAGIC **billing_events**

# COMMAND ----------

df_billing_silver = df_billing \
    .dropDuplicates(["billing_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("billing_month", to_date("billing_month")) \
    .withColumn("payment_date", to_date("payment_date")) \
    .withColumn("amount_charged_aud", col("amount_charged_aud").cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Load

# COMMAND ----------

df_customers_silver.write.format("parquet").mode("overwrite") \
    .save("abfss://silver@nastorage003.dfs.core.windows.net/customers/")

df_network_silver.write.format("parquet").mode("overwrite") \
    .save("abfss://silver@nastorage003.dfs.core.windows.net/network_telemetry/")

df_tickets_silver.write.format("parquet").mode("overwrite") \
    .save("abfss://silver@nastorage003.dfs.core.windows.net/support_tickets/")

df_billing_silver.write.format("parquet").mode("overwrite") \
    .save("abfss://silver@nastorage003.dfs.core.windows.net/billing_events/")