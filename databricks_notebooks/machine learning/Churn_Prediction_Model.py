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

dbutils.fs.ls("abfss://churnml@nastorage003.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Table

# COMMAND ----------

from pyspark.sql import functions as F

ML_BASE_PATH = "abfss://churnml@nastorage003.dfs.core.windows.net"
CHURN_FEATURE_PATH = f"{ML_BASE_PATH}/churn_feature_table"

churn_df = (
    spark.read
    .format("delta")
    .load(CHURN_FEATURE_PATH)
)

display(churn_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing

# COMMAND ----------

# Drop rows where label is null
churn_df_clean = churn_df.filter(churn_df.churn_flag.isNotNull())


# Cast label to integer (0/1)
churn_df_clean = churn_df_clean.withColumn(
    "label",
    F.col("churn_flag").cast("int")
)

display(churn_df_clean.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# Cast booleans to numeric 0/1 for ML
churn_ml_df = (
    churn_df_clean
    .withColumn("has_cybersecurity_addon_num", F.col("has_cybersecurity_addon").cast("int"))
    .withColumn("has_voip_addon_num",          F.col("has_voip_addon").cast("int"))
)


# COMMAND ----------

from pyspark.sql import functions as F

# 1) Define which columns to use
categorical_cols = ["customer_type", "plan_type", "contract_type", "region"]

numeric_cols = [
    "monthly_charge_aud",
    "tenure_months",
    "has_cybersecurity_addon_num",
    "has_voip_addon_num",
    "avg_daily_bandwidth_gb",
    "avg_peak_latency_ms",
    "avg_offpeak_latency_ms",
    "avg_packet_loss_pct",
    "avg_connection_drops",
    "outage_days",
    "sla_breach_days",
    "ticket_count",
    "ticket_resolved_rate",
    "avg_ticket_satisfaction",
    "avg_resolution_days",
    "billing_cycles",
    "avg_amount_charged",
    "late_rate",
    "failed_rate"
]

feature_cols = categorical_cols + numeric_cols
all_cols = feature_cols + ["label", "customer_id", "region"]

# 2) Convert to pandas
pandas_df = churn_ml_df.select(*all_cols).toPandas()

# 3) One-hot encode categoricals in pandas
import pandas as pd

X = pd.get_dummies(
    pandas_df[feature_cols],
    columns=categorical_cols,
    drop_first=True  # avoid redundant dummy columns
)

y = pandas_df["label"]

# COMMAND ----------

X.head()

# COMMAND ----------

from sklearn.impute import SimpleImputer
import numpy as np

# 1) Impute missing numeric values with median
imputer = SimpleImputer(strategy="median")

X = imputer.fit_transform(X)

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

# 1) Train / test split
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y   # keep churn ratio similar in train and test
)

# 2) Define and train logistic regression model
lr = LogisticRegression(
    max_iter=2000,    # allow more iterations
    solver="lbfgs",
    C=1.0             # standard L2 regularisation strength
)

lr.fit(X_train, y_train)

y_pred_proba = lr.predict_proba(X_test)[:, 1]
lr_auc = roc_auc_score(y_test, y_pred_proba)
print(f"Logistic Regression AUC: {lr_auc:.3f}")

# COMMAND ----------

from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import roc_auc_score


# 2) Gradient Boosting
gb = GradientBoostingClassifier(
    n_estimators=200,
    learning_rate=0.1,
    max_depth=3,
    random_state=42
)
gb.fit(X_train, y_train)
gb_proba = gb.predict_proba(X_test)[:, 1]
gb_auc = roc_auc_score(y_test, gb_proba)
print(f"Gradient Boosting AUC: {gb_auc:.3f}")

# 3) Random Forest
rf = RandomForestClassifier(
    n_estimators=200,
    max_depth=None,
    random_state=42,
    n_jobs=-1
)
rf.fit(X_train, y_train)
rf_proba = rf.predict_proba(X_test)[:, 1]
rf_auc = roc_auc_score(y_test, rf_proba)
print(f"Random Forest AUC: {rf_auc:.3f}")


# COMMAND ----------

import pandas as pd

# Create a small DataFrame with label and a few strongest-looking features
leak_check = pandas_df[[
    "label",
    "tenure_months",
    "ticket_count",
    "ticket_resolved_rate",
    "avg_ticket_satisfaction",
    "late_rate",
    "failed_rate",
    "sla_breach_days"
]]

leak_check.groupby("label").mean()

# COMMAND ----------

import pandas as pd

auc_table = pd.DataFrame({
    "model": [
        "Logistic Regression",
        "Gradient Boosting",
        "Random Forest"
    ],
    "auc": [
        lr_auc,
        gb_auc,
        rf_auc
    ]
})

auc_table

# COMMAND ----------

# Remove duplicate columns, keeping the first occurrence
pandas_df = pandas_df.loc[:, ~pandas_df.columns.duplicated()]

print(pandas_df.columns)

# COMMAND ----------

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

# 0) (Safety) remove any duplicate-named columns from pandas_df
pandas_df = pandas_df.loc[:, ~pandas_df.columns.duplicated()]

# 1) Refit LR on full dataset for final model
lr_final = LogisticRegression(
    max_iter=2000,
    solver="lbfgs",
    C=1.0
)

lr_final.fit(X, y)

# 2) Training AUC on all data
y_all_proba = lr_final.predict_proba(X)[:, 1]
print("Training AUC (all data):", roc_auc_score(y, y_all_proba))

# 3) Attach probabilities back to pandas_df
pandas_df["churn_probability"] = y_all_proba

# 4) Build churn scores table with rich filters
scores_pdf = pandas_df[[
    "customer_id",
    "customer_type",
    "plan_type",
    "contract_type",
    "region",
    "tenure_months",
    "monthly_charge_aud",
    "avg_daily_bandwidth_gb",
    "avg_peak_latency_ms",
    "avg_offpeak_latency_ms",
    "avg_packet_loss_pct",
    "avg_connection_drops",
    "outage_days",
    "sla_breach_days",
    "ticket_count",
    "ticket_resolved_rate",
    "avg_ticket_satisfaction",
    "billing_cycles",
    "avg_amount_charged",
    "late_rate",
    "failed_rate",
    "label",
    "churn_probability"
]].copy()

# 5) Rename label for clarity
scores_pdf = scores_pdf.rename(columns={"label": "churn_flag"})

scores_pdf.head()

# COMMAND ----------

# Convert to Spark
scores_sdf = spark.createDataFrame(scores_pdf)

print(scores_sdf.columns)   # sanity check: all unique names

ML_BASE_PATH = "abfss://churnml@nastorage003.dfs.core.windows.net/ml"
CHURN_SCORES_PATH = f"{ML_BASE_PATH}/churn_scores"

(
    scores_sdf
    .write
    .mode("overwrite")
    .format("delta")
    .save(CHURN_SCORES_PATH)
)

display(scores_sdf.limit(5))

# COMMAND ----------

# 2) Also register as a managed table in the warehouse
#    (adjust database/schema name if needed)
# Register table in the isp.ml schema

spark.sql("""
CREATE SCHEMA IF NOT EXISTS isp_ml
""")

scores_sdf.write.mode("overwrite").saveAsTable("isp_ml.churn_scores")

# Quick check from Spark SQL
display(spark.sql("SELECT * FROM isp_ml.churn_scores LIMIT 5"))