# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Preliminary Results
# MAGIC Generates and publishes preliminary close results for review

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Preliminary Close Data

# COMMAND ----------

current_period = get_current_closure_period()

df_preliminary = spark.table(TABLE_BU_PRELIMINARY) \
    .filter(col("is_validated") == True) \
    .filter(col("closure_period") == current_period)

print(f"📊 Loaded {df_preliminary.count()} validated preliminary records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate by BU and Account

# COMMAND ----------

df_aggregated = df_preliminary.groupBy(
    "bu_code",
    "bu_name",
    "account_code",
    "account_description"
).agg(
    sum("usd_amount").alias("preliminary_amount"),
    first("local_currency").alias("currency")
)

print(f"✅ Aggregated to {df_aggregated.count()} account-level records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Account Category

# COMMAND ----------

df_with_category = df_aggregated.withColumn(
    "account_category",
    when(col("account_code").like("4%"), lit("Revenue"))
    .when(col("account_code").like("5%"), lit("COGS"))
    .when(col("account_code").like("6%"), lit("OpEx"))
    .when(col("account_code").like("7%"), lit("Other Income/Expense"))
    .when(col("account_code").like("8%"), lit("Tax"))
    .otherwise(lit("Other"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Prior Period for Comparison

# COMMAND ----------

# Calculate prior period
year, month = map(int, current_period.split('-'))
prior_month = month - 1
prior_year = year
if prior_month == 0:
    prior_month = 12
    prior_year -= 1
prior_period = f"{prior_year:04d}-{prior_month:02d}"

print(f"📅 Comparing against prior period: {prior_period}")

# COMMAND ----------

# Get prior period results (if they exist)
try:
    df_prior = spark.table(TABLE_PRELIMINARY_RESULTS) \
        .filter(col("closure_period") == prior_period) \
        .groupBy("bu_code", "account_code") \
        .agg(max("preliminary_amount").alias("prior_period_amount"))
    
    prior_exists = True
    print(f"✅ Found {df_prior.count()} prior period records")
except:
    prior_exists = False
    print("⚠️  No prior period data available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Variances

# COMMAND ----------

if prior_exists:
    df_with_prior = df_with_category.join(
        df_prior,
        ["bu_code", "account_code"],
        "left"
    )
else:
    df_with_prior = df_with_category.withColumn("prior_period_amount", lit(None).cast("double"))

df_with_variance = df_with_prior.withColumn(
    "variance_amount",
    when(col("prior_period_amount").isNotNull(),
         col("preliminary_amount") - col("prior_period_amount")
    ).otherwise(lit(None))
).withColumn(
    "variance_percent",
    when((col("prior_period_amount").isNotNull()) & (col("prior_period_amount") != 0),
         (col("variance_amount") / abs(col("prior_period_amount"))) * 100
    ).otherwise(lit(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Results

# COMMAND ----------

# Get version number (increment if results already exist for this period)
try:
    max_version = spark.sql(f"""
        SELECT COALESCE(MAX(version), 0) as max_ver 
        FROM {TABLE_PRELIMINARY_RESULTS}
        WHERE closure_period = '{current_period}'
    """).collect()[0]['max_ver']
    version = max_version + 1
except:
    version = 1

print(f"📝 Publishing version {version}")

# COMMAND ----------

df_results = df_with_variance.select(
    expr("uuid()").alias("result_id"),
    lit(current_period).alias("closure_period"),
    col("bu_code"),
    col("bu_name"),
    col("account_code"),
    col("account_description"),
    col("account_category"),
    col("preliminary_amount"),
    col("currency"),
    col("preliminary_amount").alias("usd_amount"),  # Already in USD
    coalesce(col("prior_period_amount"), lit(0.0)).alias("prior_period_amount"),
    coalesce(col("variance_amount"), lit(0.0)).alias("variance_amount"),
    coalesce(col("variance_percent"), lit(0.0)).alias("variance_percent"),
    current_timestamp().alias("published_at"),
    lit(version).alias("version")
)

print(f"✅ Prepared {df_results.count()} result records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Reporting Table

# COMMAND ----------

df_results.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_PRELIMINARY_RESULTS)

print(f"✅ Published {df_results.count()} preliminary results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Summary Metrics

# COMMAND ----------

summary = df_results.groupBy("account_category").agg(
    sum("preliminary_amount").alias("total_amount"),
    avg("variance_percent").alias("avg_variance_pct")
).orderBy("account_category")

print("\n📊 Preliminary Results Summary:")
summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Closure Status

# COMMAND ----------

# Update closure status
try:
    closure_status = spark.createDataFrame([{
        "closure_id": str(uuid.uuid4()),
        "closure_period": current_period,
        "current_phase": 2,
        "phase_status": "PRELIMINARY_PUBLISHED",
        "started_at": datetime.now(),
        "expected_completion": None,
        "actual_completion": None,
        "created_by": "phase2_agent",
        "updated_at": datetime.now(),
        "notes": f"Preliminary results published - Version {version}"
    }])
    
    closure_status.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TABLE_CLOSURE_STATUS)
    
    print("✅ Updated closure status")
except Exception as e:
    print(f"⚠️  Could not update closure status: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Results by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_name,
        account_category,
        SUM(preliminary_amount) as total_amount,
        SUM(prior_period_amount) as total_prior,
        SUM(variance_amount) as total_variance,
        CASE 
            WHEN SUM(prior_period_amount) != 0 
            THEN (SUM(variance_amount) / ABS(SUM(prior_period_amount))) * 100
            ELSE 0
        END as variance_pct
    FROM {TABLE_PRELIMINARY_RESULTS}
    WHERE closure_period = '{current_period}'
    AND version = {version}
    GROUP BY bu_name, account_category
    ORDER BY bu_name, account_category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Top Variances

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_name,
        account_description,
        account_category,
        preliminary_amount,
        prior_period_amount,
        variance_amount,
        variance_percent
    FROM {TABLE_PRELIMINARY_RESULTS}
    WHERE closure_period = '{current_period}'
    AND version = {version}
    AND ABS(variance_amount) > 10000
    ORDER BY ABS(variance_amount) DESC
    LIMIT 20
"""))

# COMMAND ----------

print(f"""
✅ Preliminary Results Published!

📊 Publication Summary:
   • Closure Period: {current_period}
   • Version: {version}
   • Total Records: {df_results.count()}
   • Business Units: {df_results.select('bu_code').distinct().count()}
   • Account Categories: {df_results.select('account_category').distinct().count()}

📝 Next steps:
   1. Review preliminary results
   2. Schedule review meeting (FP&A + Tech)
   3. Receive and process accounting cuts
""")
