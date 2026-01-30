# Databricks notebook source
# MAGIC %md
# MAGIC # Process Forecast Files
# MAGIC Validates and processes forecast data and forecast FX rates

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Forecast Files

# COMMAND ----------

df_raw = spark.table(TABLE_FORECAST_FILES_RAW)

print(f"📊 Loaded {df_raw.count()} raw forecast records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Forecast FX Rates

# COMMAND ----------

df_forecast_fx = spark.table(TABLE_FORECAST_FX_RAW)

print(f"📊 Loaded {df_forecast_fx.count()} forecast FX records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks - Forecast Files

# COMMAND ----------

# Check for nulls
null_checks = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in ["bu_code", "account_code", "forecast_period", "forecast_amount"]
]).collect()[0]

print("🔍 Null Check Results:")
for field, null_count in null_checks.asDict().items():
    status = "✅" if null_count == 0 else "❌"
    print(f"   {status} {field}: {null_count} nulls")

# COMMAND ----------

# Check for valid BUs
valid_bus = set(BUSINESS_UNITS.keys())
invalid_bus = df_raw.filter(~col("bu_code").isin(valid_bus)).count()
print(f"{'✅' if invalid_bus == 0 else '❌'} Invalid BU codes: {invalid_bus}")

# Check for future dates
past_forecasts = df_raw.filter(col("forecast_period") < get_current_closure_period()).count()
print(f"{'✅' if past_forecasts == 0 else '⚠️ '} Past period forecasts: {past_forecasts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Forecast FX Rates

# COMMAND ----------

# Extract year-month from forecast date
df_fx_monthly = df_forecast_fx.withColumn(
    "forecast_period",
    date_format(col("forecast_date"), "yyyy-MM")
)

# Get average rate per currency per month
df_fx_avg = df_fx_monthly.groupBy("currency", "forecast_period") \
    .agg(avg("forecast_rate").alias("exchange_rate"))

# Join forecast files with FX rates
df_with_fx = df_raw.join(
    df_fx_avg,
    (df_raw["currency"] == df_fx_avg["currency"]) &
    (df_raw["forecast_period"] == df_fx_avg["forecast_period"]),
    "left"
).drop(df_fx_avg["currency"]).drop(df_fx_avg["forecast_period"])

# COMMAND ----------

# Calculate USD amounts
df_converted = df_with_fx.withColumn(
    "usd_amount",
    when(col("currency") == "USD", col("forecast_amount"))
    .when(col("exchange_rate").isNotNull(), col("forecast_amount") / col("exchange_rate"))
    .otherwise(lit(None))
)

missing_fx = df_converted.filter(
    (col("currency") != "USD") & col("exchange_rate").isNull()
).count()

print(f"{'✅' if missing_fx == 0 else '⚠️ '} Forecast records without FX rates: {missing_fx}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Variance from Prior Version

# COMMAND ----------

# In production, this would compare against previous forecast version
# For simulation, we'll add a placeholder variance calculation
df_with_variance = df_converted.withColumn(
    "variance_from_prior",
    lit(0.0)  # Placeholder - would calculate actual variance in production
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

current_period = get_current_closure_period()

df_validated = df_with_variance.withColumn(
    "is_validated",
    when(
        (col("bu_code").isin(valid_bus)) &
        (col("account_code").isNotNull()) &
        (col("forecast_period").isNotNull()) &
        (col("forecast_period") >= current_period) &
        (col("forecast_amount").isNotNull()) &
        ((col("currency") == "USD") | col("exchange_rate").isNotNull()),
        lit(True)
    ).otherwise(lit(False))
)

validation_summary = df_validated.groupBy("is_validated").count().collect()
print("📊 Validation Summary:")
for row in validation_summary:
    status = "Valid" if row['is_validated'] else "Invalid"
    print(f"   {status}: {row['count']} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for Processing

# COMMAND ----------

df_processed = df_validated.select(
    expr("uuid()").alias("forecast_id"),
    col("bu_code"),
    col("account_code"),
    col("forecast_period"),
    col("forecast_amount"),
    col("currency"),
    col("usd_amount"),
    col("forecast_type"),
    col("version"),
    col("is_validated"),
    col("variance_from_prior"),
    current_timestamp().alias("processed_at"),
    lit(current_period).alias("closure_period")
)

print(f"✅ Prepared {df_processed.count()} records for processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Processed Table

# COMMAND ----------

df_processed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_FORECAST_FILES)

print(f"✅ Wrote {df_processed.count()} records to {TABLE_FORECAST_FILES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Data Quality Results

# COMMAND ----------

total_records = df_processed.count()
valid_records = df_processed.filter(col("is_validated") == True).count()
failure_rate = (total_records - valid_records) / total_records if total_records > 0 else 0

quality_check = spark.createDataFrame([{
    "check_id": str(uuid.uuid4()),
    "closure_period": current_period,
    "phase": 3,
    "table_name": TABLE_FORECAST_FILES,
    "check_type": "validation",
    "check_name": "Forecast Files Processing",
    "check_result": "PASSED" if failure_rate < 0.02 else "WARNING" if failure_rate < 0.05 else "FAILED",
    "records_checked": total_records,
    "records_failed": total_records - valid_records,
    "failure_rate": failure_rate,
    "details": f"Processed forecast files with {failure_rate*100:.2f}% failure rate",
    "checked_at": datetime.now()
}])

quality_check.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_DATA_QUALITY)

print(f"✅ Logged data quality results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        currency,
        COUNT(*) as forecast_count,
        COUNT(DISTINCT forecast_period) as periods,
        COUNT(DISTINCT account_code) as accounts,
        SUM(usd_amount) as total_forecast_usd
    FROM {TABLE_FORECAST_FILES}
    WHERE is_validated = true
    GROUP BY bu_code, currency
    ORDER BY bu_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast Trend

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        forecast_period,
        SUM(CASE WHEN usd_amount > 0 THEN usd_amount ELSE 0 END) as revenue_forecast,
        SUM(CASE WHEN usd_amount < 0 THEN usd_amount ELSE 0 END) as cost_forecast,
        SUM(usd_amount) as net_forecast
    FROM {TABLE_FORECAST_FILES}
    WHERE is_validated = true
    GROUP BY forecast_period
    ORDER BY forecast_period
"""))

# COMMAND ----------

print(f"""
✅ Forecast Files Processing Complete!

📊 Processing Summary:
   • Total records: {total_records}
   • Valid records: {valid_records}
   • Invalid records: {total_records - valid_records}
   • Failure rate: {failure_rate*100:.2f}%

📈 Next step: Generate final results with forecasts
""")
