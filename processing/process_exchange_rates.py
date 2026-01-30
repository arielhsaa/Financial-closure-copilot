# Databricks notebook source
# MAGIC %md
# MAGIC # Process Exchange Rates
# MAGIC Validates and processes raw exchange rate data

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Exchange Rates

# COMMAND ----------

df_raw = spark.table(TABLE_EXCHANGE_RATES_RAW)

print(f"📊 Loaded {df_raw.count()} raw exchange rate records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for nulls
null_checks = df_raw.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in ["currency", "rate", "rate_date"]]
).collect()[0]

print("🔍 Null Check Results:")
for field, null_count in null_checks.asDict().items():
    status = "✅" if null_count == 0 else "❌"
    print(f"   {status} {field}: {null_count} nulls")

# COMMAND ----------

# Check for positive rates
negative_rates = df_raw.filter(col("rate") <= 0).count()
print(f"{'✅' if negative_rates == 0 else '❌'} Negative/zero rates: {negative_rates}")

# COMMAND ----------

# Check for valid currencies
valid_currencies = set(CURRENCIES)
invalid_currencies = df_raw.filter(~col("currency").isin(valid_currencies)).count()
print(f"{'✅' if invalid_currencies == 0 else '❌'} Invalid currencies: {invalid_currencies}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Variance from Prior Day

# COMMAND ----------

# Window function to get previous day's rate
window_spec = Window.partitionBy("currency").orderBy("rate_date")

df_with_variance = df_raw.withColumn(
    "prior_rate",
    lag("rate", 1).over(window_spec)
).withColumn(
    "variance_from_prior",
    when(col("prior_rate").isNotNull(),
         ((col("rate") - col("prior_rate")) / col("prior_rate") * 100)
    ).otherwise(lit(0.0))
)

print("✅ Calculated variance from prior day")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Significant Variances

# COMMAND ----------

# Flag rates with variance > threshold
significant_variances = df_with_variance.filter(
    abs(col("variance_from_prior")) > FX_VARIANCE_THRESHOLD
)

variance_count = significant_variances.count()
print(f"⚠️  Found {variance_count} rates with variance > {FX_VARIANCE_THRESHOLD}%")

if variance_count > 0:
    print("\n🔍 Significant Variances:")
    significant_variances.select(
        "currency", "rate_date", "rate", "prior_rate", "variance_from_prior"
    ).show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Status

# COMMAND ----------

# Determine validation status
df_validated = df_with_variance.withColumn(
    "is_validated",
    when(
        (col("currency").isin(valid_currencies)) &
        (col("rate") > 0) &
        (col("rate").isNotNull()) &
        (abs(col("variance_from_prior")) <= FX_VARIANCE_THRESHOLD),
        lit(True)
    ).otherwise(lit(False))
).withColumn(
    "validation_notes",
    when(~col("currency").isin(valid_currencies), lit("Invalid currency"))
    .when(col("rate") <= 0, lit("Invalid rate"))
    .when(col("rate").isNull(), lit("Missing rate"))
    .when(abs(col("variance_from_prior")) > FX_VARIANCE_THRESHOLD, 
          lit(f"Variance exceeds {FX_VARIANCE_THRESHOLD}%"))
    .otherwise(lit(None))
)

# COMMAND ----------

validation_summary = df_validated.groupBy("is_validated").count().collect()
print("📊 Validation Summary:")
for row in validation_summary:
    status = "Valid" if row['is_validated'] else "Invalid"
    print(f"   {status}: {row['count']} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for Processing

# COMMAND ----------

current_period = get_current_closure_period()

df_processed = df_validated.select(
    expr("uuid()").alias("rate_id"),
    col("currency"),
    col("rate"),
    col("rate_date"),
    col("source"),
    col("variance_from_prior"),
    col("is_validated"),
    col("validation_notes"),
    current_timestamp().alias("processed_at"),
    lit(current_period).alias("closure_period")
)

print(f"✅ Prepared {df_processed.count()} records for processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Processed Table

# COMMAND ----------

# Write to processed table (merge to avoid duplicates)
df_processed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_EXCHANGE_RATES)

print(f"✅ Wrote {df_processed.count()} records to {TABLE_EXCHANGE_RATES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Data Quality Results

# COMMAND ----------

# Calculate quality metrics
total_records = df_processed.count()
valid_records = df_processed.filter(col("is_validated") == True).count()
failure_rate = (total_records - valid_records) / total_records if total_records > 0 else 0

# Insert quality check results
quality_check = spark.createDataFrame([{
    "check_id": str(uuid.uuid4()),
    "closure_period": current_period,
    "phase": 1,
    "table_name": TABLE_EXCHANGE_RATES,
    "check_type": "validation",
    "check_name": "Exchange Rate Processing",
    "check_result": "PASSED" if failure_rate < 0.05 else "WARNING" if failure_rate < 0.10 else "FAILED",
    "records_checked": total_records,
    "records_failed": total_records - valid_records,
    "failure_rate": failure_rate,
    "details": f"Processed exchange rates with {failure_rate*100:.2f}% failure rate",
    "checked_at": datetime.now()
}])

quality_check.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_DATA_QUALITY)

print(f"✅ Logged data quality results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
✅ Exchange Rate Processing Complete!

📊 Processing Summary:
   • Total records processed: {total_records}
   • Valid records: {valid_records}
   • Invalid records: {total_records - valid_records}
   • Failure rate: {failure_rate*100:.2f}%
   • Significant variances: {variance_count}

📈 Next step: Process BU preliminary close files
""")

# COMMAND ----------

# Display latest rates
display(spark.sql(f"""
    SELECT 
        currency,
        rate_date,
        rate,
        variance_from_prior,
        is_validated,
        validation_notes
    FROM {TABLE_EXCHANGE_RATES}
    WHERE rate_date >= date_sub(current_date(), 7)
    ORDER BY rate_date DESC, currency
"""))
