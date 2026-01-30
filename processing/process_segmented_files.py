# Databricks notebook source
# MAGIC %md
# MAGIC # Process Segmented Files
# MAGIC Validates and processes segmented financial data

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Segmented Files

# COMMAND ----------

df_raw = spark.table(TABLE_SEGMENTED_FILES_RAW)

print(f"📊 Loaded {df_raw.count()} raw segmented records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Exchange Rates

# COMMAND ----------

current_period = get_current_closure_period()

df_fx = spark.table(TABLE_EXCHANGE_RATES) \
    .filter(col("is_validated") == True) \
    .filter(col("closure_period") == current_period) \
    .groupBy("currency") \
    .agg(max(struct("rate_date", "rate")).alias("latest")) \
    .select(
        col("currency"),
        col("latest.rate").alias("exchange_rate")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for nulls
null_checks = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in ["bu_code", "account_code", "segment_type", "segment_value", "amount"]
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

# Check segment types
expected_segments = ["Product", "Geography", "Customer", "Channel"]
invalid_segments = df_raw.filter(~col("segment_type").isin(expected_segments)).count()
print(f"{'✅' if invalid_segments == 0 else '⚠️ '} Unexpected segment types: {invalid_segments}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Segment Completeness

# COMMAND ----------

# Check if all required segments exist for each BU/Account combination
completeness_check = df_raw.groupBy("bu_code", "account_code") \
    .agg(
        countDistinct("segment_type").alias("segment_count"),
        collect_set("segment_type").alias("segments_present")
    ) \
    .withColumn(
        "is_complete",
        when(col("segment_count") == len(expected_segments), lit(True)).otherwise(lit(False))
    )

incomplete_combinations = completeness_check.filter(col("is_complete") == False).count()
print(f"{'✅' if incomplete_combinations == 0 else '⚠️ '} Incomplete segment combinations: {incomplete_combinations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Exchange Rates

# COMMAND ----------

df_with_fx = df_raw.join(
    df_fx,
    df_raw["currency"] == df_fx["currency"],
    "left"
)

df_converted = df_with_fx.withColumn(
    "usd_amount",
    when(col("currency") == "USD", col("amount"))
    .when(col("exchange_rate").isNotNull(), col("amount") / col("exchange_rate"))
    .otherwise(lit(None))
)

missing_fx = df_converted.filter(
    (col("currency") != "USD") & col("exchange_rate").isNull()
).count()

print(f"{'✅' if missing_fx == 0 else '⚠️ '} Records without FX rates: {missing_fx}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

df_validated = df_converted.withColumn(
    "is_validated",
    when(
        (col("bu_code").isin(valid_bus)) &
        (col("account_code").isNotNull()) &
        (col("segment_type").isin(expected_segments)) &
        (col("segment_value").isNotNull()) &
        (col("amount").isNotNull()) &
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
    expr("uuid()").alias("segment_id"),
    col("bu_code"),
    col("account_code"),
    col("segment_type"),
    col("segment_value"),
    col("amount"),
    col("currency"),
    col("usd_amount"),
    col("period"),
    col("is_validated"),
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
    .saveAsTable(TABLE_SEGMENTED_FILES)

print(f"✅ Wrote {df_processed.count()} records to {TABLE_SEGMENTED_FILES}")

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
    "table_name": TABLE_SEGMENTED_FILES,
    "check_type": "validation",
    "check_name": "Segmented Files Processing",
    "check_result": "PASSED" if failure_rate < 0.02 else "WARNING" if failure_rate < 0.05 else "FAILED",
    "records_checked": total_records,
    "records_failed": total_records - valid_records,
    "failure_rate": failure_rate,
    "details": f"Processed segmented files with {failure_rate*100:.2f}% failure rate",
    "checked_at": datetime.now()
}])

quality_check.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_DATA_QUALITY)

print(f"✅ Logged data quality results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Segment Type

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        segment_type,
        COUNT(*) as record_count,
        COUNT(DISTINCT segment_value) as unique_values,
        SUM(usd_amount) as total_usd,
        AVG(usd_amount) as avg_usd
    FROM {TABLE_SEGMENTED_FILES}
    WHERE is_validated = true
    GROUP BY segment_type
    ORDER BY segment_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        COUNT(*) as record_count,
        COUNT(DISTINCT segment_type) as segment_types,
        COUNT(DISTINCT account_code) as accounts,
        SUM(usd_amount) as total_usd
    FROM {TABLE_SEGMENTED_FILES}
    WHERE is_validated = true
    GROUP BY bu_code
    ORDER BY bu_code
"""))

# COMMAND ----------

print(f"""
✅ Segmented Files Processing Complete!

📊 Processing Summary:
   • Total records: {total_records}
   • Valid records: {valid_records}
   • Invalid records: {total_records - valid_records}
   • Failure rate: {failure_rate*100:.2f}%

📈 Next step: Process forecast files
""")
