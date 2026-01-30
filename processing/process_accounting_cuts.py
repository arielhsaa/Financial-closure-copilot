# Databricks notebook source
# MAGIC %md
# MAGIC # Process Accounting Cuts
# MAGIC Validates and processes accounting adjustment data (1st and 2nd cuts)

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Accounting Cuts

# COMMAND ----------

df_raw = spark.table(TABLE_ACCOUNTING_CUTS_RAW)

print(f"📊 Loaded {df_raw.count()} raw accounting cut records")

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

# Check for required fields
null_checks = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in ["cut_number", "bu_code", "account_code", "adjustment_amount"]
]).collect()[0]

print("🔍 Null Check Results:")
for field, null_count in null_checks.asDict().items():
    status = "✅" if null_count == 0 else "❌"
    print(f"   {status} {field}: {null_count} nulls")

# COMMAND ----------

# Check for valid cut numbers
invalid_cuts = df_raw.filter(~col("cut_number").isin([1, 2])).count()
print(f"{'✅' if invalid_cuts == 0 else '❌'} Invalid cut numbers: {invalid_cuts}")

# Check for valid BUs
valid_bus = set(BUSINESS_UNITS.keys())
invalid_bus = df_raw.filter(~col("bu_code").isin(valid_bus)).count()
print(f"{'✅' if invalid_bus == 0 else '❌'} Invalid BU codes: {invalid_bus}")

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
    when(col("currency") == "USD", col("adjustment_amount"))
    .when(col("exchange_rate").isNotNull(), col("adjustment_amount") / col("exchange_rate"))
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
        (col("cut_number").isin([1, 2])) &
        (col("bu_code").isin(valid_bus)) &
        (col("account_code").isNotNull()) &
        (col("adjustment_amount").isNotNull()) &
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
# MAGIC ## Add Approval Information (Mock)

# COMMAND ----------

# In production, this would come from an approval workflow
df_with_approval = df_validated.withColumn(
    "approved_by",
    when(col("is_validated"), lit("system_auto_approved")).otherwise(lit(None))
).withColumn(
    "approved_at",
    when(col("is_validated"), current_timestamp()).otherwise(lit(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for Processing

# COMMAND ----------

df_processed = df_with_approval.select(
    expr("uuid()").alias("cut_id"),
    col("cut_number"),
    col("bu_code"),
    col("account_code"),
    col("adjustment_type"),
    col("adjustment_amount"),
    col("currency"),
    col("usd_amount"),
    col("description"),
    col("period"),
    col("approved_by"),
    col("approved_at"),
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
    .saveAsTable(TABLE_ACCOUNTING_CUTS)

print(f"✅ Wrote {df_processed.count()} records to {TABLE_ACCOUNTING_CUTS}")

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
    "phase": 2,
    "table_name": TABLE_ACCOUNTING_CUTS,
    "check_type": "validation",
    "check_name": "Accounting Cuts Processing",
    "check_result": "PASSED" if failure_rate < 0.02 else "WARNING" if failure_rate < 0.05 else "FAILED",
    "records_checked": total_records,
    "records_failed": total_records - valid_records,
    "failure_rate": failure_rate,
    "details": f"Processed accounting cuts with {failure_rate*100:.2f}% failure rate",
    "checked_at": datetime.now()
}])

quality_check.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_DATA_QUALITY)

print(f"✅ Logged data quality results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Cut

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        cut_number,
        COUNT(*) as adjustment_count,
        SUM(usd_amount) as total_usd_adjustments,
        AVG(usd_amount) as avg_usd_adjustment,
        COUNT(DISTINCT bu_code) as bu_count,
        COUNT(DISTINCT adjustment_type) as adjustment_types,
        SUM(CASE WHEN is_validated THEN 1 ELSE 0 END) as valid_count
    FROM {TABLE_ACCOUNTING_CUTS}
    GROUP BY cut_number
    ORDER BY cut_number
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        cut_number,
        COUNT(*) as adjustment_count,
        SUM(usd_amount) as total_usd_adjustments,
        COUNT(DISTINCT adjustment_type) as adjustment_types
    FROM {TABLE_ACCOUNTING_CUTS}
    WHERE is_validated = true
    GROUP BY bu_code, cut_number
    ORDER BY bu_code, cut_number
"""))

# COMMAND ----------

print(f"""
✅ Accounting Cuts Processing Complete!

📊 Processing Summary:
   • Total adjustments: {total_records}
   • Valid adjustments: {valid_records}
   • Invalid adjustments: {total_records - valid_records}
   • Failure rate: {failure_rate*100:.2f}%

📈 Next step: Apply adjustments to preliminary results
""")
