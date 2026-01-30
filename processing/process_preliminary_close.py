# Databricks notebook source
# MAGIC %md
# MAGIC # Process Preliminary Close Data
# MAGIC Validates and processes BU preliminary close files

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Raw Preliminary Close Data

# COMMAND ----------

df_raw = spark.table(TABLE_BU_PRELIMINARY_RAW)

print(f"📊 Loaded {df_raw.count()} raw preliminary close records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Exchange Rates

# COMMAND ----------

current_period = get_current_closure_period()

# Get latest validated exchange rates
df_fx = spark.table(TABLE_EXCHANGE_RATES) \
    .filter(col("is_validated") == True) \
    .filter(col("closure_period") == current_period) \
    .groupBy("currency") \
    .agg(
        max(struct("rate_date", "rate")).alias("latest")
    ) \
    .select(
        col("currency"),
        col("latest.rate").alias("exchange_rate")
    )

print(f"📊 Loaded {df_fx.count()} exchange rates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for required fields
null_checks = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in ["bu_code", "account_code", "local_amount", "period"]
]).collect()[0]

print("🔍 Null Check Results:")
for field, null_count in null_checks.asDict().items():
    status = "✅" if null_count == 0 else "❌"
    print(f"   {status} {field}: {null_count} nulls")

# COMMAND ----------

# Check for valid BUs
valid_bus = set(BUSINESS_UNITS.keys())
invalid_bus = df_raw.filter(~col("bu_code").isin(valid_bus))
invalid_bu_count = invalid_bus.count()

print(f"{'✅' if invalid_bu_count == 0 else '❌'} Invalid BU codes: {invalid_bu_count}")

if invalid_bu_count > 0:
    print("\n⚠️  Invalid BU codes found:")
    invalid_bus.select("bu_code").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich with BU Information

# COMMAND ----------

# Create BU lookup DataFrame
bu_data = [(code, info["name"]) for code, info in BUSINESS_UNITS.items()]
df_bu_lookup = spark.createDataFrame(bu_data, ["bu_code", "bu_name"])

df_enriched = df_raw.join(df_bu_lookup, "bu_code", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Exchange Rates

# COMMAND ----------

# Join with FX rates to convert to USD
df_with_fx = df_enriched.join(
    df_fx,
    df_enriched["local_currency"] == df_fx["currency"],
    "left"
)

# Calculate USD amount
df_converted = df_with_fx.withColumn(
    "usd_amount",
    when(col("local_currency") == "USD", col("local_amount"))
    .when(col("exchange_rate").isNotNull(), col("local_amount") / col("exchange_rate"))
    .otherwise(lit(None))
)

# Check for records without FX rates
missing_fx = df_converted.filter(
    (col("local_currency") != "USD") & col("exchange_rate").isNull()
).count()

print(f"{'✅' if missing_fx == 0 else '⚠️ '} Records without FX rates: {missing_fx}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Collect validation issues
df_validated = df_converted.withColumn(
    "validation_issues",
    array_distinct(
        array_remove(
            array(
                when(col("bu_code").isNull(), lit("Missing BU code")).otherwise(lit(None)),
                when(~col("bu_code").isin(valid_bus), lit("Invalid BU code")).otherwise(lit(None)),
                when(col("account_code").isNull(), lit("Missing account code")).otherwise(lit(None)),
                when(col("local_amount").isNull(), lit("Missing amount")).otherwise(lit(None)),
                when((col("local_currency") != "USD") & col("exchange_rate").isNull(), 
                     lit("Missing FX rate")).otherwise(lit(None)),
                when(col("period").isNull(), lit("Missing period")).otherwise(lit(None))
            ),
            lit(None)
        )
    )
).withColumn(
    "is_validated",
    when(size(col("validation_issues")) == 0, lit(True)).otherwise(lit(False))
)

# COMMAND ----------

validation_summary = df_validated.groupBy("is_validated").count().collect()
print("📊 Validation Summary:")
for row in validation_summary:
    status = "Valid" if row['is_validated'] else "Invalid"
    print(f"   {status}: {row['count']} records")

# COMMAND ----------

# Show validation issues
invalid_records = df_validated.filter(col("is_validated") == False)
if invalid_records.count() > 0:
    print("\n⚠️  Validation Issues:")
    display(invalid_records.select(
        "bu_code", "account_code", "local_amount", "validation_issues"
    ).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for Processing

# COMMAND ----------

df_processed = df_validated.select(
    expr("uuid()").alias("record_id"),
    col("bu_code"),
    col("bu_name"),
    col("account_code"),
    col("account_description"),
    col("local_currency"),
    col("local_amount"),
    col("usd_amount"),
    col("exchange_rate"),
    col("period"),
    col("cost_center"),
    col("is_validated"),
    col("validation_issues"),
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
    .saveAsTable(TABLE_BU_PRELIMINARY)

print(f"✅ Wrote {df_processed.count()} records to {TABLE_BU_PRELIMINARY}")

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
    "phase": 1,
    "table_name": TABLE_BU_PRELIMINARY,
    "check_type": "validation",
    "check_name": "Preliminary Close Processing",
    "check_result": "PASSED" if failure_rate < 0.02 else "WARNING" if failure_rate < 0.05 else "FAILED",
    "records_checked": total_records,
    "records_failed": total_records - valid_records,
    "failure_rate": failure_rate,
    "details": f"Processed preliminary close with {failure_rate*100:.2f}% failure rate",
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
        bu_name,
        local_currency,
        COUNT(*) as record_count,
        SUM(local_amount) as total_local,
        SUM(usd_amount) as total_usd,
        SUM(CASE WHEN is_validated THEN 1 ELSE 0 END) as valid_records,
        SUM(CASE WHEN NOT is_validated THEN 1 ELSE 0 END) as invalid_records
    FROM {TABLE_BU_PRELIMINARY}
    GROUP BY bu_code, bu_name, local_currency
    ORDER BY bu_code
"""))

# COMMAND ----------

print(f"""
✅ Preliminary Close Processing Complete!

📊 Processing Summary:
   • Total records: {total_records}
   • Valid records: {valid_records}
   • Invalid records: {total_records - valid_records}
   • Failure rate: {failure_rate*100:.2f}%

📈 Next step: Publish preliminary results
""")
