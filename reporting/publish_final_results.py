# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Final Results
# MAGIC Generates and publishes final close results with segments and forecast comparison

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Required Data

# COMMAND ----------

current_period = get_current_closure_period()

# Load preliminary close
df_preliminary = spark.table(TABLE_BU_PRELIMINARY) \
    .filter(col("is_validated") == True) \
    .filter(col("closure_period") == current_period)

# Load accounting cuts
df_cuts = spark.table(TABLE_ACCOUNTING_CUTS) \
    .filter(col("is_validated") == True) \
    .filter(col("closure_period") == current_period)

# Load segmented data
df_segments = spark.table(TABLE_SEGMENTED_FILES) \
    .filter(col("is_validated") == True) \
    .filter(col("closure_period") == current_period)

# Load forecast data
df_forecast = spark.table(TABLE_FORECAST_FILES) \
    .filter(col("is_validated") == True) \
    .filter(col("forecast_period") == current_period)

print(f"""
📊 Data Loaded:
   • Preliminary: {df_preliminary.count()} records
   • Accounting Cuts: {df_cuts.count()} records
   • Segments: {df_segments.count()} records
   • Forecast: {df_forecast.count()} records
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Preliminary Amounts by Account

# COMMAND ----------

df_prelim_summary = df_preliminary.groupBy(
    "bu_code",
    "bu_name",
    "account_code"
).agg(
    sum("usd_amount").alias("preliminary_amount")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Total Adjustments by Account

# COMMAND ----------

df_adjustments_summary = df_cuts.groupBy(
    "bu_code",
    "account_code"
).agg(
    sum("usd_amount").alias("adjustments_amount")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivot Segments

# COMMAND ----------

# Get segments for each BU/Account combination
df_product = df_segments.filter(col("segment_type") == "Product") \
    .groupBy("bu_code", "account_code") \
    .agg(first("segment_value").alias("segment_product"))

df_geography = df_segments.filter(col("segment_type") == "Geography") \
    .groupBy("bu_code", "account_code") \
    .agg(first("segment_value").alias("segment_geography"))

df_customer = df_segments.filter(col("segment_type") == "Customer") \
    .groupBy("bu_code", "account_code") \
    .agg(first("segment_value").alias("segment_customer"))

df_channel = df_segments.filter(col("segment_type") == "Channel") \
    .groupBy("bu_code", "account_code") \
    .agg(first("segment_value").alias("segment_channel"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Forecast Amounts

# COMMAND ----------

df_forecast_summary = df_forecast.groupBy(
    "bu_code",
    "account_code"
).agg(
    sum("usd_amount").alias("forecast_amount")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Prior Period Results

# COMMAND ----------

# Calculate prior period
year, month = map(int, current_period.split('-'))
prior_month = month - 1
prior_year = year
if prior_month == 0:
    prior_month = 12
    prior_year -= 1
prior_period = f"{prior_year:04d}-{prior_month:02d}"

# Get prior period final results
try:
    df_prior = spark.table(TABLE_FINAL_RESULTS) \
        .filter(col("closure_period") == prior_period) \
        .filter(col("is_final") == True) \
        .groupBy("bu_code", "account_code") \
        .agg(max("final_amount").alias("prior_period_amount"))
    
    prior_exists = True
    print(f"✅ Found prior period data: {prior_period}")
except:
    prior_exists = False
    print("⚠️  No prior period data available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Data

# COMMAND ----------

# Start with preliminary amounts
df_combined = df_prelim_summary

# Add adjustments
df_combined = df_combined.join(
    df_adjustments_summary,
    ["bu_code", "account_code"],
    "left"
)

# Add segments
df_combined = df_combined \
    .join(df_product, ["bu_code", "account_code"], "left") \
    .join(df_geography, ["bu_code", "account_code"], "left") \
    .join(df_customer, ["bu_code", "account_code"], "left") \
    .join(df_channel, ["bu_code", "account_code"], "left")

# Add forecast
df_combined = df_combined.join(
    df_forecast_summary,
    ["bu_code", "account_code"],
    "left"
)

# Add prior period
if prior_exists:
    df_combined = df_combined.join(
        df_prior,
        ["bu_code", "account_code"],
        "left"
    )
else:
    df_combined = df_combined.withColumn("prior_period_amount", lit(None).cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Final Amounts and Variances

# COMMAND ----------

# Add BU region info
bu_regions = {code: info["region"] for code, info in BUSINESS_UNITS.items()}
df_bu_regions = spark.createDataFrame(
    [(code, region) for code, region in bu_regions.items()],
    ["bu_code", "region"]
)

df_combined = df_combined.join(df_bu_regions, "bu_code", "left")

# Add account info
df_combined = df_combined.join(
    df_preliminary.select("account_code", "account_description").distinct(),
    "account_code",
    "left"
)

# Calculate final amount
df_final = df_combined.withColumn(
    "final_amount",
    coalesce(col("preliminary_amount"), lit(0.0)) + 
    coalesce(col("adjustments_amount"), lit(0.0))
).withColumn(
    "account_category",
    when(col("account_code").like("4%"), lit("Revenue"))
    .when(col("account_code").like("5%"), lit("COGS"))
    .when(col("account_code").like("6%"), lit("OpEx"))
    .when(col("account_code").like("7%"), lit("Other Income/Expense"))
    .when(col("account_code").like("8%"), lit("Tax"))
    .otherwise(lit("Other"))
).withColumn(
    "forecast_variance",
    when(col("forecast_amount").isNotNull(),
         col("final_amount") - col("forecast_amount")
    ).otherwise(lit(None))
).withColumn(
    "yoy_variance",
    when(col("prior_period_amount").isNotNull(),
         col("final_amount") - col("prior_period_amount")
    ).otherwise(lit(None))
).withColumn(
    "yoy_variance_percent",
    when((col("prior_period_amount").isNotNull()) & (col("prior_period_amount") != 0),
         (col("yoy_variance") / abs(col("prior_period_amount"))) * 100
    ).otherwise(lit(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Final Results

# COMMAND ----------

df_results = df_final.select(
    expr("uuid()").alias("result_id"),
    lit(current_period).alias("closure_period"),
    col("bu_code"),
    col("bu_name"),
    col("region"),
    col("account_code"),
    col("account_description"),
    col("account_category"),
    coalesce(col("segment_product"), lit("Unallocated")).alias("segment_product"),
    coalesce(col("segment_geography"), lit("Unallocated")).alias("segment_geography"),
    coalesce(col("segment_customer"), lit("Unallocated")).alias("segment_customer"),
    coalesce(col("segment_channel"), lit("Unallocated")).alias("segment_channel"),
    coalesce(col("preliminary_amount"), lit(0.0)).alias("preliminary_amount"),
    coalesce(col("adjustments_amount"), lit(0.0)).alias("adjustments_amount"),
    col("final_amount"),
    lit("USD").alias("currency"),
    col("final_amount").alias("usd_amount"),
    col("forecast_amount"),
    coalesce(col("forecast_variance"), lit(0.0)).alias("forecast_variance"),
    coalesce(col("prior_period_amount"), lit(0.0)).alias("prior_period_amount"),
    coalesce(col("yoy_variance"), lit(0.0)).alias("yoy_variance"),
    coalesce(col("yoy_variance_percent"), lit(0.0)).alias("yoy_variance_percent"),
    current_timestamp().alias("published_at"),
    lit(True).alias("is_final"),
    lit("system").alias("approved_by"),
    current_timestamp().alias("approved_at")
)

print(f"✅ Prepared {df_results.count()} final result records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Final Results Table

# COMMAND ----------

# Clear existing results for this period
spark.sql(f"""
    DELETE FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
""")

df_results.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_FINAL_RESULTS)

print(f"✅ Published {df_results.count()} final results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate KPI Metrics

# COMMAND ----------

# Calculate key metrics
kpi_data = []

# Revenue metrics
total_revenue = df_results.filter(col("account_category") == "Revenue") \
    .agg(sum("final_amount")).collect()[0][0] or 0

# Gross margin
total_cogs = abs(df_results.filter(col("account_category") == "COGS") \
    .agg(sum("final_amount")).collect()[0][0] or 0)
gross_margin = ((total_revenue - total_cogs) / total_revenue * 100) if total_revenue != 0 else 0

# Operating expenses
total_opex = abs(df_results.filter(col("account_category") == "OpEx") \
    .agg(sum("final_amount")).collect()[0][0] or 0)

# Operating margin
operating_income = total_revenue - total_cogs - total_opex
operating_margin = (operating_income / total_revenue * 100) if total_revenue != 0 else 0

kpi_data.extend([
    {
        "metric_id": str(uuid.uuid4()),
        "closure_period": current_period,
        "metric_name": "Total Revenue",
        "metric_category": "Revenue",
        "metric_value": total_revenue,
        "target_value": None,
        "variance_from_target": None,
        "bu_code": "ALL",
        "region": "Global",
        "calculated_at": datetime.now()
    },
    {
        "metric_id": str(uuid.uuid4()),
        "closure_period": current_period,
        "metric_name": "Gross Margin %",
        "metric_category": "Profitability",
        "metric_value": gross_margin,
        "target_value": None,
        "variance_from_target": None,
        "bu_code": "ALL",
        "region": "Global",
        "calculated_at": datetime.now()
    },
    {
        "metric_id": str(uuid.uuid4()),
        "closure_period": current_period,
        "metric_name": "Operating Margin %",
        "metric_category": "Profitability",
        "metric_value": operating_margin,
        "target_value": None,
        "variance_from_target": None,
        "bu_code": "ALL",
        "region": "Global",
        "calculated_at": datetime.now()
    }
])

# Revenue by BU
for bu_code in BUSINESS_UNITS.keys():
    bu_revenue = df_results.filter(
        (col("bu_code") == bu_code) & (col("account_category") == "Revenue")
    ).agg(sum("final_amount")).collect()[0][0] or 0
    
    kpi_data.append({
        "metric_id": str(uuid.uuid4()),
        "closure_period": current_period,
        "metric_name": "Revenue",
        "metric_category": "Revenue",
        "metric_value": bu_revenue,
        "target_value": None,
        "variance_from_target": None,
        "bu_code": bu_code,
        "region": BUSINESS_UNITS[bu_code]["region"],
        "calculated_at": datetime.now()
    })

# Write KPIs
df_kpis = spark.createDataFrame(kpi_data)
df_kpis.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_KPI_METRICS)

print(f"✅ Generated {len(kpi_data)} KPI metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Closure Status

# COMMAND ----------

closure_status = spark.createDataFrame([{
    "closure_id": str(uuid.uuid4()),
    "closure_period": current_period,
    "current_phase": 5,
    "phase_status": "COMPLETED",
    "started_at": datetime.now(),
    "expected_completion": None,
    "actual_completion": datetime.now(),
    "created_by": "phase5_agent",
    "updated_at": datetime.now(),
    "notes": "Final results published successfully"
}])

closure_status.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_CLOSURE_STATUS)

print("✅ Updated closure status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Final Results Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_name,
        account_category,
        SUM(preliminary_amount) as preliminary,
        SUM(adjustments_amount) as adjustments,
        SUM(final_amount) as final_amount,
        SUM(forecast_amount) as forecast,
        SUM(forecast_variance) as vs_forecast,
        SUM(prior_period_amount) as prior_period,
        SUM(yoy_variance) as yoy_variance
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
    GROUP BY bu_name, account_category
    ORDER BY bu_name, account_category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display KPI Dashboard

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        metric_name,
        CASE 
            WHEN bu_code = 'ALL' THEN 'Global'
            ELSE bu_code
        END as business_unit,
        ROUND(metric_value, 2) as value
    FROM {TABLE_KPI_METRICS}
    WHERE closure_period = '{current_period}'
    ORDER BY 
        CASE WHEN bu_code = 'ALL' THEN 0 ELSE 1 END,
        bu_code,
        metric_name
"""))

# COMMAND ----------

print(f"""
✅ Final Results Published!

📊 Closure Summary for {current_period}:
   • Total Revenue: ${total_revenue:,.2f}
   • Gross Margin: {gross_margin:.2f}%
   • Operating Margin: {operating_margin:.2f}%
   • Business Units: {df_results.select('bu_code').distinct().count()}
   • Total Records: {df_results.count()}

🎉 Financial Close Cycle Complete!
""")
