# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 Agent: Segmented & Forecast Data Collection
# MAGIC Automated agent for Phase 3 of financial closure

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# Get parameters
dbutils.widgets.text("closure_period", get_current_closure_period(), "Closure Period")
closure_period = dbutils.widgets.get("closure_period")

agent_id = "agent-phase3-001"
agent_name = "Segmented & Forecast Data Agent"
start_time = datetime.now()

print(f"🤖 {agent_name} activated")
print(f"📅 Closure Period: {closure_period}")

# COMMAND ----------

def log_task(task_name, status, records=0, error=None):
    """Log agent task"""
    log_entry = spark.createDataFrame([{
        "log_id": str(uuid.uuid4()),
        "agent_id": agent_id,
        "agent_name": agent_name,
        "closure_period": closure_period,
        "phase": 3,
        "task_name": task_name,
        "status": status,
        "started_at": start_time,
        "completed_at": datetime.now() if status in ["COMPLETED", "FAILED"] else None,
        "duration_seconds": (datetime.now() - start_time).total_seconds(),
        "records_processed": records,
        "error_message": error,
        "metadata": {}
    }])
    log_entry.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_LOGS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Process Segmented Files

# COMMAND ----------

print("📊 Task 1: Processing Segmented Files...")

try:
    dbutils.notebook.run("../processing/process_segmented_files", timeout_seconds=1800)
    
    segment_count = spark.table(TABLE_SEGMENTED_FILES).filter(
        col("closure_period") == closure_period
    ).count()
    
    print(f"✅ Processed {segment_count} segmented records")
    log_task("Process Segmented Files", "COMPLETED", segment_count)
    
except Exception as e:
    print(f"❌ Segmented files processing failed: {str(e)}")
    log_task("Process Segmented Files", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Validate Segment Completeness

# COMMAND ----------

print("🔍 Task 2: Validating Segment Completeness...")

# Check segment coverage by BU
segment_coverage = spark.sql(f"""
    SELECT 
        bu_code,
        COUNT(DISTINCT segment_type) as segments_present,
        COLLECT_SET(segment_type) as segment_types
    FROM {TABLE_SEGMENTED_FILES}
    WHERE closure_period = '{closure_period}'
      AND is_validated = true
    GROUP BY bu_code
""")

expected_segments = ["Product", "Geography", "Customer", "Channel"]
incomplete_bus = []

for row in segment_coverage.collect():
    if row['segments_present'] < len(expected_segments):
        incomplete_bus.append(row['bu_code'])
        missing = set(expected_segments) - set(row['segment_types'])
        print(f"⚠️  {row['bu_code']}: Missing segments {missing}")

if incomplete_bus:
    # Send notifications
    for bu_code in incomplete_bus:
        bu_info = BUSINESS_UNITS[bu_code]
        notification = spark.createDataFrame([{
            "notification_id": str(uuid.uuid4()),
            "closure_period": closure_period,
            "phase": 3,
            "severity": "WARNING",
            "message": f"Incomplete segment data from {bu_info['name']}",
            "recipient": bu_info['contact'],
            "sent_at": datetime.now(),
            "acknowledged_at": None
        }])
        notification.write.format("delta").mode("append").saveAsTable(f"{FULL_SCHEMA_AUDIT}.notifications")
    
    log_task("Validate Segment Completeness", "WARNING", len(incomplete_bus), 
            f"{len(incomplete_bus)} BUs with incomplete segments")
else:
    print("✅ All BUs have complete segment data")
    log_task("Validate Segment Completeness", "COMPLETED", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Process Forecast Files

# COMMAND ----------

print("📊 Task 3: Processing Forecast Files...")

try:
    dbutils.notebook.run("../processing/process_forecast_files", timeout_seconds=1800)
    
    forecast_count = spark.table(TABLE_FORECAST_FILES).filter(
        col("closure_period") == closure_period
    ).count()
    
    print(f"✅ Processed {forecast_count} forecast records")
    log_task("Process Forecast Files", "COMPLETED", forecast_count)
    
except Exception as e:
    print(f"❌ Forecast files processing failed: {str(e)}")
    log_task("Process Forecast Files", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Validate Forecast Data

# COMMAND ----------

print("🔍 Task 4: Validating Forecast Data...")

# Check forecast submission by BU
forecast_coverage = spark.sql(f"""
    SELECT 
        bu_code,
        COUNT(DISTINCT forecast_period) as periods_forecasted,
        COUNT(*) as forecast_records
    FROM {TABLE_FORECAST_FILES}
    WHERE closure_period = '{closure_period}'
      AND is_validated = true
    GROUP BY bu_code
""").collect()

submitted_forecast_bus = {row['bu_code'] for row in forecast_coverage}
expected_bus = set(BUSINESS_UNITS.keys())
missing_forecast_bus = expected_bus - submitted_forecast_bus

if missing_forecast_bus:
    print(f"⚠️  Missing forecast data from: {', '.join(missing_forecast_bus)}")
    
    for bu_code in missing_forecast_bus:
        bu_info = BUSINESS_UNITS[bu_code]
        notification = spark.createDataFrame([{
            "notification_id": str(uuid.uuid4()),
            "closure_period": closure_period,
            "phase": 3,
            "severity": "WARNING",
            "message": f"Missing forecast data from {bu_info['name']}",
            "recipient": bu_info['contact'],
            "sent_at": datetime.now(),
            "acknowledged_at": None
        }])
        notification.write.format("delta").mode("append").saveAsTable(f"{FULL_SCHEMA_AUDIT}.notifications")
    
    log_task("Validate Forecast Data", "WARNING", len(missing_forecast_bus),
            f"{len(missing_forecast_bus)} BUs missing forecast")
else:
    print("✅ All BUs have submitted forecast data")
    log_task("Validate Forecast Data", "COMPLETED", len(submitted_forecast_bus))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Data Quality Summary

# COMMAND ----------

print("📊 Task 5: Generating Data Quality Summary...")

quality_summary = spark.sql(f"""
    SELECT 
        table_name,
        check_result,
        records_checked,
        records_failed,
        ROUND(failure_rate * 100, 2) as failure_rate_pct
    FROM {TABLE_DATA_QUALITY}
    WHERE closure_period = '{closure_period}'
      AND phase = 3
    ORDER BY checked_at DESC
""")

print("\n📋 Data Quality Summary:")
quality_summary.show(truncate=False)

critical_failures = quality_summary.filter(col("check_result") == "FAILED").count()

if critical_failures > 0:
    print(f"❌ Found {critical_failures} critical data quality failures")
    log_task("Data Quality Check", "WARNING", critical_failures)
else:
    print("✅ All data quality checks passed")
    log_task("Data Quality Check", "COMPLETED", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3 Summary

# COMMAND ----------

duration = (datetime.now() - start_time).total_seconds() / 60

print(f"""
{'='*60}
✅ PHASE 3 COMPLETE
{'='*60}

📅 Closure Period: {closure_period}
⏱️  Duration: {duration:.2f} minutes

📊 Results:
   • Segmented records processed: {segment_count}
   • Forecast records processed: {forecast_count}
   • BUs with incomplete segments: {len(incomplete_bus)}
   • BUs with forecast data: {len(submitted_forecast_bus)}/{len(expected_bus)}
   • Data quality issues: {critical_failures}

📝 Next Phase: Review & Validation
{'='*60}
""")

dbutils.notebook.exit(f"Phase 3 completed successfully in {duration:.2f} minutes")
