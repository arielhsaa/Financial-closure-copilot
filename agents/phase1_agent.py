# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 Agent: Exchange Rate & Preliminary Data Collection
# MAGIC Automated agent for Phase 1 of financial closure

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Initialization

# COMMAND ----------

# Get parameters
dbutils.widgets.text("closure_period", get_current_closure_period(), "Closure Period")
closure_period = dbutils.widgets.get("closure_period")

agent_id = "agent-phase1-001"
agent_name = "Exchange Rate & Preliminary Data Agent"
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
        "phase": 1,
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
# MAGIC ## Task 1: Process Exchange Rates

# COMMAND ----------

print("📊 Task 1: Processing Exchange Rates...")
task_start = datetime.now()

try:
    dbutils.notebook.run("../processing/process_exchange_rates", timeout_seconds=1800)
    
    # Check results
    fx_count = spark.table(TABLE_EXCHANGE_RATES).filter(
        col("closure_period") == closure_period
    ).count()
    
    print(f"✅ Exchange rates processed: {fx_count} records")
    log_task("Process Exchange Rates", "COMPLETED", fx_count)
    
except Exception as e:
    print(f"❌ Exchange rate processing failed: {str(e)}")
    log_task("Process Exchange Rates", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Validate Exchange Rates

# COMMAND ----------

print("🔍 Task 2: Validating Exchange Rates...")

# Check for significant variances
variances = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {TABLE_EXCHANGE_RATES}
    WHERE closure_period = '{closure_period}'
      AND ABS(variance_from_prior) > {FX_VARIANCE_THRESHOLD}
      AND is_validated = false
""").collect()[0]['count']

if variances > 0:
    print(f"⚠️  Found {variances} rates with significant variances - review required")
    
    # Create notification
    notification = spark.createDataFrame([{
        "notification_id": str(uuid.uuid4()),
        "closure_period": closure_period,
        "phase": 1,
        "severity": "WARNING",
        "message": f"Found {variances} exchange rates exceeding variance threshold",
        "recipient": "finance.ops@company.com",
        "sent_at": datetime.now(),
        "acknowledged_at": None
    }])
    notification.write.format("delta").mode("append").saveAsTable(f"{FULL_SCHEMA_AUDIT}.notifications")
else:
    print("✅ All exchange rates within acceptable variance")

log_task("Validate Exchange Rates", "COMPLETED", variances)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Process BU Preliminary Close

# COMMAND ----------

print("📊 Task 3: Processing BU Preliminary Close Files...")

try:
    dbutils.notebook.run("../processing/process_preliminary_close", timeout_seconds=1800)
    
    # Check results
    prelim_count = spark.table(TABLE_BU_PRELIMINARY).filter(
        col("closure_period") == closure_period
    ).count()
    
    print(f"✅ Preliminary close processed: {prelim_count} records")
    log_task("Process Preliminary Close", "COMPLETED", prelim_count)
    
except Exception as e:
    print(f"❌ Preliminary close processing failed: {str(e)}")
    log_task("Process Preliminary Close", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Check BU Completeness

# COMMAND ----------

print("🔍 Task 4: Checking BU Submission Completeness...")

# Check which BUs have submitted
submitted_bus = spark.sql(f"""
    SELECT DISTINCT bu_code
    FROM {TABLE_BU_PRELIMINARY}
    WHERE closure_period = '{closure_period}'
      AND is_validated = true
""").collect()

submitted_bu_codes = {row['bu_code'] for row in submitted_bus}
expected_bu_codes = set(BUSINESS_UNITS.keys())
missing_bu_codes = expected_bu_codes - submitted_bu_codes

if missing_bu_codes:
    print(f"⚠️  Missing submissions from: {', '.join(missing_bu_codes)}")
    
    # Send reminders
    for bu_code in missing_bu_codes:
        bu_info = BUSINESS_UNITS[bu_code]
        notification = spark.createDataFrame([{
            "notification_id": str(uuid.uuid4()),
            "closure_period": closure_period,
            "phase": 1,
            "severity": "WARNING",
            "message": f"Preliminary close file missing from {bu_info['name']}",
            "recipient": bu_info['contact'],
            "sent_at": datetime.now(),
            "acknowledged_at": None
        }])
        notification.write.format("delta").mode("append").saveAsTable(f"{FULL_SCHEMA_AUDIT}.notifications")
else:
    print("✅ All business units have submitted preliminary close files")

log_task("Check BU Completeness", "COMPLETED", len(submitted_bu_codes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Data Quality Summary

# COMMAND ----------

print("📊 Task 5: Generating Data Quality Summary...")

# Get quality metrics
quality_summary = spark.sql(f"""
    SELECT 
        table_name,
        check_result,
        records_checked,
        records_failed,
        ROUND(failure_rate * 100, 2) as failure_rate_pct
    FROM {TABLE_DATA_QUALITY}
    WHERE closure_period = '{closure_period}'
      AND phase = 1
    ORDER BY checked_at DESC
""")

print("\n📋 Data Quality Summary:")
quality_summary.show(truncate=False)

# Check if any critical failures
critical_failures = quality_summary.filter(col("check_result") == "FAILED").count()

if critical_failures > 0:
    print(f"❌ Found {critical_failures} critical data quality failures")
    log_task("Data Quality Check", "WARNING", critical_failures, 
            f"{critical_failures} critical failures found")
else:
    print("✅ All data quality checks passed")
    log_task("Data Quality Check", "COMPLETED", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1 Summary

# COMMAND ----------

duration = (datetime.now() - start_time).total_seconds() / 60

print(f"""
{'='*60}
✅ PHASE 1 COMPLETE
{'='*60}

📅 Closure Period: {closure_period}
⏱️  Duration: {duration:.2f} minutes

📊 Results:
   • Exchange rates processed: {fx_count}
   • Preliminary records processed: {prelim_count}
   • Business units submitted: {len(submitted_bu_codes)}/{len(expected_bu_codes)}
   • Data quality issues: {critical_failures}

📝 Next Phase: Adjustments & Review
{'='*60}
""")

# Return summary for orchestrator
dbutils.notebook.exit(f"Phase 1 completed successfully in {duration:.2f} minutes")
