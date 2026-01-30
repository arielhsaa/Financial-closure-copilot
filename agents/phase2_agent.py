# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 Agent: Adjustments & Review
# MAGIC Automated agent for Phase 2 of financial closure

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# Get parameters
dbutils.widgets.text("closure_period", get_current_closure_period(), "Closure Period")
closure_period = dbutils.widgets.get("closure_period")

agent_id = "agent-phase2-001"
agent_name = "Adjustments & Review Agent"
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
        "phase": 2,
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
# MAGIC ## Task 1: Publish Preliminary Results

# COMMAND ----------

print("📊 Task 1: Publishing Preliminary Results...")

try:
    dbutils.notebook.run("../reporting/publish_preliminary_results", timeout_seconds=1800)
    
    prelim_count = spark.table(TABLE_PRELIMINARY_RESULTS).filter(
        col("closure_period") == closure_period
    ).count()
    
    print(f"✅ Published {prelim_count} preliminary results")
    log_task("Publish Preliminary Results", "COMPLETED", prelim_count)
    
except Exception as e:
    print(f"❌ Publishing failed: {str(e)}")
    log_task("Publish Preliminary Results", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Schedule Preliminary Review

# COMMAND ----------

print("📅 Task 2: Scheduling Preliminary Review Meeting...")

# In production, this would integrate with calendar systems
# For now, create a meeting record

meeting_record = spark.createDataFrame([{
    "approval_id": str(uuid.uuid4()),
    "closure_period": closure_period,
    "phase": 2,
    "approval_type": "PRELIMINARY_REVIEW_MEETING",
    "required_approver": "FP&A + Tech Teams",
    "status": "SCHEDULED",
    "requested_at": datetime.now(),
    "approved_at": None,
    "comments": "Preliminary results review - auto-scheduled by agent"
}])

meeting_record.write.format("delta").mode("append").saveAsTable(TABLE_APPROVALS)

print("✅ Preliminary review meeting scheduled")
log_task("Schedule Review Meeting", "COMPLETED", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Process Accounting Cuts

# COMMAND ----------

print("📊 Task 3: Processing Accounting Cuts...")

try:
    dbutils.notebook.run("../processing/process_accounting_cuts", timeout_seconds=1800)
    
    cuts_count = spark.table(TABLE_ACCOUNTING_CUTS).filter(
        col("closure_period") == closure_period
    ).count()
    
    print(f"✅ Processed {cuts_count} accounting adjustments")
    log_task("Process Accounting Cuts", "COMPLETED", cuts_count)
    
except Exception as e:
    print(f"❌ Accounting cuts processing failed: {str(e)}")
    log_task("Process Accounting Cuts", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Validate Adjustments

# COMMAND ----------

print("🔍 Task 4: Validating Adjustments...")

# Check for large adjustments that need review
large_adjustments = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {TABLE_ACCOUNTING_CUTS}
    WHERE closure_period = '{closure_period}'
      AND ABS(usd_amount) > 100000
      AND is_validated = true
""").collect()[0]['count']

if large_adjustments > 0:
    print(f"⚠️  Found {large_adjustments} large adjustments (>$100K) - flagged for review")
    
    # Create approval request
    approval = spark.createDataFrame([{
        "approval_id": str(uuid.uuid4()),
        "closure_period": closure_period,
        "phase": 2,
        "approval_type": "LARGE_ADJUSTMENT_REVIEW",
        "required_approver": "Controller",
        "status": "PENDING",
        "requested_at": datetime.now(),
        "approved_at": None,
        "comments": f"{large_adjustments} adjustments >$100K require approval"
    }])
    approval.write.format("delta").mode("append").saveAsTable(TABLE_APPROVALS)

# Check reconciliation
reconciliation = spark.sql(f"""
    SELECT 
        SUM(preliminary_amount) as prelim_total,
        SUM(adjustments_amount) as adj_total
    FROM (
        SELECT SUM(usd_amount) as preliminary_amount, 0 as adjustments_amount
        FROM {TABLE_BU_PRELIMINARY}
        WHERE closure_period = '{closure_period}' AND is_validated = true
        UNION ALL
        SELECT 0 as preliminary_amount, SUM(usd_amount) as adjustments_amount
        FROM {TABLE_ACCOUNTING_CUTS}
        WHERE closure_period = '{closure_period}' AND is_validated = true
    )
""").collect()[0]

print(f"""
📊 Reconciliation:
   • Preliminary total: ${reconciliation['prelim_total']:,.2f}
   • Adjustments total: ${reconciliation['adj_total']:,.2f}
""")

log_task("Validate Adjustments", "COMPLETED", large_adjustments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Check Cut Completeness

# COMMAND ----------

print("🔍 Task 5: Checking Accounting Cut Completeness...")

# Check if both cuts are received
cuts_by_number = spark.sql(f"""
    SELECT 
        cut_number,
        COUNT(*) as count
    FROM {TABLE_ACCOUNTING_CUTS}
    WHERE closure_period = '{closure_period}'
      AND is_validated = true
    GROUP BY cut_number
    ORDER BY cut_number
""").collect()

cuts_received = {row['cut_number'] for row in cuts_by_number}

if 1 in cuts_received and 2 in cuts_received:
    print("✅ Both accounting cuts (1st and 2nd) received")
    log_task("Check Cut Completeness", "COMPLETED", 2)
elif 1 in cuts_received:
    print("⚠️  Only 1st accounting cut received, waiting for 2nd cut")
    log_task("Check Cut Completeness", "WARNING", 1, "2nd cut pending")
else:
    print("⚠️  No accounting cuts received yet")
    log_task("Check Cut Completeness", "WARNING", 0, "Cuts pending")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2 Summary

# COMMAND ----------

duration = (datetime.now() - start_time).total_seconds() / 60

print(f"""
{'='*60}
✅ PHASE 2 COMPLETE
{'='*60}

📅 Closure Period: {closure_period}
⏱️  Duration: {duration:.2f} minutes

📊 Results:
   • Preliminary results published: {prelim_count}
   • Accounting cuts processed: {cuts_count}
   • Large adjustments flagged: {large_adjustments}
   • Cuts received: {len(cuts_received)}/2

📝 Next Phase: Segmented & Forecast Data Collection
{'='*60}
""")

dbutils.notebook.exit(f"Phase 2 completed successfully in {duration:.2f} minutes")
