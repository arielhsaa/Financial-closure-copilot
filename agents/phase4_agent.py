# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 Agent: Review & Validation
# MAGIC Automated agent for Phase 4 of financial closure

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# Get parameters
dbutils.widgets.text("closure_period", get_current_closure_period(), "Closure Period")
closure_period = dbutils.widgets.get("closure_period")

agent_id = "agent-phase4-001"
agent_name = "Review Coordinator Agent"
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
        "phase": 4,
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
# MAGIC ## Task 1: Schedule Segmented Close Review

# COMMAND ----------

print("📅 Task 1: Scheduling Segmented Close Review Meeting...")

# Create meeting approval record
meeting_record = spark.createDataFrame([{
    "approval_id": str(uuid.uuid4()),
    "closure_period": closure_period,
    "phase": 4,
    "approval_type": "SEGMENTED_CLOSE_REVIEW",
    "required_approver": "FP&A + Tech Teams",
    "status": "SCHEDULED",
    "requested_at": datetime.now(),
    "approved_at": None,
    "comments": "Segmented close review - auto-scheduled by agent"
}])

meeting_record.write.format("delta").mode("append").saveAsTable(TABLE_APPROVALS)

print("✅ Segmented close review meeting scheduled")
log_task("Schedule Segmented Review", "COMPLETED", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Schedule Forecast Review

# COMMAND ----------

print("📅 Task 2: Scheduling Forecast Review Meeting...")

forecast_meeting = spark.createDataFrame([{
    "approval_id": str(uuid.uuid4()),
    "closure_period": closure_period,
    "phase": 4,
    "approval_type": "FORECAST_REVIEW",
    "required_approver": "FP&A + Tech Teams",
    "status": "SCHEDULED",
    "requested_at": datetime.now(),
    "approved_at": None,
    "comments": "Forecast review - auto-scheduled by agent"
}])

forecast_meeting.write.format("delta").mode("append").saveAsTable(TABLE_APPROVALS)

print("✅ Forecast review meeting scheduled")
log_task("Schedule Forecast Review", "COMPLETED", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Perform Data Completeness Validation

# COMMAND ----------

print("🔍 Task 3: Validating Data Completeness...")

# Check all required data is present
validation_results = []

# Check preliminary close
prelim_check = spark.sql(f"""
    SELECT COUNT(DISTINCT bu_code) as bu_count
    FROM {TABLE_BU_PRELIMINARY}
    WHERE closure_period = '{closure_period}' AND is_validated = true
""").collect()[0]['bu_count']

validation_results.append(("Preliminary Close", prelim_check, len(BUSINESS_UNITS), 
                          prelim_check == len(BUSINESS_UNITS)))

# Check accounting cuts
cuts_check = spark.sql(f"""
    SELECT COUNT(DISTINCT cut_number) as cut_count
    FROM {TABLE_ACCOUNTING_CUTS}
    WHERE closure_period = '{closure_period}' AND is_validated = true
""").collect()[0]['cut_count']

validation_results.append(("Accounting Cuts", cuts_check, 2, cuts_check == 2))

# Check segmented files
segment_check = spark.sql(f"""
    SELECT COUNT(DISTINCT bu_code) as bu_count
    FROM {TABLE_SEGMENTED_FILES}
    WHERE closure_period = '{closure_period}' AND is_validated = true
""").collect()[0]['bu_count']

validation_results.append(("Segmented Files", segment_check, len(BUSINESS_UNITS),
                          segment_check == len(BUSINESS_UNITS)))

# Check forecast files
forecast_check = spark.sql(f"""
    SELECT COUNT(DISTINCT bu_code) as bu_count
    FROM {TABLE_FORECAST_FILES}
    WHERE closure_period = '{closure_period}' AND is_validated = true
""").collect()[0]['bu_count']

validation_results.append(("Forecast Files", forecast_check, len(BUSINESS_UNITS),
                          forecast_check == len(BUSINESS_UNITS)))

# Display results
print("\n📋 Data Completeness Validation:")
for item, actual, expected, passed in validation_results:
    status = "✅" if passed else "❌"
    print(f"   {status} {item}: {actual}/{expected}")

all_passed = all(result[3] for result in validation_results)

if all_passed:
    print("\n✅ All data completeness checks passed")
    log_task("Data Completeness Validation", "COMPLETED", len(validation_results))
else:
    print("\n⚠️  Some data completeness checks failed")
    log_task("Data Completeness Validation", "WARNING", len(validation_results),
            "Some datasets incomplete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Perform Cross-Dataset Reconciliation

# COMMAND ----------

print("🔍 Task 4: Performing Cross-Dataset Reconciliation...")

# Reconcile preliminary close with segmented data
recon_sql = f"""
SELECT 
    'Preliminary vs Segmented' as reconciliation_type,
    prelim.total_prelim,
    segment.total_segment,
    ABS(prelim.total_prelim - segment.total_segment) as variance,
    CASE 
        WHEN ABS(prelim.total_prelim - segment.total_segment) / prelim.total_prelim < 0.01 
        THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM (
    SELECT SUM(usd_amount) as total_prelim
    FROM {TABLE_BU_PRELIMINARY}
    WHERE closure_period = '{closure_period}' 
      AND is_validated = true
      AND account_code IN ('4000', '4100', '4200', '5000', '5100')
) prelim
CROSS JOIN (
    SELECT SUM(usd_amount) as total_segment
    FROM {TABLE_SEGMENTED_FILES}
    WHERE closure_period = '{closure_period}' AND is_validated = true
) segment
"""

recon_result = spark.sql(recon_sql).collect()[0]

print(f"""
📊 Reconciliation Results:
   • Preliminary Total: ${recon_result['total_prelim']:,.2f}
   • Segmented Total: ${recon_result['total_segment']:,.2f}
   • Variance: ${recon_result['variance']:,.2f}
   • Status: {recon_result['result']}
""")

if recon_result['result'] == 'PASS':
    print("✅ Reconciliation passed")
    log_task("Cross-Dataset Reconciliation", "COMPLETED", 1)
else:
    print("⚠️  Reconciliation variance exceeds threshold")
    log_task("Cross-Dataset Reconciliation", "WARNING", 1, 
            f"Variance: ${recon_result['variance']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Generate Review Summary

# COMMAND ----------

print("📊 Task 5: Generating Review Summary...")

# Summary by BU
bu_summary = spark.sql(f"""
    SELECT 
        p.bu_code,
        p.bu_name,
        COUNT(DISTINCT p.account_code) as accounts,
        SUM(p.usd_amount) as preliminary_total,
        COUNT(DISTINCT s.segment_type) as segments_present,
        COUNT(DISTINCT f.forecast_period) as forecast_periods
    FROM {TABLE_BU_PRELIMINARY} p
    LEFT JOIN {TABLE_SEGMENTED_FILES} s 
        ON p.bu_code = s.bu_code AND s.closure_period = '{closure_period}'
    LEFT JOIN {TABLE_FORECAST_FILES} f 
        ON p.bu_code = f.bu_code AND f.closure_period = '{closure_period}'
    WHERE p.closure_period = '{closure_period}'
      AND p.is_validated = true
    GROUP BY p.bu_code, p.bu_name
    ORDER BY p.bu_code
""")

print("\n📋 Business Unit Summary:")
bu_summary.show(truncate=False)

log_task("Generate Review Summary", "COMPLETED", bu_summary.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Request Sign-offs

# COMMAND ----------

print("✅ Task 6: Requesting Final Sign-offs...")

# Create sign-off requests for each BU
for bu_code, bu_info in BUSINESS_UNITS.items():
    signoff_request = spark.createDataFrame([{
        "approval_id": str(uuid.uuid4()),
        "closure_period": closure_period,
        "phase": 4,
        "approval_type": "BU_SIGNOFF",
        "required_approver": f"{bu_info['name']} CFO",
        "status": "PENDING",
        "requested_at": datetime.now(),
        "approved_at": None,
        "comments": f"Final data sign-off required from {bu_info['name']}"
    }])
    signoff_request.write.format("delta").mode("append").saveAsTable(TABLE_APPROVALS)

print(f"✅ Sign-off requests sent to {len(BUSINESS_UNITS)} business units")
log_task("Request Sign-offs", "COMPLETED", len(BUSINESS_UNITS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4 Summary

# COMMAND ----------

duration = (datetime.now() - start_time).total_seconds() / 60

print(f"""
{'='*60}
✅ PHASE 4 COMPLETE
{'='*60}

📅 Closure Period: {closure_period}
⏱️  Duration: {duration:.2f} minutes

📊 Results:
   • Review meetings scheduled: 2
   • Data completeness: {'All passed' if all_passed else 'Issues found'}
   • Reconciliation: {recon_result['result']}
   • Sign-off requests sent: {len(BUSINESS_UNITS)}

📝 Next Phase: Reporting & Final Sign-off
{'='*60}
""")

dbutils.notebook.exit(f"Phase 4 completed successfully in {duration:.2f} minutes")
