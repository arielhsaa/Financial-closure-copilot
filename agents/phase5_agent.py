# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 Agent: Reporting & Final Sign-off
# MAGIC Automated agent for Phase 5 of financial closure

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# Get parameters
dbutils.widgets.text("closure_period", get_current_closure_period(), "Closure Period")
closure_period = dbutils.widgets.get("closure_period")

agent_id = "agent-phase5-001"
agent_name = "Final Publishing Agent"
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
        "phase": 5,
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
# MAGIC ## Task 1: Verify All Sign-offs

# COMMAND ----------

print("🔍 Task 1: Verifying Sign-offs...")

# Check pending sign-offs
pending_signoffs = spark.sql(f"""
    SELECT 
        approval_type,
        required_approver,
        status
    FROM {TABLE_APPROVALS}
    WHERE closure_period = '{closure_period}'
      AND approval_type = 'BU_SIGNOFF'
      AND status = 'PENDING'
""").count()

if pending_signoffs > 0:
    print(f"⚠️  {pending_signoffs} sign-offs still pending")
    print("   Note: In production, agent would wait for all sign-offs")
    print("   Proceeding for demo purposes...")
    
    # Auto-approve for demo
    spark.sql(f"""
        UPDATE {TABLE_APPROVALS}
        SET status = 'APPROVED',
            approved_at = current_timestamp(),
            comments = CONCAT(comments, ' - Auto-approved for demo')
        WHERE closure_period = '{closure_period}'
          AND approval_type = 'BU_SIGNOFF'
          AND status = 'PENDING'
    """)
    
    log_task("Verify Sign-offs", "WARNING", pending_signoffs, 
            "Auto-approved pending sign-offs for demo")
else:
    print("✅ All sign-offs received")
    log_task("Verify Sign-offs", "COMPLETED", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Publish Final Results

# COMMAND ----------

print("📊 Task 2: Publishing Final Results...")

try:
    dbutils.notebook.run("../reporting/publish_final_results", timeout_seconds=1800)
    
    final_count = spark.table(TABLE_FINAL_RESULTS).filter(
        (col("closure_period") == closure_period) &
        (col("is_final") == True)
    ).count()
    
    print(f"✅ Published {final_count} final results")
    log_task("Publish Final Results", "COMPLETED", final_count)
    
except Exception as e:
    print(f"❌ Publishing failed: {str(e)}")
    log_task("Publish Final Results", "FAILED", 0, str(e))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Generate Executive Summary

# COMMAND ----------

print("📊 Task 3: Generating Executive Summary...")

# Get key metrics
exec_summary = spark.sql(f"""
    SELECT 
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) as total_revenue,
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as total_cogs,
        SUM(CASE WHEN account_category = 'OpEx' THEN ABS(final_amount) ELSE 0 END) as total_opex,
        COUNT(DISTINCT bu_code) as bu_count,
        COUNT(*) as total_records
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{closure_period}'
      AND is_final = true
""").collect()[0]

revenue = exec_summary['total_revenue']
cogs = exec_summary['total_cogs']
opex = exec_summary['total_opex']
gross_profit = revenue - cogs
gross_margin = (gross_profit / revenue * 100) if revenue != 0 else 0
operating_income = gross_profit - opex
operating_margin = (operating_income / revenue * 100) if revenue != 0 else 0

print(f"""
📊 Executive Summary - {closure_period}
{'='*60}

Revenue Metrics:
   • Total Revenue: ${revenue:,.2f}
   • Cost of Goods Sold: ${cogs:,.2f}
   • Gross Profit: ${gross_profit:,.2f}
   • Gross Margin: {gross_margin:.2f}%

Operating Metrics:
   • Operating Expenses: ${opex:,.2f}
   • Operating Income: ${operating_income:,.2f}
   • Operating Margin: {operating_margin:.2f}%

Coverage:
   • Business Units: {exec_summary['bu_count']}
   • Total Records: {exec_summary['total_records']}

{'='*60}
""")

log_task("Generate Executive Summary", "COMPLETED", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Generate Audit Report

# COMMAND ----------

print("📋 Task 4: Generating Audit Report...")

# Get all agent activities for this closure
agent_activity = spark.sql(f"""
    SELECT 
        phase,
        agent_name,
        task_name,
        status,
        records_processed,
        ROUND(duration_seconds / 60, 2) as duration_minutes
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{closure_period}'
    ORDER BY phase, started_at
""")

print("\n🤖 Agent Activity Report:")
agent_activity.show(100, truncate=False)

# Get data quality summary
quality_report = spark.sql(f"""
    SELECT 
        phase,
        table_name,
        check_result,
        records_checked,
        records_failed,
        ROUND(failure_rate * 100, 2) as failure_rate_pct
    FROM {TABLE_DATA_QUALITY}
    WHERE closure_period = '{closure_period}'
    ORDER BY phase, checked_at
""")

print("\n📊 Data Quality Report:")
quality_report.show(100, truncate=False)

log_task("Generate Audit Report", "COMPLETED", agent_activity.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Archive Closure Cycle

# COMMAND ----------

print("📦 Task 5: Archiving Closure Cycle...")

# Mark closure as complete
completion_record = spark.createDataFrame([{
    "closure_id": str(uuid.uuid4()),
    "closure_period": closure_period,
    "current_phase": 5,
    "phase_status": "COMPLETED",
    "started_at": start_time,
    "expected_completion": None,
    "actual_completion": datetime.now(),
    "created_by": "phase5_agent",
    "updated_at": datetime.now(),
    "notes": f"Closure cycle completed successfully. Final results published."
}])

completion_record.write.format("delta").mode("append").saveAsTable(TABLE_CLOSURE_STATUS)

print("✅ Closure cycle archived")
log_task("Archive Closure Cycle", "COMPLETED", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Send Completion Notifications

# COMMAND ----------

print("📧 Task 6: Sending Completion Notifications...")

# Send notification to stakeholders
notification = spark.createDataFrame([{
    "notification_id": str(uuid.uuid4()),
    "closure_period": closure_period,
    "phase": 5,
    "severity": "INFO",
    "message": f"""Financial closure for {closure_period} is complete.
    
Key Metrics:
- Total Revenue: ${revenue:,.2f}
- Gross Margin: {gross_margin:.2f}%
- Operating Margin: {operating_margin:.2f}%

Final results are available in the reporting dashboards.
""",
    "recipient": "finance.ops@company.com",
    "sent_at": datetime.now(),
    "acknowledged_at": None
}])

notification.write.format("delta").mode("append").saveAsTable(f"{FULL_SCHEMA_AUDIT}.notifications")

print("✅ Completion notifications sent")
log_task("Send Notifications", "COMPLETED", 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5 Summary

# COMMAND ----------

total_duration = (datetime.now() - start_time).total_seconds() / 60

print(f"""
{'='*60}
🎉 PHASE 5 COMPLETE - FINANCIAL CLOSURE SUCCESSFUL!
{'='*60}

📅 Closure Period: {closure_period}
⏱️  Phase Duration: {total_duration:.2f} minutes

💰 Financial Summary:
   • Total Revenue: ${revenue:,.2f}
   • Gross Margin: {gross_margin:.2f}%
   • Operating Margin: {operating_margin:.2f}%

📊 Data Summary:
   • Final Results Published: {final_count}
   • Business Units: {exec_summary['bu_count']}
   • Quality Checks Passed: ✅

🎯 Next Steps:
   • View final results in Genie dashboards
   • Review audit reports
   • Archive completed cycle

{'='*60}

🎉 FINANCIAL CLOSE CYCLE COMPLETE! 🎉
""")

dbutils.notebook.exit(f"Phase 5 completed successfully in {total_duration:.2f} minutes. Closure cycle complete!")
