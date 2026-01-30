# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Performance Monitoring
# MAGIC Monitor the health and performance of all closure agents

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Closure Status

# COMMAND ----------

current_period = get_current_closure_period()

print(f"📅 Monitoring Period: {current_period}\n")

current_status = spark.sql(f"""
    SELECT 
        closure_period,
        current_phase,
        phase_status,
        started_at,
        expected_completion,
        actual_completion,
        CASE 
            WHEN actual_completion IS NOT NULL THEN 'COMPLETED'
            WHEN expected_completion < CURRENT_TIMESTAMP() THEN 'OVERDUE'
            ELSE 'IN_PROGRESS'
        END as status,
        notes
    FROM {TABLE_CLOSURE_STATUS}
    WHERE closure_period = '{current_period}'
    ORDER BY started_at DESC
    LIMIT 1
""")

display(current_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Execution Summary

# COMMAND ----------

agent_summary = spark.sql(f"""
    SELECT 
        agent_name,
        phase,
        COUNT(*) as total_tasks,
        SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END) as running,
        ROUND(AVG(duration_seconds) / 60, 2) as avg_duration_minutes,
        SUM(records_processed) as total_records,
        MAX(started_at) as last_run
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{current_period}'
    GROUP BY agent_name, phase
    ORDER BY phase, agent_name
""")

print("🤖 Agent Execution Summary:\n")
display(agent_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Task Timeline

# COMMAND ----------

task_timeline = spark.sql(f"""
    SELECT 
        agent_name,
        phase,
        task_name,
        status,
        started_at,
        completed_at,
        ROUND(duration_seconds / 60, 2) as duration_minutes,
        records_processed,
        error_message
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{current_period}'
    ORDER BY started_at DESC
""")

print("📋 Task Execution Timeline:\n")
display(task_timeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Performance Metrics

# COMMAND ----------

# Calculate performance metrics
perf_metrics = spark.sql(f"""
    SELECT 
        agent_name,
        COUNT(*) as executions,
        SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as successful,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
        ROUND(
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) as success_rate,
        ROUND(AVG(duration_seconds) / 60, 2) as avg_duration_min,
        ROUND(MIN(duration_seconds) / 60, 2) as min_duration_min,
        ROUND(MAX(duration_seconds) / 60, 2) as max_duration_min,
        SUM(records_processed) as total_records
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{current_period}'
    GROUP BY agent_name
    ORDER BY agent_name
""")

print("📊 Agent Performance Metrics:\n")
display(perf_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failed Tasks

# COMMAND ----------

failed_tasks = spark.sql(f"""
    SELECT 
        agent_name,
        phase,
        task_name,
        started_at,
        error_message
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{current_period}'
      AND status = 'FAILED'
    ORDER BY started_at DESC
""")

failed_count = failed_tasks.count()

if failed_count > 0:
    print(f"❌ Found {failed_count} failed tasks:\n")
    display(failed_tasks)
else:
    print("✅ No failed tasks!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Dashboard

# COMMAND ----------

quality_dashboard = spark.sql(f"""
    SELECT 
        phase,
        table_name,
        check_name,
        check_result,
        records_checked,
        records_failed,
        ROUND(failure_rate * 100, 2) as failure_rate_pct,
        checked_at
    FROM {TABLE_DATA_QUALITY}
    WHERE closure_period = '{current_period}'
    ORDER BY phase, checked_at DESC
""")

print("📊 Data Quality Dashboard:\n")
display(quality_dashboard)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Issues

# COMMAND ----------

quality_issues = spark.sql(f"""
    SELECT 
        phase,
        table_name,
        check_name,
        check_result,
        records_failed,
        ROUND(failure_rate * 100, 2) as failure_rate_pct,
        details
    FROM {TABLE_DATA_QUALITY}
    WHERE closure_period = '{current_period}'
      AND check_result IN ('WARNING', 'FAILED')
    ORDER BY phase, failure_rate DESC
""")

issues_count = quality_issues.count()

if issues_count > 0:
    print(f"⚠️  Found {issues_count} quality issues:\n")
    display(quality_issues)
else:
    print("✅ No quality issues!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase Progress

# COMMAND ----------

phase_progress = spark.sql(f"""
    WITH phase_tasks AS (
        SELECT 
            phase,
            COUNT(*) as total_tasks,
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_tasks
        FROM {TABLE_AGENT_LOGS}
        WHERE closure_period = '{current_period}'
        GROUP BY phase
    )
    SELECT 
        phase,
        CASE phase
            WHEN 1 THEN 'Data Gathering - Exchange Rates'
            WHEN 2 THEN 'Adjustments'
            WHEN 3 THEN 'Data Gathering - Segmented & Forecast'
            WHEN 4 THEN 'Review'
            WHEN 5 THEN 'Reporting & Sign-off'
        END as phase_name,
        total_tasks,
        completed_tasks,
        ROUND(completed_tasks * 100.0 / total_tasks, 2) as completion_pct
    FROM phase_tasks
    ORDER BY phase
""")

print("📈 Phase Progress:\n")
display(phase_progress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notifications

# COMMAND ----------

notifications = spark.sql(f"""
    SELECT 
        phase,
        severity,
        message,
        recipient,
        sent_at,
        acknowledged_at,
        CASE 
            WHEN acknowledged_at IS NOT NULL THEN 'Acknowledged'
            ELSE 'Pending'
        END as status
    FROM {FULL_SCHEMA_AUDIT}.notifications
    WHERE closure_period = '{current_period}'
    ORDER BY sent_at DESC
""")

print("📧 Notifications:\n")
display(notifications)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Summary

# COMMAND ----------

# Get overall stats
overall_stats = spark.sql(f"""
    SELECT 
        COUNT(DISTINCT agent_name) as total_agents,
        COUNT(*) as total_tasks,
        SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed_tasks,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_tasks,
        ROUND(SUM(duration_seconds) / 3600, 2) as total_hours,
        SUM(records_processed) as total_records
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{current_period}'
""").collect()[0]

print(f"""
{'='*60}
📊 OVERALL PERFORMANCE SUMMARY
{'='*60}

Closure Period: {current_period}

Agent Statistics:
   • Total Agents: {overall_stats['total_agents']}
   • Total Tasks: {overall_stats['total_tasks']}
   • Completed: {overall_stats['completed_tasks']}
   • Failed: {overall_stats['failed_tasks']}
   • Success Rate: {(overall_stats['completed_tasks'] / overall_stats['total_tasks'] * 100):.2f}%

Performance:
   • Total Execution Time: {overall_stats['total_hours']:.2f} hours
   • Records Processed: {overall_stats['total_records']:,}

Status: {'✅ Healthy' if overall_stats['failed_tasks'] == 0 else '⚠️  Issues Detected'}
{'='*60}
""")
