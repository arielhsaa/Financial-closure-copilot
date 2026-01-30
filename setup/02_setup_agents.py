# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Financial Closure Agents
# MAGIC Initializes and registers all closure automation agents

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Registry

# COMMAND ----------

# Create agent registry table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_registry (
        agent_id STRING,
        agent_name STRING,
        agent_type STRING,
        phase INT,
        description STRING,
        is_enabled BOOLEAN,
        config MAP<STRING, STRING>,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Registry of all closure automation agents'
""")

print("✅ Agent registry created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Agents

# COMMAND ----------

agents = [
    {
        "agent_id": "agent-orchestrator-001",
        "agent_name": "Closure Orchestrator",
        "agent_type": "orchestrator",
        "phase": 0,
        "description": "Master orchestrator coordinating all closure phases and agents",
        "is_enabled": True,
        "config": {
            "check_interval_minutes": "5",
            "max_retries": "3",
            "notification_email": "finance.ops@company.com"
        }
    },
    {
        "agent_id": "agent-phase1-001",
        "agent_name": "Exchange Rate & Preliminary Data Agent",
        "agent_type": "data_collector",
        "phase": 1,
        "description": "Collects and validates exchange rates and preliminary close files",
        "is_enabled": True,
        "config": {
            "auto_validate": "true",
            "alert_on_missing_data": "true",
            "fx_variance_threshold": "5.0"
        }
    },
    {
        "agent_id": "agent-phase2-001",
        "agent_name": "Adjustments & Review Agent",
        "agent_type": "processor",
        "phase": 2,
        "description": "Processes preliminary close, manages accounting cuts, and coordinates reviews",
        "is_enabled": True,
        "config": {
            "auto_publish": "false",
            "reconciliation_threshold": "0.01",
            "require_approval": "true"
        }
    },
    {
        "agent_id": "agent-phase3-001",
        "agent_name": "Segmented & Forecast Data Agent",
        "agent_type": "data_collector",
        "phase": 3,
        "description": "Collects segmented files and forecast data from all business units",
        "is_enabled": True,
        "config": {
            "auto_validate": "true",
            "remind_after_hours": "12",
            "segment_completeness_check": "true"
        }
    },
    {
        "agent_id": "agent-phase4-001",
        "agent_name": "Review Coordinator Agent",
        "agent_type": "coordinator",
        "phase": 4,
        "description": "Coordinates review meetings and manages sign-off process",
        "is_enabled": True,
        "config": {
            "auto_schedule_reviews": "true",
            "require_all_signoffs": "true",
            "escalation_hours": "24"
        }
    },
    {
        "agent_id": "agent-phase5-001",
        "agent_name": "Final Publishing Agent",
        "agent_type": "publisher",
        "phase": 5,
        "description": "Publishes final results and archives closure cycle",
        "is_enabled": True,
        "config": {
            "auto_archive": "true",
            "generate_audit_report": "true",
            "send_completion_notification": "true"
        }
    },
    {
        "agent_id": "agent-monitor-001",
        "agent_name": "Data Quality Monitor",
        "agent_type": "monitor",
        "phase": 0,
        "description": "Continuously monitors data quality across all phases",
        "is_enabled": True,
        "config": {
            "check_frequency_minutes": "15",
            "quality_threshold": "95.0",
            "alert_on_threshold_breach": "true"
        }
    },
    {
        "agent_id": "agent-reconciliation-001",
        "agent_name": "Reconciliation Agent",
        "agent_type": "validator",
        "phase": 0,
        "description": "Performs automated reconciliations across all data sources",
        "is_enabled": True,
        "config": {
            "tolerance_amount": "0.01",
            "auto_resolve": "false",
            "escalate_differences": "true"
        }
    }
]

# Insert agents into registry
for agent in agents:
    spark.sql(f"""
        INSERT INTO {FULL_SCHEMA_AUDIT}.agent_registry
        VALUES (
            '{agent['agent_id']}',
            '{agent['agent_name']}',
            '{agent['agent_type']}',
            {agent['phase']},
            '{agent['description']}',
            {agent['is_enabled']},
            map{tuple((k, v) for k, v in agent['config'].items())},
            current_timestamp(),
            current_timestamp()
        )
    """)

print(f"✅ Registered {len(agents)} agents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Agent Task Queue

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_task_queue (
        task_id STRING,
        agent_id STRING,
        closure_period STRING,
        phase INT,
        task_type STRING,
        task_description STRING,
        priority INT,
        status STRING,
        created_at TIMESTAMP,
        scheduled_for TIMESTAMP,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        result STRING,
        error_message STRING,
        retry_count INT,
        metadata MAP<STRING, STRING>
    )
    USING DELTA
    COMMENT 'Task queue for agent execution'
""")

print("✅ Agent task queue created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Agent Communication Log

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_communications (
        communication_id STRING,
        closure_period STRING,
        from_agent_id STRING,
        to_agent_id STRING,
        message_type STRING,
        message_content STRING,
        sent_at TIMESTAMP,
        received_at TIMESTAMP,
        acknowledged_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Inter-agent communication log'
""")

print("✅ Agent communication log created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Agent Performance Metrics

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_performance (
        metric_id STRING,
        agent_id STRING,
        closure_period STRING,
        metric_date DATE,
        tasks_completed INT,
        tasks_failed INT,
        average_duration_seconds DOUBLE,
        total_records_processed BIGINT,
        error_rate DOUBLE,
        sla_compliance_rate DOUBLE,
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Performance metrics for each agent'
""")

print("✅ Agent performance table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Agent Capabilities

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_capabilities (
        capability_id STRING,
        agent_id STRING,
        capability_name STRING,
        capability_type STRING,
        is_enabled BOOLEAN,
        parameters MAP<STRING, STRING>
    )
    USING DELTA
    COMMENT 'Defines capabilities of each agent'
""")

capabilities = [
    ("agent-phase1-001", "file_ingestion", "Upload and parse exchange rate files"),
    ("agent-phase1-001", "data_validation", "Validate exchange rate data quality"),
    ("agent-phase1-001", "variance_detection", "Detect unusual rate variances"),
    ("agent-phase1-001", "notification", "Send alerts for missing data"),
    ("agent-phase2-001", "data_processing", "Process preliminary close files"),
    ("agent-phase2-001", "reconciliation", "Reconcile preliminary amounts"),
    ("agent-phase2-001", "publishing", "Publish preliminary results"),
    ("agent-phase2-001", "meeting_coordination", "Schedule review meetings"),
    ("agent-phase3-001", "file_ingestion", "Collect segmented and forecast files"),
    ("agent-phase3-001", "data_validation", "Validate segmented data completeness"),
    ("agent-phase3-001", "reminder", "Send reminders for pending submissions"),
    ("agent-phase4-001", "meeting_coordination", "Coordinate review meetings"),
    ("agent-phase4-001", "approval_tracking", "Track sign-offs and approvals"),
    ("agent-phase4-001", "escalation", "Escalate blocked items"),
    ("agent-phase5-001", "final_calculations", "Calculate final close results"),
    ("agent-phase5-001", "publishing", "Publish final reports"),
    ("agent-phase5-001", "archiving", "Archive closure cycle data"),
    ("agent-monitor-001", "quality_monitoring", "Monitor data quality metrics"),
    ("agent-monitor-001", "anomaly_detection", "Detect data anomalies"),
    ("agent-reconciliation-001", "auto_reconciliation", "Perform automated reconciliations")
]

for agent_id, capability_name, description in capabilities:
    capability_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO {FULL_SCHEMA_AUDIT}.agent_capabilities
        VALUES (
            '{capability_id}',
            '{agent_id}',
            '{capability_name}',
            '{description}',
            true,
            map()
        )
    """)

print(f"✅ Registered {len(capabilities)} agent capabilities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Agent Decision Log

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_decisions (
        decision_id STRING,
        agent_id STRING,
        closure_period STRING,
        phase INT,
        decision_type STRING,
        decision_description STRING,
        decision_outcome STRING,
        confidence_score DOUBLE,
        factors ARRAY<STRING>,
        made_at TIMESTAMP,
        overridden BOOLEAN,
        overridden_by STRING,
        overridden_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Logs all autonomous agent decisions'
""")

print("✅ Agent decision log created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Agent Registry

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        agent_name,
        agent_type,
        CASE 
            WHEN phase = 0 THEN 'All Phases'
            ELSE CONCAT('Phase ', phase)
        END as phase,
        description,
        is_enabled
    FROM {FULL_SCHEMA_AUDIT}.agent_registry
    ORDER BY phase, agent_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

agent_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {FULL_SCHEMA_AUDIT}.agent_registry").collect()[0]['cnt']
capability_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {FULL_SCHEMA_AUDIT}.agent_capabilities").collect()[0]['cnt']

print(f"""
✅ Agent Setup Complete!

📊 Summary:
   • Agents registered: {agent_count}
   • Capabilities defined: {capability_count}
   • Support tables created: 6

🤖 Agents by Phase:
   • Phase 0 (Continuous): Orchestrator, Monitor, Reconciliation
   • Phase 1: Exchange Rate & Preliminary Data Agent
   • Phase 2: Adjustments & Review Agent
   • Phase 3: Segmented & Forecast Data Agent
   • Phase 4: Review Coordinator Agent
   • Phase 5: Final Publishing Agent

📝 Next steps:
   1. Run data simulation notebooks to generate test data
   2. Execute agent notebooks to start automation
   3. Monitor agent performance in dashboards
""")
