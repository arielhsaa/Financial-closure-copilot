# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Closure Configuration
# MAGIC Central configuration for the financial closure automation system

# COMMAND ----------

import json
from datetime import datetime, timedelta
from typing import Dict, List

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database Configuration

# COMMAND ----------

# Unity Catalog Configuration
CATALOG_NAME = "financial_closure"
SCHEMA_RAW = "raw_data"
SCHEMA_PROCESSED = "processed_data"
SCHEMA_REPORTING = "reporting"
SCHEMA_AUDIT = "audit"

# Full schema paths
FULL_SCHEMA_RAW = f"{CATALOG_NAME}.{SCHEMA_RAW}"
FULL_SCHEMA_PROCESSED = f"{CATALOG_NAME}.{SCHEMA_PROCESSED}"
FULL_SCHEMA_REPORTING = f"{CATALOG_NAME}.{SCHEMA_REPORTING}"
FULL_SCHEMA_AUDIT = f"{CATALOG_NAME}.{SCHEMA_AUDIT}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Names

# COMMAND ----------

# Raw data tables
TABLE_EXCHANGE_RATES_RAW = f"{FULL_SCHEMA_RAW}.exchange_rates_raw"
TABLE_BU_PRELIMINARY_RAW = f"{FULL_SCHEMA_RAW}.bu_preliminary_close_raw"
TABLE_ACCOUNTING_CUTS_RAW = f"{FULL_SCHEMA_RAW}.accounting_cuts_raw"
TABLE_SEGMENTED_FILES_RAW = f"{FULL_SCHEMA_RAW}.segmented_files_raw"
TABLE_FORECAST_FILES_RAW = f"{FULL_SCHEMA_RAW}.forecast_files_raw"
TABLE_FORECAST_FX_RAW = f"{FULL_SCHEMA_RAW}.forecast_fx_raw"

# Processed tables
TABLE_EXCHANGE_RATES = f"{FULL_SCHEMA_PROCESSED}.exchange_rates"
TABLE_BU_PRELIMINARY = f"{FULL_SCHEMA_PROCESSED}.bu_preliminary_close"
TABLE_ACCOUNTING_CUTS = f"{FULL_SCHEMA_PROCESSED}.accounting_cuts"
TABLE_SEGMENTED_FILES = f"{FULL_SCHEMA_PROCESSED}.segmented_files"
TABLE_FORECAST_FILES = f"{FULL_SCHEMA_PROCESSED}.forecast_files"

# Reporting tables
TABLE_PRELIMINARY_RESULTS = f"{FULL_SCHEMA_REPORTING}.preliminary_results"
TABLE_FINAL_RESULTS = f"{FULL_SCHEMA_REPORTING}.final_results"
TABLE_CLOSURE_STATUS = f"{FULL_SCHEMA_REPORTING}.closure_status"
TABLE_KPI_METRICS = f"{FULL_SCHEMA_REPORTING}.kpi_metrics"

# Audit tables
TABLE_AGENT_LOGS = f"{FULL_SCHEMA_AUDIT}.agent_execution_logs"
TABLE_DATA_QUALITY = f"{FULL_SCHEMA_AUDIT}.data_quality_checks"
TABLE_APPROVALS = f"{FULL_SCHEMA_AUDIT}.approvals"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Units Configuration

# COMMAND ----------

BUSINESS_UNITS = {
    "NA": {
        "name": "North America",
        "region": "Americas",
        "currency": "USD",
        "contact": "na.finance@company.com",
        "reporting_deadline_days": 2
    },
    "EMEA": {
        "name": "Europe, Middle East, Africa",
        "region": "EMEA",
        "currency": "EUR",
        "contact": "emea.finance@company.com",
        "reporting_deadline_days": 2
    },
    "APAC": {
        "name": "Asia Pacific",
        "region": "APAC",
        "currency": "SGD",
        "contact": "apac.finance@company.com",
        "reporting_deadline_days": 3
    },
    "LATAM": {
        "name": "Latin America",
        "region": "Americas",
        "currency": "BRL",
        "contact": "latam.finance@company.com",
        "reporting_deadline_days": 2
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Currencies and Exchange Rates

# COMMAND ----------

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CNY", "SGD", "BRL", "MXN", "CAD", "AUD"]
BASE_CURRENCY = "USD"

# Exchange rate validation thresholds (percentage)
FX_VARIANCE_THRESHOLD = 5.0  # Alert if rate changes more than 5%

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closure Phases Configuration

# COMMAND ----------

CLOSURE_PHASES = {
    1: {
        "name": "Data Gathering - Exchange Rates",
        "description": "Receive and process exchange rates and BU preliminary files",
        "tasks": [
            "receive_exchange_rates",
            "process_exchange_rates",
            "receive_bu_preliminary",
            "validate_preliminary_data"
        ],
        "sla_hours": 24
    },
    2: {
        "name": "Adjustments",
        "description": "Process preliminary close and accounting cuts",
        "tasks": [
            "process_bu_preliminary",
            "publish_preliminary_results",
            "conduct_preliminary_review",
            "receive_accounting_cut_1",
            "receive_accounting_cut_2"
        ],
        "sla_hours": 48
    },
    3: {
        "name": "Data Gathering - Segmented & Forecast",
        "description": "Receive segmented and forecast files",
        "tasks": [
            "receive_segmented_files",
            "receive_forecast_files",
            "receive_forecast_fx"
        ],
        "sla_hours": 24
    },
    4: {
        "name": "Review",
        "description": "Review meetings and validations",
        "tasks": [
            "segmented_close_review",
            "forecast_review",
            "validate_all_data"
        ],
        "sla_hours": 24
    },
    5: {
        "name": "Reporting & Sign-off",
        "description": "Publish final results and close period",
        "tasks": [
            "publish_final_results",
            "get_signoffs",
            "archive_closure"
        ],
        "sla_hours": 12
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Configuration

# COMMAND ----------

AGENT_CONFIG = {
    "orchestrator": {
        "enabled": True,
        "check_interval_minutes": 5,
        "max_retries": 3,
        "notification_email": "finance.ops@company.com"
    },
    "phase1_agent": {
        "enabled": True,
        "auto_validate": True,
        "alert_on_missing_data": True
    },
    "phase2_agent": {
        "enabled": True,
        "auto_publish": False,  # Requires manual approval
        "reconciliation_threshold": 0.01  # 1% variance threshold
    },
    "phase3_agent": {
        "enabled": True,
        "auto_validate": True,
        "remind_after_hours": 12
    },
    "phase4_agent": {
        "enabled": True,
        "auto_schedule_reviews": True,
        "require_all_signoffs": True
    },
    "phase5_agent": {
        "enabled": True,
        "auto_archive": True,
        "generate_audit_report": True
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Rules

# COMMAND ----------

DATA_QUALITY_RULES = {
    "exchange_rates": [
        {"rule": "no_nulls", "columns": ["currency", "rate", "date"]},
        {"rule": "positive_values", "columns": ["rate"]},
        {"rule": "variance_check", "threshold": FX_VARIANCE_THRESHOLD}
    ],
    "bu_preliminary": [
        {"rule": "no_nulls", "columns": ["bu_code", "account", "amount", "period"]},
        {"rule": "valid_bu", "allowed_values": list(BUSINESS_UNITS.keys())},
        {"rule": "balance_check", "debit_credit_match": True}
    ],
    "accounting_cuts": [
        {"rule": "no_nulls", "columns": ["cut_number", "account", "amount"]},
        {"rule": "valid_cut_number", "allowed_values": [1, 2]},
        {"rule": "reconciliation", "tolerance": 0.01}
    ],
    "segmented_files": [
        {"rule": "no_nulls", "columns": ["bu_code", "segment", "amount"]},
        {"rule": "segment_completeness", "required_segments": ["Product", "Geography", "Customer"]}
    ],
    "forecast_files": [
        {"rule": "no_nulls", "columns": ["bu_code", "forecast_month", "amount"]},
        {"rule": "future_dates_only", "column": "forecast_month"}
    ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Configuration

# COMMAND ----------

WORKFLOW_CONFIG = {
    "cluster_config": {
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
        "spark_version": "14.3.x-scala2.12",
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        }
    },
    "email_notifications": {
        "on_start": True,
        "on_success": True,
        "on_failure": True,
        "recipients": ["finance.ops@company.com"]
    },
    "schedule": {
        "enabled": True,
        "cron_expression": "0 0 1 * *",  # 1st of every month at midnight
        "timezone": "UTC"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_current_closure_period():
    """Get the current closure period (YYYY-MM format)"""
    today = datetime.now()
    # Closure is for previous month
    if today.day < 10:
        closure_date = today - timedelta(days=today.day + 5)
    else:
        closure_date = today
    return closure_date.strftime("%Y-%m")

def get_closure_deadline(phase: int):
    """Calculate deadline for a closure phase"""
    sla_hours = CLOSURE_PHASES[phase]["sla_hours"]
    return datetime.now() + timedelta(hours=sla_hours)

def validate_business_unit(bu_code: str) -> bool:
    """Validate if business unit code exists"""
    return bu_code in BUSINESS_UNITS

def get_bu_currency(bu_code: str) -> str:
    """Get currency for a business unit"""
    return BUSINESS_UNITS.get(bu_code, {}).get("currency", BASE_CURRENCY)

def create_checkpoint_location(table_name: str) -> str:
    """Create checkpoint location for streaming tables"""
    return f"/mnt/checkpoints/{table_name}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Configuration

# COMMAND ----------

# Create configuration dictionary for export
CONFIG = {
    "catalog": CATALOG_NAME,
    "schemas": {
        "raw": SCHEMA_RAW,
        "processed": SCHEMA_PROCESSED,
        "reporting": SCHEMA_REPORTING,
        "audit": SCHEMA_AUDIT
    },
    "business_units": BUSINESS_UNITS,
    "currencies": CURRENCIES,
    "base_currency": BASE_CURRENCY,
    "closure_phases": CLOSURE_PHASES,
    "agent_config": AGENT_CONFIG,
    "data_quality_rules": DATA_QUALITY_RULES,
    "workflow_config": WORKFLOW_CONFIG
}

# Make available to other notebooks
dbutils.jobs.taskValues.set(key="config", value=json.dumps(CONFIG))

print("✅ Configuration loaded successfully")
print(f"📅 Current closure period: {get_current_closure_period()}")
print(f"🏢 Business units configured: {len(BUSINESS_UNITS)}")
print(f"💱 Currencies supported: {len(CURRENCIES)}")
