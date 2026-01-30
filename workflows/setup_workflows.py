# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Databricks Workflows for Financial Closure
# MAGIC Creates automated workflows for the financial closure process

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Definition

# COMMAND ----------

# Define the closure workflow
closure_workflow = {
    "name": f"Financial_Closure_Automation_{get_current_closure_period()}",
    "tags": {
        "project": "financial-closure-copilot",
        "environment": "production"
    },
    "email_notifications": {
        "on_start": ["finance.ops@company.com"],
        "on_success": ["finance.ops@company.com"],
        "on_failure": ["finance.ops@company.com", "tech.ops@company.com"]
    },
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "initialize_environment",
            "description": "Initialize Unity Catalog and schemas",
            "notebook_task": {
                "notebook_path": "./setup/00_initialize_environment",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800,
            "max_retries": 2,
            "min_retry_interval_millis": 60000
        },
        {
            "task_key": "create_tables",
            "description": "Create all Delta tables",
            "depends_on": [{"task_key": "initialize_environment"}],
            "notebook_task": {
                "notebook_path": "./setup/01_create_tables",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800,
            "max_retries": 2
        },
        {
            "task_key": "setup_agents",
            "description": "Register and configure agents",
            "depends_on": [{"task_key": "create_tables"}],
            "notebook_task": {
                "notebook_path": "./setup/02_setup_agents",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800,
            "max_retries": 2
        },
        {
            "task_key": "simulate_data",
            "description": "Generate simulated data for testing",
            "depends_on": [{"task_key": "setup_agents"}],
            "notebook_task": {
                "notebook_path": "./data_simulation/01_simulate_exchange_rates",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "simulate_preliminary",
            "description": "Generate BU preliminary data",
            "depends_on": [{"task_key": "simulate_data"}],
            "notebook_task": {
                "notebook_path": "./data_simulation/02_simulate_bu_preliminary",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "simulate_cuts",
            "description": "Generate accounting cuts",
            "depends_on": [{"task_key": "simulate_preliminary"}],
            "notebook_task": {
                "notebook_path": "./data_simulation/03_simulate_accounting_cuts",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "simulate_segments",
            "description": "Generate segmented files",
            "depends_on": [{"task_key": "simulate_cuts"}],
            "notebook_task": {
                "notebook_path": "./data_simulation/04_simulate_segmented_files",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "simulate_forecasts",
            "description": "Generate forecast files",
            "depends_on": [{"task_key": "simulate_segments"}],
            "notebook_task": {
                "notebook_path": "./data_simulation/05_simulate_forecast_files",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "run_closure_orchestrator",
            "description": "Execute full closure cycle with agents",
            "depends_on": [{"task_key": "simulate_forecasts"}],
            "notebook_task": {
                "notebook_path": "./agents/agent_orchestrator",
                "source": "WORKSPACE",
                "base_parameters": {
                    "mode": "full",
                    "closure_period": get_current_closure_period()
                }
            },
            "timeout_seconds": 7200,  # 2 hours
            "max_retries": 1
        }
    ],
    "format": "MULTI_TASK"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Workflow Configuration

# COMMAND ----------

print("📋 Closure Workflow Configuration:\n")
print(json.dumps(closure_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instructions to Create Workflow

# COMMAND ----------

print("""
To create this workflow in Databricks:

METHOD 1: Using Databricks CLI
===============================
1. Install Databricks CLI:
   pip install databricks-cli

2. Configure authentication:
   databricks configure --token

3. Save the workflow JSON above to a file: closure_workflow.json

4. Create the workflow:
   databricks jobs create --json-file closure_workflow.json

METHOD 2: Using Databricks UI
===============================
1. Go to Workflows in your Databricks workspace
2. Click "Create Job"
3. Name it: "Financial_Closure_Automation"
4. Add tasks as defined above in sequence
5. Configure cluster settings (recommend Serverless)
6. Set email notifications
7. Configure schedule (optional)
8. Click "Create"

METHOD 3: Using Databricks REST API
====================================
Use the Jobs API to create the workflow programmatically.

SCHEDULE CONFIGURATION (Optional)
==================================
To run automatically on the 1st of each month:
- Cron expression: 0 0 1 * *
- Timezone: UTC

IMPORTANT NOTES:
================
- Ensure all notebook paths are correct for your workspace
- Configure appropriate cluster settings based on data volume
- Set up proper Unity Catalog permissions
- Configure email notifications for your team
- Test with simulated data before production use
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Monthly Scheduled Workflow

# COMMAND ----------

monthly_workflow = {
    "name": "Financial_Closure_Monthly",
    "schedule": {
        "quartz_cron_expression": "0 0 1 * * ?",  # 1st of every month at midnight
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"
    },
    "tags": {
        "project": "financial-closure-copilot",
        "environment": "production",
        "schedule": "monthly"
    },
    "email_notifications": {
        "on_start": ["finance.ops@company.com"],
        "on_success": ["finance.ops@company.com"],
        "on_failure": ["finance.ops@company.com", "tech.ops@company.com"]
    },
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "run_closure_orchestrator",
            "description": "Execute monthly financial closure",
            "notebook_task": {
                "notebook_path": "./agents/agent_orchestrator",
                "source": "WORKSPACE",
                "base_parameters": {
                    "mode": "full"
                }
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2,
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                }
            },
            "timeout_seconds": 14400,  # 4 hours
            "max_retries": 1,
            "min_retry_interval_millis": 60000
        }
    ],
    "format": "MULTI_TASK"
}

print("\n📅 Monthly Scheduled Workflow:\n")
print(json.dumps(monthly_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Start Workflow (Without Simulation)

# COMMAND ----------

quickstart_workflow = {
    "name": "Financial_Closure_QuickStart",
    "description": "Quick start for production use with real data",
    "tasks": [
        {
            "task_key": "verify_setup",
            "description": "Verify environment is ready",
            "notebook_task": {
                "notebook_path": "./setup/00_initialize_environment",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "run_closure",
            "description": "Run closure with real data",
            "depends_on": [{"task_key": "verify_setup"}],
            "notebook_task": {
                "notebook_path": "./agents/agent_orchestrator",
                "source": "WORKSPACE",
                "base_parameters": {
                    "mode": "full"
                }
            },
            "timeout_seconds": 7200,
            "max_retries": 1
        }
    ]
}

print("\n🚀 Quick Start Workflow (Production):\n")
print(json.dumps(quickstart_workflow, indent=2))

# COMMAND ----------

print("""
✅ Workflow Configuration Complete!

Next Steps:
===========
1. Choose the appropriate workflow type:
   - Full workflow with simulation (for testing)
   - Monthly scheduled workflow (for production)
   - Quick start workflow (for production with existing data)

2. Create the workflow using one of the methods above

3. Test the workflow with simulated data first

4. Monitor execution in Databricks Workflows UI

5. Review results in Genie dashboards

For questions or issues, contact the Data Engineering team.
""")
