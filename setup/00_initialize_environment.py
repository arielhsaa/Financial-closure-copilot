# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Financial Closure Environment
# MAGIC This notebook sets up the Unity Catalog, schemas, and base configuration for the Financial Closure Copilot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

print("🏗️  Creating Unity Catalog and Schemas...")

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")

print(f"✅ Catalog created: {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas

# COMMAND ----------

# Create schemas
schemas = [
    (SCHEMA_RAW, "Raw landing zone for incoming files"),
    (SCHEMA_PROCESSED, "Processed and validated data"),
    (SCHEMA_REPORTING, "Reporting and analytics tables"),
    (SCHEMA_AUDIT, "Audit trails and logs")
]

for schema_name, comment in schemas:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name}
        COMMENT '{comment}'
    """)
    print(f"✅ Schema created: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Catalog Permissions

# COMMAND ----------

# Grant permissions (adjust based on your groups)
try:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `account users`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FULL_SCHEMA_RAW} TO `account users`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FULL_SCHEMA_PROCESSED} TO `account users`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FULL_SCHEMA_REPORTING} TO `account users`")
    spark.sql(f"GRANT SELECT ON SCHEMA {FULL_SCHEMA_REPORTING} TO `account users`")
    print("✅ Permissions granted")
except Exception as e:
    print(f"⚠️  Permission grant skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for File Storage

# COMMAND ----------

# Create volumes for landing files
volumes = ["exchange_rates", "preliminary_close", "accounting_cuts", "segmented_files", "forecast_files"]

for volume in volumes:
    try:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {FULL_SCHEMA_RAW}.{volume}
        """)
        print(f"✅ Volume created: {volume}")
    except Exception as e:
        print(f"⚠️  Volume creation for {volume}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Agent State Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.agent_state (
        agent_id STRING,
        agent_name STRING,
        phase INT,
        status STRING,
        current_task STRING,
        closure_period STRING,
        started_at TIMESTAMP,
        updated_at TIMESTAMP,
        metadata MAP<STRING, STRING>
    )
    USING DELTA
    COMMENT 'Tracks the state of all closure agents'
""")

print("✅ Agent state table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Closure Tracking Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CLOSURE_STATUS} (
        closure_id STRING,
        closure_period STRING,
        current_phase INT,
        phase_status STRING,
        started_at TIMESTAMP,
        expected_completion TIMESTAMP,
        actual_completion TIMESTAMP,
        created_by STRING,
        updated_at TIMESTAMP,
        notes STRING
    )
    USING DELTA
    COMMENT 'Tracks overall closure cycle progress'
""")

print("✅ Closure status table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Notification Queue

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_SCHEMA_AUDIT}.notifications (
        notification_id STRING,
        closure_period STRING,
        phase INT,
        severity STRING,
        message STRING,
        recipient STRING,
        sent_at TIMESTAMP,
        acknowledged_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Queue for agent notifications'
""")

print("✅ Notification queue created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Approval Workflow Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_APPROVALS} (
        approval_id STRING,
        closure_period STRING,
        phase INT,
        approval_type STRING,
        required_approver STRING,
        status STRING,
        requested_at TIMESTAMP,
        approved_at TIMESTAMP,
        comments STRING
    )
    USING DELTA
    COMMENT 'Tracks approvals and sign-offs'
""")

print("✅ Approval table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# Verify catalog and schemas
print("\n📋 Environment Summary:")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schemas: {len(schemas)}")

# List all tables
all_tables = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_AUDIT}").collect()
print(f"   Audit tables created: {len(all_tables)}")

# Check volumes
try:
    all_volumes = spark.sql(f"SHOW VOLUMES IN {FULL_SCHEMA_RAW}").collect()
    print(f"   Volumes created: {len(all_volumes)}")
except:
    print(f"   Volumes: Not available in this workspace")

print("\n✅ Environment initialization complete!")
print("\n📝 Next steps:")
print("   1. Run 01_create_tables to create all data tables")
print("   2. Run 02_setup_agents to configure agents")
print("   3. Run data simulation notebooks to generate test data")
