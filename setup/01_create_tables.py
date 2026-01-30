# Databricks notebook source
# MAGIC %md
# MAGIC # Create All Financial Closure Tables
# MAGIC Creates all Delta tables for the financial closure process

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Data Tables

# COMMAND ----------

# Exchange Rates Raw
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_EXCHANGE_RATES_RAW} (
        load_id STRING,
        currency STRING,
        rate DOUBLE,
        rate_date DATE,
        source STRING,
        loaded_at TIMESTAMP,
        file_name STRING
    )
    USING DELTA
    COMMENT 'Raw exchange rate data as received'
""")

print("✅ Exchange rates raw table created")

# COMMAND ----------

# BU Preliminary Close Raw
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_BU_PRELIMINARY_RAW} (
        load_id STRING,
        bu_code STRING,
        account_code STRING,
        account_description STRING,
        local_currency STRING,
        local_amount DOUBLE,
        period STRING,
        cost_center STRING,
        loaded_at TIMESTAMP,
        file_name STRING
    )
    USING DELTA
    COMMENT 'Raw preliminary close data from business units'
""")

print("✅ BU preliminary close raw table created")

# COMMAND ----------

# Accounting Cuts Raw
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_ACCOUNTING_CUTS_RAW} (
        load_id STRING,
        cut_number INT,
        bu_code STRING,
        account_code STRING,
        adjustment_type STRING,
        adjustment_amount DOUBLE,
        currency STRING,
        description STRING,
        period STRING,
        loaded_at TIMESTAMP,
        file_name STRING
    )
    USING DELTA
    COMMENT 'Raw accounting adjustment data'
""")

print("✅ Accounting cuts raw table created")

# COMMAND ----------

# Segmented Files Raw
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SEGMENTED_FILES_RAW} (
        load_id STRING,
        bu_code STRING,
        account_code STRING,
        segment_type STRING,
        segment_value STRING,
        amount DOUBLE,
        currency STRING,
        period STRING,
        loaded_at TIMESTAMP,
        file_name STRING
    )
    USING DELTA
    COMMENT 'Raw segmented financial data'
""")

print("✅ Segmented files raw table created")

# COMMAND ----------

# Forecast Files Raw
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FORECAST_FILES_RAW} (
        load_id STRING,
        bu_code STRING,
        account_code STRING,
        forecast_period STRING,
        forecast_amount DOUBLE,
        currency STRING,
        forecast_type STRING,
        version STRING,
        loaded_at TIMESTAMP,
        file_name STRING
    )
    USING DELTA
    COMMENT 'Raw forecast data from business units'
""")

print("✅ Forecast files raw table created")

# COMMAND ----------

# Forecast FX Raw
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FORECAST_FX_RAW} (
        load_id STRING,
        currency STRING,
        forecast_date DATE,
        forecast_rate DOUBLE,
        source STRING,
        loaded_at TIMESTAMP,
        file_name STRING
    )
    USING DELTA
    COMMENT 'Raw forecast exchange rate data'
""")

print("✅ Forecast FX raw table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processed Data Tables

# COMMAND ----------

# Exchange Rates Processed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_EXCHANGE_RATES} (
        rate_id STRING,
        currency STRING,
        rate DOUBLE,
        rate_date DATE,
        source STRING,
        variance_from_prior DOUBLE,
        is_validated BOOLEAN,
        validation_notes STRING,
        processed_at TIMESTAMP,
        closure_period STRING
    )
    USING DELTA
    COMMENT 'Validated and processed exchange rates'
""")

print("✅ Exchange rates processed table created")

# COMMAND ----------

# BU Preliminary Close Processed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_BU_PRELIMINARY} (
        record_id STRING,
        bu_code STRING,
        bu_name STRING,
        account_code STRING,
        account_description STRING,
        local_currency STRING,
        local_amount DOUBLE,
        usd_amount DOUBLE,
        exchange_rate DOUBLE,
        period STRING,
        cost_center STRING,
        is_validated BOOLEAN,
        validation_issues ARRAY<STRING>,
        processed_at TIMESTAMP,
        closure_period STRING
    )
    USING DELTA
    COMMENT 'Validated preliminary close data'
""")

print("✅ BU preliminary close processed table created")

# COMMAND ----------

# Accounting Cuts Processed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_ACCOUNTING_CUTS} (
        cut_id STRING,
        cut_number INT,
        bu_code STRING,
        account_code STRING,
        adjustment_type STRING,
        adjustment_amount DOUBLE,
        currency STRING,
        usd_amount DOUBLE,
        description STRING,
        period STRING,
        approved_by STRING,
        approved_at TIMESTAMP,
        is_validated BOOLEAN,
        processed_at TIMESTAMP,
        closure_period STRING
    )
    USING DELTA
    COMMENT 'Validated accounting adjustments'
""")

print("✅ Accounting cuts processed table created")

# COMMAND ----------

# Segmented Files Processed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SEGMENTED_FILES} (
        segment_id STRING,
        bu_code STRING,
        account_code STRING,
        segment_type STRING,
        segment_value STRING,
        amount DOUBLE,
        currency STRING,
        usd_amount DOUBLE,
        period STRING,
        is_validated BOOLEAN,
        processed_at TIMESTAMP,
        closure_period STRING
    )
    USING DELTA
    COMMENT 'Validated segmented financial data'
""")

print("✅ Segmented files processed table created")

# COMMAND ----------

# Forecast Files Processed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FORECAST_FILES} (
        forecast_id STRING,
        bu_code STRING,
        account_code STRING,
        forecast_period STRING,
        forecast_amount DOUBLE,
        currency STRING,
        usd_amount DOUBLE,
        forecast_type STRING,
        version STRING,
        is_validated BOOLEAN,
        variance_from_prior DOUBLE,
        processed_at TIMESTAMP,
        closure_period STRING
    )
    USING DELTA
    COMMENT 'Validated forecast data'
""")

print("✅ Forecast files processed table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reporting Tables

# COMMAND ----------

# Preliminary Results
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_PRELIMINARY_RESULTS} (
        result_id STRING,
        closure_period STRING,
        bu_code STRING,
        bu_name STRING,
        account_code STRING,
        account_description STRING,
        account_category STRING,
        preliminary_amount DOUBLE,
        currency STRING,
        usd_amount DOUBLE,
        prior_period_amount DOUBLE,
        variance_amount DOUBLE,
        variance_percent DOUBLE,
        published_at TIMESTAMP,
        version INT
    )
    USING DELTA
    COMMENT 'Preliminary close results for review'
""")

print("✅ Preliminary results table created")

# COMMAND ----------

# Final Results
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FINAL_RESULTS} (
        result_id STRING,
        closure_period STRING,
        bu_code STRING,
        bu_name STRING,
        region STRING,
        account_code STRING,
        account_description STRING,
        account_category STRING,
        segment_product STRING,
        segment_geography STRING,
        segment_customer STRING,
        segment_channel STRING,
        preliminary_amount DOUBLE,
        adjustments_amount DOUBLE,
        final_amount DOUBLE,
        currency STRING,
        usd_amount DOUBLE,
        forecast_amount DOUBLE,
        forecast_variance DOUBLE,
        prior_period_amount DOUBLE,
        yoy_variance DOUBLE,
        yoy_variance_percent DOUBLE,
        published_at TIMESTAMP,
        is_final BOOLEAN,
        approved_by STRING,
        approved_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Final close results with all segments and comparisons'
""")

print("✅ Final results table created")

# COMMAND ----------

# KPI Metrics
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_KPI_METRICS} (
        metric_id STRING,
        closure_period STRING,
        metric_name STRING,
        metric_category STRING,
        metric_value DOUBLE,
        target_value DOUBLE,
        variance_from_target DOUBLE,
        bu_code STRING,
        region STRING,
        calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Key performance indicators for the closure period'
""")

print("✅ KPI metrics table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables

# COMMAND ----------

# Agent Execution Logs
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_AGENT_LOGS} (
        log_id STRING,
        agent_id STRING,
        agent_name STRING,
        closure_period STRING,
        phase INT,
        task_name STRING,
        status STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        duration_seconds DOUBLE,
        records_processed BIGINT,
        error_message STRING,
        metadata MAP<STRING, STRING>
    )
    USING DELTA
    COMMENT 'Detailed logs of all agent executions'
""")

print("✅ Agent logs table created")

# COMMAND ----------

# Data Quality Checks
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_DATA_QUALITY} (
        check_id STRING,
        closure_period STRING,
        phase INT,
        table_name STRING,
        check_type STRING,
        check_name STRING,
        check_result STRING,
        records_checked BIGINT,
        records_failed BIGINT,
        failure_rate DOUBLE,
        details STRING,
        checked_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Results of all data quality validations'
""")

print("✅ Data quality table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Indexes and Optimize

# COMMAND ----------

# Optimize and Z-order key tables
tables_to_optimize = [
    (TABLE_EXCHANGE_RATES, ["currency", "rate_date"]),
    (TABLE_BU_PRELIMINARY, ["bu_code", "period"]),
    (TABLE_ACCOUNTING_CUTS, ["cut_number", "period"]),
    (TABLE_PRELIMINARY_RESULTS, ["closure_period", "bu_code"]),
    (TABLE_FINAL_RESULTS, ["closure_period", "bu_code"]),
    (TABLE_AGENT_LOGS, ["closure_period", "phase"]),
    (TABLE_DATA_QUALITY, ["closure_period", "phase"])
]

for table_name, z_order_cols in tables_to_optimize:
    try:
        # Optimize table
        spark.sql(f"OPTIMIZE {table_name}")
        
        # Z-order by key columns
        z_order_str = ", ".join(z_order_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_str})")
        
        print(f"✅ Optimized: {table_name}")
    except Exception as e:
        print(f"⚠️  Optimization skipped for {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Creation

# COMMAND ----------

print("\n📊 Table Creation Summary:\n")

schemas_to_check = [
    (FULL_SCHEMA_RAW, "Raw Data"),
    (FULL_SCHEMA_PROCESSED, "Processed Data"),
    (FULL_SCHEMA_REPORTING, "Reporting"),
    (FULL_SCHEMA_AUDIT, "Audit")
]

total_tables = 0
for schema, label in schemas_to_check:
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
    count = len(tables)
    total_tables += count
    print(f"   {label}: {count} tables")
    for table in tables:
        print(f"      • {table.tableName}")

print(f"\n✅ Total tables created: {total_tables}")
print("\n📝 Next step: Run data simulation notebooks to populate tables")
