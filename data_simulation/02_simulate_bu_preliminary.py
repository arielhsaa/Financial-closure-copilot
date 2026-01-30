# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate BU Preliminary Close Data
# MAGIC Generates realistic preliminary close files from business units

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Account Structure

# COMMAND ----------

accounts = [
    # Revenue accounts
    {"code": "4000", "description": "Product Revenue", "category": "Revenue", "sign": 1},
    {"code": "4100", "description": "Service Revenue", "category": "Revenue", "sign": 1},
    {"code": "4200", "description": "Subscription Revenue", "category": "Revenue", "sign": 1},
    {"code": "4300", "description": "Other Revenue", "category": "Revenue", "sign": 1},
    
    # COGS accounts
    {"code": "5000", "description": "Product Cost", "category": "COGS", "sign": -1},
    {"code": "5100", "description": "Service Cost", "category": "COGS", "sign": -1},
    {"code": "5200", "description": "Freight & Logistics", "category": "COGS", "sign": -1},
    
    # Operating expenses
    {"code": "6000", "description": "Sales & Marketing", "category": "OpEx", "sign": -1},
    {"code": "6100", "description": "Research & Development", "category": "OpEx", "sign": -1},
    {"code": "6200", "description": "General & Administrative", "category": "OpEx", "sign": -1},
    {"code": "6300", "description": "Facilities", "category": "OpEx", "sign": -1},
    {"code": "6400", "description": "IT & Technology", "category": "OpEx", "sign": -1},
    
    # Other
    {"code": "7000", "description": "Interest Income", "category": "Other", "sign": 1},
    {"code": "7100", "description": "Interest Expense", "category": "Other", "sign": -1},
    {"code": "8000", "description": "Tax Expense", "category": "Tax", "sign": -1}
]

cost_centers = ["CC001", "CC002", "CC003", "CC004", "CC005"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Preliminary Close Data

# COMMAND ----------

current_period = get_current_closure_period()
preliminary_data = []
load_id = str(uuid.uuid4())

# Generate data for each BU
for bu_code, bu_info in BUSINESS_UNITS.items():
    local_currency = bu_info["currency"]
    
    # Base multiplier based on BU size
    bu_multipliers = {"NA": 10, "EMEA": 8, "APAC": 6, "LATAM": 4}
    multiplier = bu_multipliers.get(bu_code, 5)
    
    # Generate records for each account
    for account in accounts:
        for cost_center in cost_centers:
            # Generate amount with some randomness
            base_amount = (hash(bu_code + account["code"] + cost_center) % 1000000) / 100
            amount = base_amount * multiplier * account["sign"]
            
            # Add monthly variance
            variance = (hash(current_period + bu_code + account["code"]) % 200 - 100) / 1000
            amount = amount * (1 + variance)
            
            preliminary_data.append({
                "load_id": load_id,
                "bu_code": bu_code,
                "account_code": account["code"],
                "account_description": account["description"],
                "local_currency": local_currency,
                "local_amount": round(amount, 2),
                "period": current_period,
                "cost_center": cost_center,
                "loaded_at": datetime.now(),
                "file_name": f"{bu_code}_preliminary_{current_period.replace('-', '')}.csv"
            })

# Create DataFrame
schema = StructType([
    StructField("load_id", StringType(), False),
    StructField("bu_code", StringType(), False),
    StructField("account_code", StringType(), False),
    StructField("account_description", StringType(), True),
    StructField("local_currency", StringType(), False),
    StructField("local_amount", DoubleType(), False),
    StructField("period", StringType(), False),
    StructField("cost_center", StringType(), True),
    StructField("loaded_at", TimestampType(), False),
    StructField("file_name", StringType(), True)
])

df_preliminary = spark.createDataFrame(preliminary_data, schema)

print(f"✅ Generated {df_preliminary.count()} preliminary close records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Data Quality Issues for Testing

# COMMAND ----------

# Add some records with issues for agent testing
issues = [
    # Missing account description
    {
        "load_id": load_id,
        "bu_code": "NA",
        "account_code": "9999",
        "account_description": None,
        "local_currency": "USD",
        "local_amount": 50000.0,
        "period": current_period,
        "cost_center": "CC001",
        "loaded_at": datetime.now(),
        "file_name": f"NA_preliminary_{current_period.replace('-', '')}_error.csv"
    },
    # Invalid BU code
    {
        "load_id": load_id,
        "bu_code": "INVALID",
        "account_code": "4000",
        "account_description": "Test Revenue",
        "local_currency": "USD",
        "local_amount": 10000.0,
        "period": current_period,
        "cost_center": "CC001",
        "loaded_at": datetime.now(),
        "file_name": f"INVALID_preliminary_{current_period.replace('-', '')}.csv"
    }
]

df_issues = spark.createDataFrame(issues, schema)
df_preliminary = df_preliminary.union(df_issues)

print("✅ Added records with data quality issues for testing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Raw Table

# COMMAND ----------

df_preliminary.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_BU_PRELIMINARY_RAW)

print(f"✅ Loaded {df_preliminary.count()} records to {TABLE_BU_PRELIMINARY_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        account_code,
        account_description,
        local_currency,
        local_amount,
        period,
        cost_center
    FROM {TABLE_BU_PRELIMINARY_RAW}
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        local_currency,
        COUNT(*) as record_count,
        SUM(local_amount) as total_amount,
        AVG(local_amount) as avg_amount,
        COUNT(DISTINCT account_code) as unique_accounts,
        COUNT(DISTINCT cost_center) as unique_cost_centers
    FROM {TABLE_BU_PRELIMINARY_RAW}
    WHERE bu_code IN ('NA', 'EMEA', 'APAC', 'LATAM')
    GROUP BY bu_code, local_currency
    ORDER BY bu_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account Category Summary

# COMMAND ----------

display(spark.sql(f"""
    WITH account_mapping AS (
        SELECT 
            '4%' as pattern,
            'Revenue' as category
        UNION ALL SELECT '5%', 'COGS'
        UNION ALL SELECT '6%', 'OpEx'
        UNION ALL SELECT '7%', 'Other'
        UNION ALL SELECT '8%', 'Tax'
    )
    SELECT 
        CASE 
            WHEN p.account_code LIKE '4%' THEN 'Revenue'
            WHEN p.account_code LIKE '5%' THEN 'COGS'
            WHEN p.account_code LIKE '6%' THEN 'OpEx'
            WHEN p.account_code LIKE '7%' THEN 'Other'
            WHEN p.account_code LIKE '8%' THEN 'Tax'
            ELSE 'Unknown'
        END as category,
        COUNT(*) as record_count,
        SUM(p.local_amount) as total_amount
    FROM {TABLE_BU_PRELIMINARY_RAW} p
    WHERE bu_code IN ('NA', 'EMEA', 'APAC', 'LATAM')
    GROUP BY category
    ORDER BY category
"""))

# COMMAND ----------

print("✅ BU preliminary close simulation complete!")
