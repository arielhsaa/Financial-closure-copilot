# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate Accounting Cuts
# MAGIC Generates realistic accounting adjustment data (1st and 2nd cut)

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Adjustment Types

# COMMAND ----------

adjustment_types = [
    "Accrual Adjustment",
    "Reclassification",
    "Revenue Recognition",
    "Depreciation",
    "Foreign Exchange",
    "Intercompany Elimination",
    "Inventory Adjustment",
    "Bad Debt Provision",
    "Tax Adjustment",
    "Other"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate First Accounting Cut

# COMMAND ----------

current_period = get_current_closure_period()
cut1_data = []
load_id_cut1 = str(uuid.uuid4())

# Generate adjustments for each BU
for bu_code in BUSINESS_UNITS.keys():
    bu_currency = get_bu_currency(bu_code)
    
    # Generate 5-10 adjustments per BU for first cut
    num_adjustments = 5 + (hash(bu_code + "cut1") % 6)
    
    for i in range(num_adjustments):
        adj_type = adjustment_types[hash(bu_code + str(i)) % len(adjustment_types)]
        
        # Generate adjustment amount (typically smaller corrections)
        base_amount = (hash(bu_code + str(i) + "cut1") % 100000) / 100
        amount = base_amount * (1 if i % 2 == 0 else -1)
        
        # Account code (adjustments typically to revenue, COGS, or OpEx)
        account_codes = ["4000", "4100", "5000", "5100", "6000", "6200", "7100"]
        account_code = account_codes[hash(bu_code + str(i)) % len(account_codes)]
        
        cut1_data.append({
            "load_id": load_id_cut1,
            "cut_number": 1,
            "bu_code": bu_code,
            "account_code": account_code,
            "adjustment_type": adj_type,
            "adjustment_amount": round(amount, 2),
            "currency": bu_currency,
            "description": f"{adj_type} for {account_code} - Cut 1",
            "period": current_period,
            "loaded_at": datetime.now(),
            "file_name": f"{bu_code}_accounting_cut1_{current_period.replace('-', '')}.csv"
        })

print(f"✅ Generated {len(cut1_data)} first cut adjustments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Second (Final) Accounting Cut

# COMMAND ----------

cut2_data = []
load_id_cut2 = str(uuid.uuid4())

# Generate adjustments for second cut (typically fewer and smaller)
for bu_code in BUSINESS_UNITS.keys():
    bu_currency = get_bu_currency(bu_code)
    
    # Generate 2-5 adjustments per BU for second cut
    num_adjustments = 2 + (hash(bu_code + "cut2") % 4)
    
    for i in range(num_adjustments):
        adj_type = adjustment_types[hash(bu_code + str(i) + "cut2") % len(adjustment_types)]
        
        # Generate smaller adjustment amounts for final cut
        base_amount = (hash(bu_code + str(i) + "cut2") % 50000) / 100
        amount = base_amount * (1 if i % 2 == 0 else -1)
        
        account_codes = ["4000", "5000", "6000", "6200", "8000"]
        account_code = account_codes[hash(bu_code + str(i) + "2") % len(account_codes)]
        
        cut2_data.append({
            "load_id": load_id_cut2,
            "cut_number": 2,
            "bu_code": bu_code,
            "account_code": account_code,
            "adjustment_type": adj_type,
            "adjustment_amount": round(amount, 2),
            "currency": bu_currency,
            "description": f"{adj_type} for {account_code} - Final Cut",
            "period": current_period,
            "loaded_at": datetime.now(),
            "file_name": f"{bu_code}_accounting_cut2_{current_period.replace('-', '')}.csv"
        })

print(f"✅ Generated {len(cut2_data)} second cut adjustments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Create DataFrame

# COMMAND ----------

all_cuts = cut1_data + cut2_data

schema = StructType([
    StructField("load_id", StringType(), False),
    StructField("cut_number", IntegerType(), False),
    StructField("bu_code", StringType(), False),
    StructField("account_code", StringType(), False),
    StructField("adjustment_type", StringType(), False),
    StructField("adjustment_amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("description", StringType(), True),
    StructField("period", StringType(), False),
    StructField("loaded_at", TimestampType(), False),
    StructField("file_name", StringType(), True)
])

df_cuts = spark.createDataFrame(all_cuts, schema)

print(f"✅ Total accounting cuts: {df_cuts.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Raw Table

# COMMAND ----------

df_cuts.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_ACCOUNTING_CUTS_RAW)

print(f"✅ Loaded {df_cuts.count()} records to {TABLE_ACCOUNTING_CUTS_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        cut_number,
        bu_code,
        account_code,
        adjustment_type,
        adjustment_amount,
        currency,
        description,
        period
    FROM {TABLE_ACCOUNTING_CUTS_RAW}
    ORDER BY cut_number, bu_code
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Cut

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        cut_number,
        COUNT(*) as adjustment_count,
        SUM(adjustment_amount) as total_adjustments,
        AVG(adjustment_amount) as avg_adjustment,
        COUNT(DISTINCT bu_code) as bu_count,
        COUNT(DISTINCT adjustment_type) as adjustment_types
    FROM {TABLE_ACCOUNTING_CUTS_RAW}
    GROUP BY cut_number
    ORDER BY cut_number
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by BU and Cut

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        cut_number,
        currency,
        COUNT(*) as adjustment_count,
        SUM(adjustment_amount) as total_adjustments,
        AVG(adjustment_amount) as avg_adjustment
    FROM {TABLE_ACCOUNTING_CUTS_RAW}
    GROUP BY bu_code, cut_number, currency
    ORDER BY bu_code, cut_number
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adjustment Type Distribution

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        adjustment_type,
        cut_number,
        COUNT(*) as count,
        SUM(adjustment_amount) as total_amount
    FROM {TABLE_ACCOUNTING_CUTS_RAW}
    GROUP BY adjustment_type, cut_number
    ORDER BY adjustment_type, cut_number
"""))

# COMMAND ----------

print("✅ Accounting cuts simulation complete!")
