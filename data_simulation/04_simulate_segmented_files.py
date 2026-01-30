# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate Segmented Files
# MAGIC Generates realistic segmented financial data from business units

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Segments

# COMMAND ----------

segments = {
    "Product": ["Hardware", "Software", "Services", "Subscriptions"],
    "Geography": ["North", "South", "East", "West", "Central"],
    "Customer": ["Enterprise", "SMB", "Consumer"],
    "Channel": ["Direct", "Partner", "Online", "Retail"]
}

# Revenue and COGS accounts for segmentation
segmented_accounts = [
    {"code": "4000", "description": "Product Revenue"},
    {"code": "4100", "description": "Service Revenue"},
    {"code": "4200", "description": "Subscription Revenue"},
    {"code": "5000", "description": "Product Cost"},
    {"code": "5100", "description": "Service Cost"}
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Segmented Data

# COMMAND ----------

current_period = get_current_closure_period()
segmented_data = []
load_id = str(uuid.uuid4())

# Generate segmented data for each BU
for bu_code, bu_info in BUSINESS_UNITS.items():
    bu_currency = bu_info["currency"]
    
    # Base multiplier for BU size
    bu_multipliers = {"NA": 10, "EMEA": 8, "APAC": 6, "LATAM": 4}
    multiplier = bu_multipliers.get(bu_code, 5)
    
    # Generate records for each account
    for account in segmented_accounts:
        # Generate combinations of segments
        for product in segments["Product"]:
            for geography in segments["Geography"]:
                for customer in segments["Customer"]:
                    for channel in segments["Channel"]:
                        # Generate amount with randomness
                        seed = hash(bu_code + account["code"] + product + geography + customer + channel)
                        base_amount = (seed % 50000) / 100
                        amount = base_amount * multiplier
                        
                        # Add variance
                        variance = (hash(current_period + str(seed)) % 200 - 100) / 1000
                        amount = amount * (1 + variance)
                        
                        # COGS should be negative
                        if account["code"].startswith("5"):
                            amount = -abs(amount)
                        
                        # Create record for each segment type
                        segments_to_add = [
                            ("Product", product),
                            ("Geography", geography),
                            ("Customer", customer),
                            ("Channel", channel)
                        ]
                        
                        for segment_type, segment_value in segments_to_add:
                            segmented_data.append({
                                "load_id": load_id,
                                "bu_code": bu_code,
                                "account_code": account["code"],
                                "segment_type": segment_type,
                                "segment_value": segment_value,
                                "amount": round(amount / len(segments_to_add), 2),
                                "currency": bu_currency,
                                "period": current_period,
                                "loaded_at": datetime.now(),
                                "file_name": f"{bu_code}_segmented_{current_period.replace('-', '')}.csv"
                            })

print(f"✅ Generated {len(segmented_data)} segmented records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Records (Too many to load all)

# COMMAND ----------

# Take a representative sample to avoid overwhelming the system
sample_size = min(10000, len(segmented_data))
sampled_indices = [i * len(segmented_data) // sample_size for i in range(sample_size)]
segmented_sample = [segmented_data[i] for i in sampled_indices]

print(f"✅ Sampled {len(segmented_sample)} records for loading")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DataFrame

# COMMAND ----------

schema = StructType([
    StructField("load_id", StringType(), False),
    StructField("bu_code", StringType(), False),
    StructField("account_code", StringType(), False),
    StructField("segment_type", StringType(), False),
    StructField("segment_value", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("period", StringType(), False),
    StructField("loaded_at", TimestampType(), False),
    StructField("file_name", StringType(), True)
])

df_segmented = spark.createDataFrame(segmented_sample, schema)

print(f"✅ Created DataFrame with {df_segmented.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Missing Segment Issues for Testing

# COMMAND ----------

# Add some incomplete segment data for testing
issues = [
    {
        "load_id": load_id,
        "bu_code": "NA",
        "account_code": "4000",
        "segment_type": "Product",
        "segment_value": "Unknown",
        "amount": 50000.0,
        "currency": "USD",
        "period": current_period,
        "loaded_at": datetime.now(),
        "file_name": f"NA_segmented_{current_period.replace('-', '')}_incomplete.csv"
    }
]

df_issues = spark.createDataFrame(issues, schema)
df_segmented = df_segmented.union(df_issues)

print("✅ Added incomplete segment records for testing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Raw Table

# COMMAND ----------

df_segmented.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_SEGMENTED_FILES_RAW)

print(f"✅ Loaded {df_segmented.count()} records to {TABLE_SEGMENTED_FILES_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        account_code,
        segment_type,
        segment_value,
        amount,
        currency,
        period
    FROM {TABLE_SEGMENTED_FILES_RAW}
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Segment Type

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        segment_type,
        COUNT(*) as record_count,
        COUNT(DISTINCT segment_value) as unique_values,
        SUM(amount) as total_amount,
        COUNT(DISTINCT bu_code) as bu_count
    FROM {TABLE_SEGMENTED_FILES_RAW}
    GROUP BY segment_type
    ORDER BY segment_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        currency,
        COUNT(*) as record_count,
        COUNT(DISTINCT segment_type) as segment_types,
        SUM(amount) as total_amount
    FROM {TABLE_SEGMENTED_FILES_RAW}
    GROUP BY bu_code, currency
    ORDER BY bu_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Segment Analysis

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        segment_value as product,
        COUNT(*) as record_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {TABLE_SEGMENTED_FILES_RAW}
    WHERE segment_type = 'Product'
    GROUP BY segment_value
    ORDER BY total_amount DESC
"""))

# COMMAND ----------

print("✅ Segmented files simulation complete!")
