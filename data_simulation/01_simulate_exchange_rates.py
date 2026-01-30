# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate Exchange Rate Data
# MAGIC Generates realistic exchange rate data for testing

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Base Exchange Rates

# COMMAND ----------

# Base rates (relative to USD)
base_rates = {
    "EUR": 0.92,
    "GBP": 0.79,
    "JPY": 149.50,
    "CNY": 7.24,
    "SGD": 1.34,
    "BRL": 4.97,
    "MXN": 17.13,
    "CAD": 1.36,
    "AUD": 1.52
}

# Generate daily rates for last 90 days with realistic variance
current_period = get_current_closure_period()
end_date = datetime.now().date()
start_date = end_date - timedelta(days=90)

rates_data = []
load_id = str(uuid.uuid4())

for currency, base_rate in base_rates.items():
    current_date = start_date
    current_rate = base_rate
    
    while current_date <= end_date:
        # Add daily variance (-1% to +1%)
        daily_variance = (hash(str(current_date) + currency) % 200 - 100) / 10000
        current_rate = base_rate * (1 + daily_variance)
        
        rates_data.append({
            "load_id": load_id,
            "currency": currency,
            "rate": round(current_rate, 4),
            "rate_date": current_date,
            "source": "CENTRAL_BANK",
            "loaded_at": datetime.now(),
            "file_name": f"fx_rates_{current_date.strftime('%Y%m%d')}.csv"
        })
        
        current_date += timedelta(days=1)

# Create DataFrame
schema = StructType([
    StructField("load_id", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("rate", DoubleType(), False),
    StructField("rate_date", DateType(), False),
    StructField("source", StringType(), True),
    StructField("loaded_at", TimestampType(), False),
    StructField("file_name", StringType(), True)
])

df_rates = spark.createDataFrame(rates_data, schema)

print(f"✅ Generated {df_rates.count()} exchange rate records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Some Anomalies for Testing

# COMMAND ----------

# Add a few records with unusual variances for agent testing
anomaly_date = end_date - timedelta(days=5)
anomalies = [
    {
        "load_id": str(uuid.uuid4()),
        "currency": "GBP",
        "rate": 0.72,  # 8% variance
        "rate_date": anomaly_date,
        "source": "CENTRAL_BANK",
        "loaded_at": datetime.now(),
        "file_name": f"fx_rates_{anomaly_date.strftime('%Y%m%d')}_correction.csv"
    }
]

df_anomalies = spark.createDataFrame(anomalies, schema)
df_rates = df_rates.union(df_anomalies)

print("✅ Added anomaly records for testing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Raw Table

# COMMAND ----------

# Write to Delta table
df_rates.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_EXCHANGE_RATES_RAW)

print(f"✅ Loaded {df_rates.count()} records to {TABLE_EXCHANGE_RATES_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        currency,
        rate_date,
        rate,
        source,
        loaded_at
    FROM {TABLE_EXCHANGE_RATES_RAW}
    WHERE rate_date >= date_sub(current_date(), 7)
    ORDER BY rate_date DESC, currency
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        currency,
        COUNT(*) as record_count,
        MIN(rate) as min_rate,
        MAX(rate) as max_rate,
        AVG(rate) as avg_rate,
        STDDEV(rate) as stddev_rate,
        MIN(rate_date) as earliest_date,
        MAX(rate_date) as latest_date
    FROM {TABLE_EXCHANGE_RATES_RAW}
    GROUP BY currency
    ORDER BY currency
"""))

# COMMAND ----------

print("✅ Exchange rate simulation complete!")
