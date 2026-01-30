# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate Forecast Files
# MAGIC Generates realistic forecast data and forecast FX rates

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Forecast Files

# COMMAND ----------

current_period = get_current_closure_period()
current_year, current_month = map(int, current_period.split('-'))

# Generate forecasts for next 12 months
forecast_periods = []
for i in range(1, 13):
    month = current_month + i
    year = current_year
    while month > 12:
        month -= 12
        year += 1
    forecast_periods.append(f"{year:04d}-{month:02d}")

# Account codes for forecasting
forecast_accounts = [
    {"code": "4000", "description": "Product Revenue", "volatility": 0.1},
    {"code": "4100", "description": "Service Revenue", "volatility": 0.08},
    {"code": "4200", "description": "Subscription Revenue", "volatility": 0.05},
    {"code": "5000", "description": "Product Cost", "volatility": 0.12},
    {"code": "5100", "description": "Service Cost", "volatility": 0.10},
    {"code": "6000", "description": "Sales & Marketing", "volatility": 0.15},
    {"code": "6100", "description": "R&D", "volatility": 0.07},
    {"code": "6200", "description": "G&A", "volatility": 0.06}
]

# COMMAND ----------

forecast_data = []
load_id = str(uuid.uuid4())

# Generate forecasts for each BU
for bu_code, bu_info in BUSINESS_UNITS.items():
    bu_currency = bu_info["currency"]
    
    # Base multiplier
    bu_multipliers = {"NA": 10, "EMEA": 8, "APAC": 6, "LATAM": 4}
    multiplier = bu_multipliers.get(bu_code, 5)
    
    # Generate forecasts for each account and period
    for account in forecast_accounts:
        # Base amount for this account
        base_amount = (hash(bu_code + account["code"]) % 1000000) / 100
        base_amount = base_amount * multiplier
        
        # COGS and expenses are negative
        if account["code"].startswith("5") or account["code"].startswith("6"):
            base_amount = -abs(base_amount)
        
        # Generate forecast for each period with trend and seasonality
        for period_idx, forecast_period in enumerate(forecast_periods):
            # Add growth trend (2% per month for revenue, 1.5% for costs)
            growth_rate = 0.02 if account["code"].startswith("4") else 0.015
            trend_factor = (1 + growth_rate) ** period_idx
            
            # Add seasonality (Q4 is typically stronger)
            month = int(forecast_period.split('-')[1])
            seasonality = 1.0
            if month in [11, 12]:  # Nov, Dec
                seasonality = 1.15
            elif month in [1, 2]:  # Jan, Feb
                seasonality = 0.90
            
            # Add random variance based on volatility
            seed = hash(bu_code + account["code"] + forecast_period)
            variance = (seed % 200 - 100) / 1000 * account["volatility"]
            
            # Calculate final forecast amount
            forecast_amount = base_amount * trend_factor * seasonality * (1 + variance)
            
            forecast_data.append({
                "load_id": load_id,
                "bu_code": bu_code,
                "account_code": account["code"],
                "forecast_period": forecast_period,
                "forecast_amount": round(forecast_amount, 2),
                "currency": bu_currency,
                "forecast_type": "Rolling12",
                "version": "v1.0",
                "loaded_at": datetime.now(),
                "file_name": f"{bu_code}_forecast_{current_period.replace('-', '')}.csv"
            })

print(f"✅ Generated {len(forecast_data)} forecast records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Forecast DataFrame

# COMMAND ----------

schema_forecast = StructType([
    StructField("load_id", StringType(), False),
    StructField("bu_code", StringType(), False),
    StructField("account_code", StringType(), False),
    StructField("forecast_period", StringType(), False),
    StructField("forecast_amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("forecast_type", StringType(), True),
    StructField("version", StringType(), True),
    StructField("loaded_at", TimestampType(), False),
    StructField("file_name", StringType(), True)
])

df_forecast = spark.createDataFrame(forecast_data, schema_forecast)

print(f"✅ Created forecast DataFrame with {df_forecast.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Forecast FX Rates

# COMMAND ----------

# Base rates (same as current rates)
base_fx_rates = {
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

forecast_fx_data = []
load_id_fx = str(uuid.uuid4())

# Generate forecast rates for next 12 months
for currency, base_rate in base_fx_rates.items():
    for period_idx, forecast_period in enumerate(forecast_periods):
        # Add gradual drift (analyst predictions)
        # Assume slight appreciation/depreciation trends
        drift = 0.002 if hash(currency) % 2 == 0 else -0.002
        trend_factor = (1 + drift) ** period_idx
        
        # Add variance
        variance = (hash(currency + forecast_period) % 100 - 50) / 5000
        
        forecast_rate = base_rate * trend_factor * (1 + variance)
        
        # Convert period to date (first day of month)
        year, month = map(int, forecast_period.split('-'))
        forecast_date = datetime(year, month, 1).date()
        
        forecast_fx_data.append({
            "load_id": load_id_fx,
            "currency": currency,
            "forecast_date": forecast_date,
            "forecast_rate": round(forecast_rate, 4),
            "source": "TREASURY_FORECAST",
            "loaded_at": datetime.now(),
            "file_name": f"fx_forecast_{current_period.replace('-', '')}.csv"
        })

print(f"✅ Generated {len(forecast_fx_data)} forecast FX records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Forecast FX DataFrame

# COMMAND ----------

schema_fx = StructType([
    StructField("load_id", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("forecast_date", DateType(), False),
    StructField("forecast_rate", DoubleType(), False),
    StructField("source", StringType(), True),
    StructField("loaded_at", TimestampType(), False),
    StructField("file_name", StringType(), True)
])

df_forecast_fx = spark.createDataFrame(forecast_fx_data, schema_fx)

print(f"✅ Created forecast FX DataFrame with {df_forecast_fx.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Raw Tables

# COMMAND ----------

# Write forecast files
df_forecast.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_FORECAST_FILES_RAW)

print(f"✅ Loaded {df_forecast.count()} records to {TABLE_FORECAST_FILES_RAW}")

# Write forecast FX
df_forecast_fx.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_FORECAST_FX_RAW)

print(f"✅ Loaded {df_forecast_fx.count()} records to {TABLE_FORECAST_FX_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Forecast Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        account_code,
        forecast_period,
        forecast_amount,
        currency,
        forecast_type,
        version
    FROM {TABLE_FORECAST_FILES_RAW}
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Forecast FX

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        currency,
        forecast_date,
        forecast_rate,
        source
    FROM {TABLE_FORECAST_FX_RAW}
    ORDER BY forecast_date, currency
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast Summary by BU

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        bu_code,
        currency,
        COUNT(*) as forecast_count,
        COUNT(DISTINCT forecast_period) as periods,
        COUNT(DISTINCT account_code) as accounts,
        SUM(forecast_amount) as total_forecast
    FROM {TABLE_FORECAST_FILES_RAW}
    GROUP BY bu_code, currency
    ORDER BY bu_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast Trend Analysis

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        forecast_period,
        SUM(CASE WHEN forecast_amount > 0 THEN forecast_amount ELSE 0 END) as total_revenue,
        SUM(CASE WHEN forecast_amount < 0 THEN forecast_amount ELSE 0 END) as total_costs,
        SUM(forecast_amount) as net_income
    FROM {TABLE_FORECAST_FILES_RAW}
    GROUP BY forecast_period
    ORDER BY forecast_period
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast FX Rate Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        currency,
        COUNT(*) as forecast_count,
        MIN(forecast_rate) as min_rate,
        MAX(forecast_rate) as max_rate,
        AVG(forecast_rate) as avg_rate,
        MIN(forecast_date) as first_date,
        MAX(forecast_date) as last_date
    FROM {TABLE_FORECAST_FX_RAW}
    GROUP BY currency
    ORDER BY currency
"""))

# COMMAND ----------

print("✅ Forecast files simulation complete!")
