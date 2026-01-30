# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Closure Metrics Dashboard
# MAGIC Real-time metrics and KPIs for the closure process

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closure Cycle Overview

# COMMAND ----------

current_period = get_current_closure_period()

print(f"📅 Closure Period: {current_period}\n")

# Get all closure periods
all_periods = spark.sql(f"""
    SELECT DISTINCT closure_period
    FROM {TABLE_CLOSURE_STATUS}
    ORDER BY closure_period DESC
    LIMIT 6
""")

print("📊 Recent Closure Periods:")
display(all_periods)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Cycle Status

# COMMAND ----------

cycle_status = spark.sql(f"""
    SELECT 
        cs.closure_period,
        cs.current_phase,
        CASE cs.current_phase
            WHEN 1 THEN 'Data Gathering - Exchange Rates'
            WHEN 2 THEN 'Adjustments'
            WHEN 3 THEN 'Data Gathering - Segmented & Forecast'
            WHEN 4 THEN 'Review'
            WHEN 5 THEN 'Reporting & Sign-off'
        END as phase_name,
        cs.phase_status,
        cs.started_at,
        cs.expected_completion,
        cs.actual_completion,
        CASE 
            WHEN cs.actual_completion IS NOT NULL THEN 
                DATEDIFF(cs.actual_completion, cs.started_at)
            ELSE 
                DATEDIFF(CURRENT_TIMESTAMP(), cs.started_at)
        END as days_elapsed,
        CASE 
            WHEN cs.actual_completion IS NOT NULL THEN 'COMPLETED'
            WHEN cs.expected_completion < CURRENT_TIMESTAMP() THEN 'OVERDUE'
            ELSE 'IN_PROGRESS'
        END as status
    FROM {TABLE_CLOSURE_STATUS} cs
    WHERE cs.closure_period = '{current_period}'
    ORDER BY cs.started_at DESC
    LIMIT 1
""")

display(cycle_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Summary

# COMMAND ----------

financial_summary = spark.sql(f"""
    SELECT 
        '{current_period}' as period,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) as total_revenue,
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as total_cogs,
        SUM(CASE WHEN account_category = 'OpEx' THEN ABS(final_amount) ELSE 0 END) as total_opex,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as gross_profit,
        (SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
         SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END)) /
         NULLIF(SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END), 0) * 100 as gross_margin_pct,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) -
        SUM(CASE WHEN account_category = 'OpEx' THEN ABS(final_amount) ELSE 0 END) as operating_income,
        (SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
         SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) -
         SUM(CASE WHEN account_category = 'OpEx' THEN ABS(final_amount) ELSE 0 END)) /
         NULLIF(SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END), 0) * 100 as operating_margin_pct
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
      AND is_final = true
""")

print("💰 Financial Summary:\n")
display(financial_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Business Unit

# COMMAND ----------

bu_revenue = spark.sql(f"""
    SELECT 
        bu_name,
        region,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) as revenue,
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as cogs,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as gross_profit,
        (SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
         SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END)) /
         NULLIF(SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END), 0) * 100 as gross_margin_pct
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
      AND is_final = true
    GROUP BY bu_name, region
    ORDER BY revenue DESC
""")

print("🏢 Revenue by Business Unit:\n")
display(bu_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Segment Performance

# COMMAND ----------

product_performance = spark.sql(f"""
    SELECT 
        segment_product,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) as revenue,
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as cogs,
        SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
        SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as gross_profit,
        (SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) -
         SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END)) /
         NULLIF(SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END), 0) * 100 as gross_margin_pct
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
      AND is_final = true
      AND segment_product != 'Unallocated'
    GROUP BY segment_product
    ORDER BY revenue DESC
""")

print("📦 Product Segment Performance:\n")
display(product_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forecast Accuracy

# COMMAND ----------

forecast_accuracy = spark.sql(f"""
    SELECT 
        bu_name,
        account_category,
        SUM(final_amount) as actual,
        SUM(forecast_amount) as forecast,
        SUM(forecast_variance) as variance,
        CASE 
            WHEN SUM(forecast_amount) != 0 THEN
                (SUM(forecast_variance) / ABS(SUM(forecast_amount))) * 100
            ELSE 0
        END as variance_pct,
        CASE 
            WHEN ABS((SUM(forecast_variance) / NULLIF(ABS(SUM(forecast_amount)), 0)) * 100) <= 5 THEN '✅ Excellent'
            WHEN ABS((SUM(forecast_variance) / NULLIF(ABS(SUM(forecast_amount)), 0)) * 100) <= 10 THEN '✓ Good'
            WHEN ABS((SUM(forecast_variance) / NULLIF(ABS(SUM(forecast_amount)), 0)) * 100) <= 15 THEN '⚠️  Fair'
            ELSE '❌ Poor'
        END as accuracy_rating
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
      AND is_final = true
      AND forecast_amount IS NOT NULL
    GROUP BY bu_name, account_category
    ORDER BY bu_name, account_category
""")

print("🎯 Forecast Accuracy:\n")
display(forecast_accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Year-over-Year Comparison

# COMMAND ----------

yoy_comparison = spark.sql(f"""
    SELECT 
        account_category,
        SUM(final_amount) as current_period,
        SUM(prior_period_amount) as prior_period,
        SUM(yoy_variance) as variance,
        CASE 
            WHEN SUM(prior_period_amount) != 0 THEN
                (SUM(yoy_variance) / ABS(SUM(prior_period_amount))) * 100
            ELSE 0
        END as variance_pct
    FROM {TABLE_FINAL_RESULTS}
    WHERE closure_period = '{current_period}'
      AND is_final = true
    GROUP BY account_category
    ORDER BY 
        CASE account_category
            WHEN 'Revenue' THEN 1
            WHEN 'COGS' THEN 2
            WHEN 'OpEx' THEN 3
            ELSE 4
        END
""")

print("📈 Year-over-Year Comparison:\n")
display(yoy_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Performance Indicators

# COMMAND ----------

kpis = spark.sql(f"""
    SELECT 
        metric_name,
        CASE WHEN bu_code = 'ALL' THEN 'Global' ELSE bu_code END as scope,
        metric_category,
        ROUND(metric_value, 2) as value,
        ROUND(target_value, 2) as target,
        ROUND(variance_from_target, 2) as variance
    FROM {TABLE_KPI_METRICS}
    WHERE closure_period = '{current_period}'
    ORDER BY 
        CASE WHEN bu_code = 'ALL' THEN 0 ELSE 1 END,
        bu_code,
        metric_name
""")

print("📊 Key Performance Indicators:\n")
display(kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Completeness

# COMMAND ----------

completeness = spark.sql(f"""
    WITH dataset_counts AS (
        SELECT 'Exchange Rates' as dataset, COUNT(*) as record_count
        FROM {TABLE_EXCHANGE_RATES}
        WHERE closure_period = '{current_period}' AND is_validated = true
        
        UNION ALL
        SELECT 'Preliminary Close', COUNT(*)
        FROM {TABLE_BU_PRELIMINARY}
        WHERE closure_period = '{current_period}' AND is_validated = true
        
        UNION ALL
        SELECT 'Accounting Cuts', COUNT(*)
        FROM {TABLE_ACCOUNTING_CUTS}
        WHERE closure_period = '{current_period}' AND is_validated = true
        
        UNION ALL
        SELECT 'Segmented Files', COUNT(*)
        FROM {TABLE_SEGMENTED_FILES}
        WHERE closure_period = '{current_period}' AND is_validated = true
        
        UNION ALL
        SELECT 'Forecast Files', COUNT(*)
        FROM {TABLE_FORECAST_FILES}
        WHERE closure_period = '{current_period}' AND is_validated = true
    )
    SELECT 
        dataset,
        record_count,
        CASE 
            WHEN record_count > 0 THEN '✅ Complete'
            ELSE '❌ Missing'
        END as status
    FROM dataset_counts
    ORDER BY dataset
""")

print("📋 Data Completeness:\n")
display(completeness)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Dashboard

# COMMAND ----------

# Get key metrics for display
try:
    metrics = spark.sql(f"""
        SELECT 
            SUM(CASE WHEN account_category = 'Revenue' THEN final_amount ELSE 0 END) as revenue,
            SUM(CASE WHEN account_category = 'COGS' THEN ABS(final_amount) ELSE 0 END) as cogs,
            SUM(CASE WHEN account_category = 'OpEx' THEN ABS(final_amount) ELSE 0 END) as opex
        FROM {TABLE_FINAL_RESULTS}
        WHERE closure_period = '{current_period}' AND is_final = true
    """).collect()[0]
    
    revenue = metrics['revenue'] or 0
    cogs = metrics['cogs'] or 0
    opex = metrics['opex'] or 0
    gross_profit = revenue - cogs
    gross_margin = (gross_profit / revenue * 100) if revenue != 0 else 0
    operating_income = gross_profit - opex
    operating_margin = (operating_income / revenue * 100) if revenue != 0 else 0
    
    print(f"""
{'='*60}
📊 EXECUTIVE DASHBOARD
{'='*60}

Period: {current_period}

💰 Financial Performance:
   • Revenue: ${revenue:,.2f}
   • Gross Profit: ${gross_profit:,.2f} ({gross_margin:.2f}%)
   • Operating Income: ${operating_income:,.2f} ({operating_margin:.2f}%)

📈 Status: {'✅ Complete' if revenue > 0 else '⏳ In Progress'}
{'='*60}
    """)
except Exception as e:
    print(f"⚠️  Final results not yet available: {str(e)}")
