-- Databricks Genie Space Dashboard Definitions
-- Financial Closure Copilot
-- Execute these queries in your Genie Space to enable natural language queries

-- =============================================================================
-- 1. CLOSURE STATUS OVERVIEW
-- =============================================================================
-- Purpose: Real-time view of closure cycle progress
-- Genie Query: "What's the current status of the closure cycle?"

CREATE OR REPLACE VIEW financial_closure.reporting.v_closure_status_overview AS
SELECT 
    cs.closure_period,
    cs.current_phase,
    CASE cs.current_phase
        WHEN 1 THEN 'Data Gathering - Exchange Rates'
        WHEN 2 THEN 'Adjustments'
        WHEN 3 THEN 'Data Gathering - Segmented & Forecast'
        WHEN 4 THEN 'Review'
        WHEN 5 THEN 'Reporting & Sign-off'
        ELSE 'Unknown'
    END as phase_name,
    cs.phase_status,
    cs.started_at,
    cs.expected_completion,
    cs.actual_completion,
    CASE 
        WHEN cs.actual_completion IS NOT NULL THEN 'Completed'
        WHEN cs.expected_completion < CURRENT_TIMESTAMP() THEN 'Overdue'
        ELSE 'In Progress'
    END as status,
    DATEDIFF(COALESCE(cs.actual_completion, CURRENT_TIMESTAMP()), cs.started_at) as days_elapsed,
    cs.notes
FROM financial_closure.reporting.closure_status cs
WHERE cs.closure_period = (SELECT MAX(closure_period) FROM financial_closure.reporting.closure_status)
ORDER BY cs.started_at DESC
LIMIT 1;

-- =============================================================================
-- 2. EXCHANGE RATE MONITORING
-- =============================================================================
-- Purpose: Monitor exchange rates and variances
-- Genie Query: "Show me exchange rate variances this month"

CREATE OR REPLACE VIEW financial_closure.reporting.v_exchange_rate_monitor AS
SELECT 
    er.currency,
    er.rate_date,
    er.rate,
    er.variance_from_prior,
    er.is_validated,
    er.validation_notes,
    CASE 
        WHEN ABS(er.variance_from_prior) > 5 THEN 'High Variance'
        WHEN ABS(er.variance_from_prior) > 2 THEN 'Medium Variance'
        ELSE 'Normal'
    END as variance_level,
    er.source,
    er.closure_period
FROM financial_closure.processed_data.exchange_rates er
WHERE er.rate_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY er.rate_date DESC, ABS(er.variance_from_prior) DESC;

-- =============================================================================
-- 3. BU SUBMISSION STATUS
-- =============================================================================
-- Purpose: Track which BUs have submitted their files
-- Genie Query: "Which business units haven't submitted their files?"

CREATE OR REPLACE VIEW financial_closure.reporting.v_bu_submission_status AS
WITH bu_list AS (
    SELECT 'NA' as bu_code, 'North America' as bu_name
    UNION ALL SELECT 'EMEA', 'Europe, Middle East, Africa'
    UNION ALL SELECT 'APAC', 'Asia Pacific'
    UNION ALL SELECT 'LATAM', 'Latin America'
),
current_period AS (
    SELECT MAX(closure_period) as period 
    FROM financial_closure.reporting.closure_status
),
preliminary_status AS (
    SELECT DISTINCT bu_code, 'Preliminary Close' as file_type, TRUE as submitted
    FROM financial_closure.processed_data.bu_preliminary_close
    WHERE closure_period = (SELECT period FROM current_period)
),
segmented_status AS (
    SELECT DISTINCT bu_code, 'Segmented Files' as file_type, TRUE as submitted
    FROM financial_closure.processed_data.segmented_files
    WHERE closure_period = (SELECT period FROM current_period)
),
forecast_status AS (
    SELECT DISTINCT bu_code, 'Forecast Files' as file_type, TRUE as submitted
    FROM financial_closure.processed_data.forecast_files
    WHERE closure_period = (SELECT period FROM current_period)
)
SELECT 
    bl.bu_code,
    bl.bu_name,
    COALESCE(ps.submitted, FALSE) as preliminary_submitted,
    COALESCE(ss.submitted, FALSE) as segmented_submitted,
    COALESCE(fs.submitted, FALSE) as forecast_submitted,
    CASE 
        WHEN COALESCE(ps.submitted, FALSE) 
         AND COALESCE(ss.submitted, FALSE) 
         AND COALESCE(fs.submitted, FALSE) THEN 'Complete'
        WHEN COALESCE(ps.submitted, FALSE) THEN 'Partial'
        ELSE 'Pending'
    END as overall_status
FROM bu_list bl
LEFT JOIN preliminary_status ps ON bl.bu_code = ps.bu_code
LEFT JOIN segmented_status ss ON bl.bu_code = ss.bu_code
LEFT JOIN forecast_status fs ON bl.bu_code = fs.bu_code
ORDER BY overall_status, bl.bu_code;

-- =============================================================================
-- 4. TOP ADJUSTMENTS
-- =============================================================================
-- Purpose: View largest accounting adjustments
-- Genie Query: "What are the top 10 adjustments by amount?"

CREATE OR REPLACE VIEW financial_closure.reporting.v_top_adjustments AS
SELECT 
    ac.bu_code,
    ac.account_code,
    ac.adjustment_type,
    ac.cut_number,
    ABS(ac.usd_amount) as adjustment_amount,
    ac.usd_amount as signed_amount,
    ac.description,
    ac.approved_by,
    ac.approved_at,
    ac.closure_period
FROM financial_closure.processed_data.accounting_cuts ac
WHERE ac.is_validated = TRUE
  AND ac.closure_period = (
      SELECT MAX(closure_period) 
      FROM financial_closure.processed_data.accounting_cuts
  )
ORDER BY ABS(ac.usd_amount) DESC
LIMIT 20;

-- =============================================================================
-- 5. PRELIMINARY RESULTS SUMMARY
-- =============================================================================
-- Purpose: View preliminary close results by BU and category
-- Genie Query: "Show me preliminary results by business unit"

CREATE OR REPLACE VIEW financial_closure.reporting.v_preliminary_summary AS
SELECT 
    pr.closure_period,
    pr.bu_name,
    pr.account_category,
    SUM(pr.preliminary_amount) as total_amount,
    SUM(pr.prior_period_amount) as prior_period,
    SUM(pr.variance_amount) as variance,
    CASE 
        WHEN SUM(pr.prior_period_amount) != 0 
        THEN (SUM(pr.variance_amount) / ABS(SUM(pr.prior_period_amount))) * 100
        ELSE 0
    END as variance_percent,
    COUNT(*) as account_count
FROM financial_closure.reporting.preliminary_results pr
WHERE pr.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.reporting.preliminary_results
)
GROUP BY pr.closure_period, pr.bu_name, pr.account_category
ORDER BY pr.bu_name, 
         CASE pr.account_category
             WHEN 'Revenue' THEN 1
             WHEN 'COGS' THEN 2
             WHEN 'OpEx' THEN 3
             ELSE 4
         END;

-- =============================================================================
-- 6. FINAL RESULTS WITH SEGMENTS
-- =============================================================================
-- Purpose: View final results with all segment dimensions
-- Genie Query: "Show me final results by product and geography"

CREATE OR REPLACE VIEW financial_closure.reporting.v_final_results_segmented AS
SELECT 
    fr.closure_period,
    fr.bu_name,
    fr.region,
    fr.account_category,
    fr.segment_product,
    fr.segment_geography,
    fr.segment_customer,
    fr.segment_channel,
    SUM(fr.final_amount) as final_amount,
    SUM(fr.forecast_amount) as forecast_amount,
    SUM(fr.forecast_variance) as vs_forecast,
    SUM(fr.prior_period_amount) as prior_year,
    SUM(fr.yoy_variance) as yoy_variance,
    CASE 
        WHEN SUM(fr.prior_period_amount) != 0 
        THEN (SUM(fr.yoy_variance) / ABS(SUM(fr.prior_period_amount))) * 100
        ELSE 0
    END as yoy_variance_percent
FROM financial_closure.reporting.final_results fr
WHERE fr.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.reporting.final_results
)
  AND fr.is_final = TRUE
GROUP BY 
    fr.closure_period, fr.bu_name, fr.region, fr.account_category,
    fr.segment_product, fr.segment_geography, fr.segment_customer, fr.segment_channel;

-- =============================================================================
-- 7. FORECAST VS ACTUAL COMPARISON
-- =============================================================================
-- Purpose: Compare forecast to actual results
-- Genie Query: "Compare forecast vs actual by segment"

CREATE OR REPLACE VIEW financial_closure.reporting.v_forecast_vs_actual AS
SELECT 
    fr.bu_name,
    fr.account_category,
    fr.segment_product,
    SUM(fr.final_amount) as actual_amount,
    SUM(fr.forecast_amount) as forecast_amount,
    SUM(fr.forecast_variance) as variance,
    CASE 
        WHEN SUM(fr.forecast_amount) != 0 
        THEN (SUM(fr.forecast_variance) / ABS(SUM(fr.forecast_amount))) * 100
        ELSE 0
    END as variance_percent,
    CASE 
        WHEN ABS((SUM(fr.forecast_variance) / NULLIF(ABS(SUM(fr.forecast_amount)), 0)) * 100) <= 5 THEN 'On Target'
        WHEN ABS((SUM(fr.forecast_variance) / NULLIF(ABS(SUM(fr.forecast_amount)), 0)) * 100) <= 10 THEN 'Minor Variance'
        ELSE 'Significant Variance'
    END as accuracy_rating
FROM financial_closure.reporting.final_results fr
WHERE fr.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.reporting.final_results
)
  AND fr.is_final = TRUE
  AND fr.forecast_amount IS NOT NULL
GROUP BY fr.bu_name, fr.account_category, fr.segment_product
ORDER BY ABS(SUM(fr.forecast_variance)) DESC;

-- =============================================================================
-- 8. KPI METRICS DASHBOARD
-- =============================================================================
-- Purpose: Display key performance indicators
-- Genie Query: "Show me the key financial metrics"

CREATE OR REPLACE VIEW financial_closure.reporting.v_kpi_dashboard AS
SELECT 
    km.closure_period,
    km.metric_name,
    km.metric_category,
    CASE WHEN km.bu_code = 'ALL' THEN 'Global' ELSE km.bu_code END as business_unit,
    km.region,
    ROUND(km.metric_value, 2) as metric_value,
    ROUND(km.target_value, 2) as target_value,
    ROUND(km.variance_from_target, 2) as variance_from_target,
    CASE 
        WHEN km.target_value IS NOT NULL 
         AND ABS(km.variance_from_target / NULLIF(km.target_value, 0) * 100) <= 5 THEN '✅ On Target'
        WHEN km.target_value IS NOT NULL 
         AND ABS(km.variance_from_target / NULLIF(km.target_value, 0) * 100) <= 10 THEN '⚠️  Minor Miss'
        WHEN km.target_value IS NOT NULL THEN '❌ Significant Miss'
        ELSE 'No Target'
    END as performance_status
FROM financial_closure.reporting.kpi_metrics km
WHERE km.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.reporting.kpi_metrics
)
ORDER BY 
    CASE WHEN km.bu_code = 'ALL' THEN 0 ELSE 1 END,
    km.bu_code,
    km.metric_category,
    km.metric_name;

-- =============================================================================
-- 9. DATA QUALITY DASHBOARD
-- =============================================================================
-- Purpose: Monitor data quality across all phases
-- Genie Query: "What's the data quality status?"

CREATE OR REPLACE VIEW financial_closure.reporting.v_data_quality_dashboard AS
SELECT 
    dq.closure_period,
    dq.phase,
    CASE dq.phase
        WHEN 1 THEN 'Data Gathering - Exchange Rates'
        WHEN 2 THEN 'Adjustments'
        WHEN 3 THEN 'Data Gathering - Segmented & Forecast'
        WHEN 4 THEN 'Review'
        WHEN 5 THEN 'Reporting & Sign-off'
        ELSE 'Unknown'
    END as phase_name,
    dq.table_name,
    dq.check_name,
    dq.check_result,
    dq.records_checked,
    dq.records_failed,
    ROUND(dq.failure_rate * 100, 2) as failure_rate_percent,
    CASE 
        WHEN dq.check_result = 'PASSED' THEN '✅'
        WHEN dq.check_result = 'WARNING' THEN '⚠️ '
        ELSE '❌'
    END as status_icon,
    dq.details,
    dq.checked_at
FROM financial_closure.audit.data_quality_checks dq
WHERE dq.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.audit.data_quality_checks
)
ORDER BY dq.phase, dq.checked_at DESC;

-- =============================================================================
-- 10. AGENT ACTIVITY MONITOR
-- =============================================================================
-- Purpose: Monitor agent executions and performance
-- Genie Query: "How are the agents performing?"

CREATE OR REPLACE VIEW financial_closure.reporting.v_agent_activity AS
SELECT 
    al.agent_name,
    al.phase,
    al.task_name,
    al.status,
    al.started_at,
    al.completed_at,
    ROUND(al.duration_seconds, 2) as duration_seconds,
    al.records_processed,
    CASE 
        WHEN al.status = 'COMPLETED' THEN '✅'
        WHEN al.status = 'RUNNING' THEN '🔄'
        WHEN al.status = 'FAILED' THEN '❌'
        ELSE '⏸️ '
    END as status_icon,
    al.error_message,
    al.closure_period
FROM financial_closure.audit.agent_execution_logs al
WHERE al.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.audit.agent_execution_logs
)
ORDER BY al.started_at DESC
LIMIT 50;

-- =============================================================================
-- 11. HISTORICAL TREND ANALYSIS
-- =============================================================================
-- Purpose: Track financial metrics over time
-- Genie Query: "Show me revenue trends over the last 12 months"

CREATE OR REPLACE VIEW financial_closure.reporting.v_historical_trends AS
SELECT 
    fr.closure_period,
    DATE_TRUNC('MONTH', TO_DATE(fr.closure_period, 'yyyy-MM')) as period_date,
    fr.bu_name,
    SUM(CASE WHEN fr.account_category = 'Revenue' THEN fr.final_amount ELSE 0 END) as revenue,
    ABS(SUM(CASE WHEN fr.account_category = 'COGS' THEN fr.final_amount ELSE 0 END)) as cogs,
    ABS(SUM(CASE WHEN fr.account_category = 'OpEx' THEN fr.final_amount ELSE 0 END)) as opex,
    SUM(CASE WHEN fr.account_category = 'Revenue' THEN fr.final_amount ELSE 0 END) -
    ABS(SUM(CASE WHEN fr.account_category = 'COGS' THEN fr.final_amount ELSE 0 END)) as gross_profit,
    (SUM(CASE WHEN fr.account_category = 'Revenue' THEN fr.final_amount ELSE 0 END) -
     ABS(SUM(CASE WHEN fr.account_category = 'COGS' THEN fr.final_amount ELSE 0 END))) /
     NULLIF(SUM(CASE WHEN fr.account_category = 'Revenue' THEN fr.final_amount ELSE 0 END), 0) * 100 as gross_margin_pct
FROM financial_closure.reporting.final_results fr
WHERE fr.is_final = TRUE
GROUP BY fr.closure_period, fr.bu_name
ORDER BY fr.closure_period DESC, fr.bu_name;

-- =============================================================================
-- 12. VARIANCE ANALYSIS
-- =============================================================================
-- Purpose: Deep dive into significant variances
-- Genie Query: "Show me the biggest variances from last month"

CREATE OR REPLACE VIEW financial_closure.reporting.v_variance_analysis AS
SELECT 
    fr.bu_name,
    fr.account_description,
    fr.account_category,
    fr.segment_product,
    fr.final_amount as current_period,
    fr.prior_period_amount as prior_period,
    fr.yoy_variance as variance_amount,
    fr.yoy_variance_percent as variance_percent,
    CASE 
        WHEN ABS(fr.yoy_variance_percent) > 50 THEN 'Extreme'
        WHEN ABS(fr.yoy_variance_percent) > 20 THEN 'High'
        WHEN ABS(fr.yoy_variance_percent) > 10 THEN 'Medium'
        ELSE 'Normal'
    END as variance_severity,
    fr.closure_period
FROM financial_closure.reporting.final_results fr
WHERE fr.closure_period = (
    SELECT MAX(closure_period) 
    FROM financial_closure.reporting.final_results
)
  AND fr.is_final = TRUE
  AND ABS(fr.yoy_variance) > 10000  -- Only show significant variances
ORDER BY ABS(fr.yoy_variance) DESC
LIMIT 50;

-- =============================================================================
-- GENIE SPACE CONFIGURATION
-- =============================================================================
-- After creating these views, configure your Genie Space with:
-- 1. Add all views as data sources
-- 2. Configure natural language aliases for common queries
-- 3. Set up automatic refresh schedules
-- 4. Configure user permissions

-- Example Natural Language Queries that Genie will understand:
-- - "What's the current closure status?"
-- - "Show me top adjustments"
-- - "Which BUs haven't submitted files?"
-- - "Compare forecast vs actual"
-- - "What are the revenue trends?"
-- - "Show me data quality issues"
-- - "How are agents performing?"
-- - "What are the biggest variances?"
