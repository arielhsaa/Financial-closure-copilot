# Quick Start Guide - Financial Closure Copilot

## Get Up and Running in 15 Minutes

This guide will help you quickly deploy and test the Financial Closure Copilot solution.

## Prerequisites
- Azure Databricks workspace (Premium/Enterprise tier)
- Unity Catalog enabled
- Workspace access with appropriate permissions

## 5-Step Quick Start

### Step 1: Upload Project (2 minutes)

**Using Databricks Repos (Recommended):**
1. Click "Repos" in the left sidebar
2. Click "Add Repo"
3. Enter the Git URL
4. Click "Create"

**Or upload manually:**
1. Download and extract the project
2. Navigate to Workspace in Databricks
3. Click "Import" → Select the project folder

### Step 2: Initialize Environment (3 minutes)

Run these notebooks in sequence:

```
1. /setup/00_initialize_environment     (Creates catalog and schemas)
2. /setup/01_create_tables              (Creates all Delta tables)
3. /setup/02_setup_agents               (Registers agents)
```

**What to expect:**
- ✅ Unity Catalog `financial_closure` created
- ✅ 4 schemas with 20+ tables created
- ✅ 8 agents registered

### Step 3: Generate Test Data (5 minutes)

Run these simulation notebooks:

```
1. /data_simulation/01_simulate_exchange_rates
2. /data_simulation/02_simulate_bu_preliminary
3. /data_simulation/03_simulate_accounting_cuts
4. /data_simulation/04_simulate_segmented_files
5. /data_simulation/05_simulate_forecast_files
```

**What to expect:**
- ~2,000 simulated financial records created
- Data across all closure phases

### Step 4: Run Closure Cycle (5 minutes)

Execute the orchestrator:

```
/agents/agent_orchestrator
```

**Parameters:**
- `closure_period`: Leave default (current period)
- `mode`: Select "full"

**What to expect:**
- All 5 phases execute automatically
- Final results published
- Closure cycle complete

### Step 5: View Results (2 minutes)

Check your results:

1. **View final results:**
   ```sql
   SELECT * FROM financial_closure.reporting.final_results
   WHERE is_final = true
   ORDER BY bu_name, account_category
   ```

2. **View agent performance:**
   Run `/monitoring/agent_monitoring`

3. **View financial metrics:**
   Run `/monitoring/closure_metrics`

## 🎉 Success!

You've successfully deployed and run a complete financial closure cycle!

## What Just Happened?

The solution automatically:
- ✅ Collected and validated exchange rates
- ✅ Processed preliminary close files from 4 business units
- ✅ Applied accounting adjustments
- ✅ Processed segmented financial data
- ✅ Incorporated forecast data
- ✅ Generated final reports with KPIs

## Next Steps

### Setup Genie Dashboards (Optional - 5 minutes)

1. Create a Genie Space:
   - Navigate to "Genie" in Databricks
   - Click "Create Space"
   - Name it "Financial Closure Dashboard"

2. Run the SQL definitions:
   ```
   /reporting/genie_dashboard_definitions.sql
   ```

3. Try natural language queries:
   - "What's the current closure status?"
   - "Show me revenue by business unit"
   - "What are the top adjustments?"

### Customize for Your Organization

Edit the configuration:

```
/config/business_units.json       (Add your business units)
/config/closure_config.py         (Adjust settings)
```

### Schedule Automation

Create a monthly workflow:
```
/workflows/setup_workflows.py     (Follow instructions)
```

## Common Commands

**Check closure status:**
```sql
SELECT * FROM financial_closure.reporting.closure_status
ORDER BY started_at DESC
LIMIT 5
```

**View agent logs:**
```sql
SELECT * FROM financial_closure.audit.agent_execution_logs
ORDER BY started_at DESC
LIMIT 20
```

**Check data quality:**
```sql
SELECT * FROM financial_closure.audit.data_quality_checks
WHERE check_result IN ('WARNING', 'FAILED')
```

## Troubleshooting

**Issue: Tables not created**
- Verify Unity Catalog is enabled
- Check you have CREATE permissions

**Issue: Agent execution failed**
- Check agent_execution_logs table
- Review error messages in notebook output

**Issue: No data in final results**
- Verify simulation notebooks completed
- Check that orchestrator ran successfully

## Getting Help

- 📖 Full documentation: `README.md`
- 🚀 Detailed deployment: `DEPLOYMENT_GUIDE.md`
- 💡 Check agent monitoring dashboards
- 🔍 Review agent execution logs

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│         Financial Closure Copilot                │
├─────────────────────────────────────────────────┤
│  Phase 1: Exchange Rates & Preliminary Close    │
│  Phase 2: Adjustments & Review                  │
│  Phase 3: Segmented & Forecast Data             │
│  Phase 4: Review & Validation                   │
│  Phase 5: Final Reporting & Sign-off            │
└─────────────────────────────────────────────────┘
           ↓
┌─────────────────────────────────────────────────┐
│         Unity Catalog (Delta Lake)               │
│  • Raw Data      • Processed Data                │
│  • Reporting     • Audit Trails                  │
└─────────────────────────────────────────────────┘
           ↓
┌─────────────────────────────────────────────────┐
│              Genie Dashboards                    │
│  • Natural Language Queries                      │
│  • Real-time Analytics                           │
│  • Interactive Reports                           │
└─────────────────────────────────────────────────┘
```

## Key Features You Just Experienced

1. **Intelligent Agents**: Autonomous task execution across 5 phases
2. **Data Quality**: Automatic validation and issue detection
3. **Audit Trail**: Complete logging of all operations
4. **Scalability**: Handles millions of records with Delta Lake
5. **Natural Language**: Query results with Genie

## Production Readiness

To go production:
1. ✅ Replace simulated data with real sources
2. ✅ Customize business unit configuration
3. ✅ Set up automated workflows
4. ✅ Configure alerts and monitoring
5. ✅ Establish data governance policies

## Performance Metrics

With test data:
- **Setup Time**: ~15 minutes
- **Execution Time**: ~5 minutes
- **Records Processed**: ~2,000
- **Phases Automated**: 5/5
- **Agents Active**: 8/8

## What Makes This Special?

This is not just automation - it's intelligent automation:

- 🤖 **Agents** that make decisions
- 🔍 **Self-healing** data validation
- 📊 **Real-time** monitoring
- 🎯 **Proactive** issue detection
- 💬 **Natural language** analytics

## Congratulations! 🎉

You've successfully deployed an enterprise-grade, intelligent financial closure solution in just 15 minutes!

---

**Ready for more?** Check out the full `DEPLOYMENT_GUIDE.md` for production deployment.

**Questions?** Review the `README.md` for detailed architecture and capabilities.
