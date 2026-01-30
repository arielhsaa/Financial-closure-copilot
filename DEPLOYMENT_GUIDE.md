# Financial Closure Copilot - Deployment Guide

## Overview
Complete deployment guide for the intelligent financial closure agentic solution on Azure Databricks.

## Prerequisites

### Azure Databricks Requirements
- **Workspace**: Azure Databricks Premium or Enterprise tier
- **Unity Catalog**: Enabled and configured
- **SQL Warehouse**: Serverless SQL Warehouse (recommended)
- **Genie**: Genie Spaces enabled
- **Compute**: Access to create job clusters
- **Permissions**: Workspace admin or sufficient privileges

### User Requirements
- Databricks account with appropriate permissions
- Azure subscription with Databricks workspace
- Basic SQL and Python knowledge for customization

## Deployment Steps

### Step 1: Setup Databricks Workspace

1. **Access your Databricks workspace**
   ```
   https://adb-<workspace-id>.<region>.azuredatabricks.net
   ```

2. **Verify Unity Catalog is enabled**
   - Navigate to Data → Unity Catalog
   - Confirm you have a metastore attached

3. **Verify Genie Spaces is available**
   - Navigate to Genie
   - Confirm the feature is enabled

### Step 2: Upload Project Files

#### Option A: Using Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload project
databricks workspace import_dir \
  /path/to/Financial-closure-copilot \
  /Workspace/Financial-closure-copilot \
  --overwrite
```

#### Option B: Using Databricks Repos (Recommended)

1. Navigate to Repos in your workspace
2. Click "Add Repo"
3. Enter your Git repository URL
4. Clone the repository

#### Option C: Manual Upload

1. Navigate to Workspace in Databricks UI
2. Create folder: `Financial-closure-copilot`
3. Upload all notebooks and files maintaining the directory structure

### Step 3: Initialize Environment

1. **Open and run the initialization notebook:**
   ```
   /Workspace/Financial-closure-copilot/setup/00_initialize_environment
   ```

2. **Expected actions:**
   - Creates Unity Catalog: `financial_closure`
   - Creates schemas: `raw_data`, `processed_data`, `reporting`, `audit`
   - Creates volumes for file storage (if supported)
   - Sets up base permissions

3. **Verify completion:**
   - Check for success messages
   - Verify catalog and schemas exist in Data Explorer

### Step 4: Create Tables

1. **Run the table creation notebook:**
   ```
   /Workspace/Financial-closure-copilot/setup/01_create_tables
   ```

2. **Expected actions:**
   - Creates all Delta tables (20+ tables)
   - Optimizes and Z-orders key tables
   - Verifies table creation

3. **Verify completion:**
   - Navigate to Data → Unity Catalog
   - Confirm all tables exist in respective schemas

### Step 5: Setup Agents

1. **Run the agent setup notebook:**
   ```
   /Workspace/Financial-closure-copilot/setup/02_setup_agents
   ```

2. **Expected actions:**
   - Registers all closure agents
   - Creates agent registry
   - Sets up agent communication tables
   - Defines agent capabilities

3. **Verify completion:**
   - Check agent registry table
   - Confirm 8 agents registered

### Step 6: Test with Simulated Data

1. **Run data simulation notebooks in sequence:**
   ```
   /Workspace/Financial-closure-copilot/data_simulation/01_simulate_exchange_rates
   /Workspace/Financial-closure-copilot/data_simulation/02_simulate_bu_preliminary
   /Workspace/Financial-closure-copilot/data_simulation/03_simulate_accounting_cuts
   /Workspace/Financial-closure-copilot/data_simulation/04_simulate_segmented_files
   /Workspace/Financial-closure-copilot/data_simulation/05_simulate_forecast_files
   ```

2. **Expected results:**
   - Exchange rates: ~810 records
   - Preliminary close: ~400 records
   - Accounting cuts: ~40 records
   - Segmented files: ~10,000 records
   - Forecast files: ~384 records

3. **Verify data:**
   - Query raw tables in Data Explorer
   - Check record counts match expected values

### Step 7: Run Full Closure Cycle

1. **Execute the orchestrator:**
   ```
   /Workspace/Financial-closure-copilot/agents/agent_orchestrator
   ```

2. **Set parameters:**
   - `closure_period`: Current period (YYYY-MM)
   - `mode`: "full"

3. **Expected duration:**
   - Test environment: 15-30 minutes
   - Production: 1-4 hours (depending on data volume)

4. **Monitor execution:**
   - Watch notebook output for progress
   - Check agent logs table for detailed status

### Step 8: Setup Genie Dashboards

1. **Create a new Genie Space:**
   - Navigate to Genie in Databricks
   - Click "Create Space"
   - Name: "Financial Closure Dashboard"

2. **Execute SQL definitions:**
   ```
   /Workspace/Financial-closure-copilot/reporting/genie_dashboard_definitions.sql
   ```

3. **Add views to Genie Space:**
   - Select all created views
   - Add them as data sources to your Genie Space

4. **Test natural language queries:**
   - "What's the current closure status?"
   - "Show me revenue by business unit"
   - "What are the top 10 adjustments?"

### Step 9: Setup Workflows (Production)

1. **Review workflow definition:**
   ```
   /Workspace/Financial-closure-copilot/workflows/closure_workflow_definition.json
   ```

2. **Create workflow via UI:**
   - Navigate to Workflows → Create Job
   - Use the provided JSON as reference
   - Configure cluster settings
   - Set up schedule (optional)

3. **Or use Databricks CLI:**
   ```bash
   databricks jobs create --json-file closure_workflow_definition.json
   ```

4. **Configure schedule:**
   - Cron: `0 0 1 * * ?` (1st of every month)
   - Timezone: UTC (or your preference)

### Step 10: Configure Monitoring

1. **Setup monitoring notebooks:**
   ```
   /Workspace/Financial-closure-copilot/monitoring/agent_monitoring
   /Workspace/Financial-closure-copilot/monitoring/closure_metrics
   ```

2. **Create alerts (optional):**
   - Configure SQL Alert for failed agents
   - Configure SQL Alert for data quality issues
   - Configure SQL Alert for overdue phases

## Customization

### Business Units

Edit `/config/business_units.json`:

```json
{
  "business_units": {
    "YOUR_BU": {
      "name": "Your Business Unit Name",
      "region": "Your Region",
      "currency": "USD",
      "contact": "your.email@company.com",
      "reporting_deadline_days": 2
    }
  }
}
```

### Accounts and Segments

Edit `/config/closure_config.py`:

- Add/modify account structures
- Update segment dimensions
- Adjust data quality rules
- Configure thresholds

### Validation Rules

Edit processing notebooks to add custom validation logic:

```python
# Example: Add custom validation
df_validated = df.withColumn(
    "custom_validation",
    when(col("amount") > 1000000, lit("Requires approval"))
)
```

## Production Readiness Checklist

### Security
- [ ] Configure Unity Catalog permissions
- [ ] Set up row-level security on sensitive tables
- [ ] Enable audit logging
- [ ] Configure data masking if needed
- [ ] Review and restrict notebook access

### Performance
- [ ] Optimize table partitioning for large datasets
- [ ] Configure appropriate cluster sizes
- [ ] Enable Photon acceleration
- [ ] Set up caching for frequently accessed data
- [ ] Monitor query performance

### Reliability
- [ ] Configure automated backups
- [ ] Set up disaster recovery procedures
- [ ] Implement retry logic for external calls
- [ ] Configure timeout parameters
- [ ] Set up health checks

### Monitoring
- [ ] Configure SQL alerts for failures
- [ ] Set up notification channels
- [ ] Create monitoring dashboards
- [ ] Enable log aggregation
- [ ] Set up performance tracking

### Data Integration
- [ ] Replace simulated data with real data sources
- [ ] Configure file upload locations
- [ ] Set up data pipelines from source systems
- [ ] Implement data quality checks at ingestion
- [ ] Configure data retention policies

## Integration with Source Systems

### Exchange Rate Integration

```python
# Example: Read from external API or file system
df_fx = spark.read \
    .option("header", "true") \
    .csv("/mnt/finance/exchange_rates/*.csv")

# Write to raw table
df_fx.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_EXCHANGE_RATES_RAW)
```

### BU Data Integration

```python
# Example: Read from mounted storage
df_bu = spark.read \
    .option("header", "true") \
    .csv(f"/mnt/finance/bu_files/{bu_code}/*.csv")

# Process and write
df_bu.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TABLE_BU_PRELIMINARY_RAW)
```

## Troubleshooting

### Common Issues

**Issue: Unity Catalog not accessible**
- Solution: Verify metastore is attached to workspace
- Check: Data → Unity Catalog → Metastore settings

**Issue: Permissions denied**
- Solution: Grant appropriate catalog/schema permissions
- Check: Run `GRANT USE CATALOG ON CATALOG financial_closure TO users`

**Issue: Agents failing**
- Solution: Check agent logs table for error messages
- Check: Run monitoring notebooks to identify issues

**Issue: Genie queries not working**
- Solution: Verify views are created and accessible
- Check: Query views directly in SQL Editor first

**Issue: Workflow not triggering**
- Solution: Verify schedule is enabled (not paused)
- Check: Workflows UI → Job details → Schedule status

### Debug Mode

Enable verbose logging by adding to agent notebooks:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Support Resources

- Databricks Documentation: https://docs.databricks.com
- Unity Catalog Guide: https://docs.databricks.com/data-governance/unity-catalog/
- Genie Documentation: https://docs.databricks.com/genie/
- Community Forums: https://community.databricks.com

## Maintenance

### Regular Tasks

**Weekly:**
- Review agent execution logs
- Check data quality metrics
- Monitor workflow performance

**Monthly:**
- Archive completed closure cycles
- Review and optimize slow queries
- Update business unit configurations

**Quarterly:**
- Review and update validation rules
- Audit security permissions
- Performance tuning and optimization

### Upgrades

To upgrade to new version:

1. Backup current configuration
2. Pull latest code from repository
3. Review changelog for breaking changes
4. Test in development environment
5. Deploy to production during maintenance window

## Best Practices

1. **Always test with simulated data first**
2. **Monitor agent health regularly**
3. **Review data quality reports daily during closure**
4. **Keep business unit configuration up to date**
5. **Archive completed closures for compliance**
6. **Document any customizations**
7. **Maintain change log**

## Getting Help

For assistance:
1. Check this deployment guide
2. Review README.md for architecture
3. Check Databricks documentation
4. Review agent logs for specific errors
5. Contact your Databricks support team

## Success Criteria

Deployment is successful when:
- ✅ All tables created in Unity Catalog
- ✅ Agents registered and configured
- ✅ Full closure cycle completes successfully
- ✅ Genie dashboards showing data
- ✅ Workflows scheduled and running
- ✅ Monitoring dashboards operational

## Next Steps After Deployment

1. Replace simulated data with real data sources
2. Customize business rules and validations
3. Train users on Genie dashboard usage
4. Set up production monitoring and alerts
5. Document any custom configurations
6. Schedule regular review meetings

---

**Congratulations!** Your Financial Closure Copilot is now deployed and operational.

For questions or issues, please refer to the project README or contact your Databricks administrator.
