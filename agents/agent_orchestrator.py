# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Closure Agent Orchestrator
# MAGIC Master orchestrator that coordinates all closure phases and agents

# COMMAND ----------

# MAGIC %run ../config/closure_config

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Orchestrator

# COMMAND ----------

class ClosureOrchestrator:
    """
    Master orchestrator for financial closure automation
    Coordinates all phase agents and manages closure lifecycle
    """
    
    def __init__(self, closure_period=None):
        self.closure_period = closure_period or get_current_closure_period()
        self.orchestrator_id = "agent-orchestrator-001"
        self.start_time = datetime.now()
        
        print(f"🤖 Closure Orchestrator initialized for period: {self.closure_period}")
    
    def log_activity(self, task_name, status, details="", records_processed=0):
        """Log orchestrator activity"""
        log_entry = spark.createDataFrame([{
            "log_id": str(uuid.uuid4()),
            "agent_id": self.orchestrator_id,
            "agent_name": "Closure Orchestrator",
            "closure_period": self.closure_period,
            "phase": 0,
            "task_name": task_name,
            "status": status,
            "started_at": self.start_time,
            "completed_at": datetime.now() if status in ["COMPLETED", "FAILED"] else None,
            "duration_seconds": (datetime.now() - self.start_time).total_seconds(),
            "records_processed": records_processed,
            "error_message": details if status == "FAILED" else None,
            "metadata": {"closure_period": self.closure_period}
        }])
        
        log_entry.write.format("delta").mode("append").saveAsTable(TABLE_AGENT_LOGS)
    
    def get_closure_status(self):
        """Get current closure status"""
        try:
            status_df = spark.sql(f"""
                SELECT current_phase, phase_status
                FROM {TABLE_CLOSURE_STATUS}
                WHERE closure_period = '{self.closure_period}'
                ORDER BY started_at DESC
                LIMIT 1
            """)
            
            if status_df.count() > 0:
                row = status_df.collect()[0]
                return row['current_phase'], row['phase_status']
            else:
                return 0, "NOT_STARTED"
        except:
            return 0, "NOT_STARTED"
    
    def initialize_closure(self):
        """Initialize a new closure cycle"""
        print(f"🚀 Initializing closure cycle for {self.closure_period}")
        
        # Create closure record
        closure_record = spark.createDataFrame([{
            "closure_id": str(uuid.uuid4()),
            "closure_period": self.closure_period,
            "current_phase": 1,
            "phase_status": "INITIALIZED",
            "started_at": datetime.now(),
            "expected_completion": datetime.now() + timedelta(days=7),
            "actual_completion": None,
            "created_by": "orchestrator",
            "updated_at": datetime.now(),
            "notes": "Closure cycle initialized by orchestrator"
        }])
        
        closure_record.write.format("delta").mode("append").saveAsTable(TABLE_CLOSURE_STATUS)
        
        self.log_activity("Initialize Closure", "COMPLETED", "Closure cycle initialized")
        print("✅ Closure cycle initialized")
    
    def execute_phase(self, phase_number):
        """Execute a specific closure phase"""
        phase_config = CLOSURE_PHASES.get(phase_number)
        if not phase_config:
            print(f"❌ Invalid phase number: {phase_number}")
            return False
        
        print(f"\n{'='*60}")
        print(f"📋 Phase {phase_number}: {phase_config['name']}")
        print(f"📝 {phase_config['description']}")
        print(f"⏱️  SLA: {phase_config['sla_hours']} hours")
        print(f"{'='*60}\n")
        
        phase_start = datetime.now()
        
        try:
            # Execute phase-specific logic
            if phase_number == 1:
                self.execute_phase_1()
            elif phase_number == 2:
                self.execute_phase_2()
            elif phase_number == 3:
                self.execute_phase_3()
            elif phase_number == 4:
                self.execute_phase_4()
            elif phase_number == 5:
                self.execute_phase_5()
            
            # Update closure status
            self.update_phase_status(phase_number, "COMPLETED")
            
            duration = (datetime.now() - phase_start).total_seconds() / 3600
            print(f"\n✅ Phase {phase_number} completed in {duration:.2f} hours")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Phase {phase_number} failed: {str(e)}")
            self.update_phase_status(phase_number, "FAILED")
            self.log_activity(f"Phase {phase_number}", "FAILED", str(e))
            return False
    
    def execute_phase_1(self):
        """Phase 1: Data Gathering - Exchange Rates & Preliminary Close"""
        print("🔄 Running Phase 1 agent...")
        dbutils.notebook.run("../agents/phase1_agent", timeout_seconds=3600, 
                            arguments={"closure_period": self.closure_period})
    
    def execute_phase_2(self):
        """Phase 2: Adjustments"""
        print("🔄 Running Phase 2 agent...")
        dbutils.notebook.run("../agents/phase2_agent", timeout_seconds=3600,
                            arguments={"closure_period": self.closure_period})
    
    def execute_phase_3(self):
        """Phase 3: Data Gathering - Segmented & Forecast"""
        print("🔄 Running Phase 3 agent...")
        dbutils.notebook.run("../agents/phase3_agent", timeout_seconds=3600,
                            arguments={"closure_period": self.closure_period})
    
    def execute_phase_4(self):
        """Phase 4: Review"""
        print("🔄 Running Phase 4 agent...")
        dbutils.notebook.run("../agents/phase4_agent", timeout_seconds=3600,
                            arguments={"closure_period": self.closure_period})
    
    def execute_phase_5(self):
        """Phase 5: Reporting & Sign-off"""
        print("🔄 Running Phase 5 agent...")
        dbutils.notebook.run("../agents/phase5_agent", timeout_seconds=3600,
                            arguments={"closure_period": self.closure_period})
    
    def update_phase_status(self, phase, status):
        """Update closure phase status"""
        closure_record = spark.createDataFrame([{
            "closure_id": str(uuid.uuid4()),
            "closure_period": self.closure_period,
            "current_phase": phase,
            "phase_status": status,
            "started_at": datetime.now(),
            "expected_completion": get_closure_deadline(phase),
            "actual_completion": datetime.now() if status == "COMPLETED" else None,
            "created_by": "orchestrator",
            "updated_at": datetime.now(),
            "notes": f"Phase {phase} {status}"
        }])
        
        closure_record.write.format("delta").mode("append").saveAsTable(TABLE_CLOSURE_STATUS)
    
    def run_full_cycle(self):
        """Execute complete closure cycle"""
        print(f"\n{'='*60}")
        print(f"🎯 Starting Full Closure Cycle")
        print(f"📅 Period: {self.closure_period}")
        print(f"{'='*60}\n")
        
        # Check if closure already exists
        current_phase, status = self.get_closure_status()
        
        if status == "NOT_STARTED":
            self.initialize_closure()
            current_phase = 1
        else:
            print(f"📍 Resuming from Phase {current_phase} (Status: {status})")
        
        # Execute phases sequentially
        for phase in range(current_phase, 6):
            success = self.execute_phase(phase)
            
            if not success:
                print(f"\n⚠️  Closure halted at Phase {phase}")
                return False
        
        # Closure complete
        print(f"\n{'='*60}")
        print(f"🎉 CLOSURE CYCLE COMPLETE!")
        print(f"📅 Period: {self.closure_period}")
        print(f"⏱️  Duration: {(datetime.now() - self.start_time).total_seconds() / 3600:.2f} hours")
        print(f"{'='*60}\n")
        
        self.log_activity("Full Closure Cycle", "COMPLETED", 
                         f"Completed in {(datetime.now() - self.start_time).total_seconds() / 3600:.2f} hours")
        
        return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Orchestrator

# COMMAND ----------

# Get parameters (if running as job)
dbutils.widgets.text("closure_period", get_current_closure_period(), "Closure Period")
dbutils.widgets.dropdown("mode", "full", ["full", "phase_1", "phase_2", "phase_3", "phase_4", "phase_5"], "Mode")

closure_period = dbutils.widgets.get("closure_period")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

# Create orchestrator
orchestrator = ClosureOrchestrator(closure_period)

# COMMAND ----------

# Execute based on mode
if mode == "full":
    orchestrator.run_full_cycle()
else:
    # Execute specific phase
    phase_num = int(mode.split("_")[1])
    orchestrator.execute_phase(phase_num)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Status

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        closure_period,
        current_phase,
        phase_status,
        started_at,
        actual_completion,
        ROUND(DATEDIFF(COALESCE(actual_completion, CURRENT_TIMESTAMP()), started_at), 2) as days_elapsed,
        notes
    FROM {TABLE_CLOSURE_STATUS}
    WHERE closure_period = '{closure_period}'
    ORDER BY started_at DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Agent Logs

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        agent_name,
        phase,
        task_name,
        status,
        started_at,
        ROUND(duration_seconds / 60, 2) as duration_minutes,
        records_processed,
        error_message
    FROM {TABLE_AGENT_LOGS}
    WHERE closure_period = '{closure_period}'
    ORDER BY started_at DESC
    LIMIT 50
"""))
