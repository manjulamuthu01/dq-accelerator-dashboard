"""
Automated Test Suite for Phase 7: Scheduling and Automation
Validates all Phase 7 requirements:
1. Scheduled and manual execution.
2. Filtered execution by project and dataset.
3. Integration with the generic orchestrator.
4. History tables persistence.
5. Tracking of status, execution ID, duration, and failure details.
6. Transient error handling and retry readiness.
7. Dashboard status and history retrieval.
8. Deduplication of execution records.
"""

import unittest
import os
import time
import pandas as pd
import db_adapter
import dash_app

class TestPhase7(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db_adapter._init_local_db()

    def test_01_job_runs_table_schema(self):
        """Req 4 & 5: Verify dq_job_runs table schema tracks all necessary execution metadata."""
        df = db_adapter.query_df("PRAGMA table_info(dq_job_runs)")
        cols = df["name"].tolist()
        expected_cols = [
            "run_id", "job_id", "execution_id", "run_type",
            "project_name", "dataset_name", "status",
            "started_at", "ended_at", "duration_ms", "error_details"
        ]
        for col in expected_cols:
            self.assertIn(col, cols, f"Column {col} missing in dq_job_runs table")

    def test_02_manual_and_scheduled_execution_support(self):
        """Req 1: Support both manual and scheduled execution triggers."""
        # Test manual trigger
        res_manual = db_adapter.trigger_validation(project_name="Hospital", dataset_name="source_table", run_type="MANUAL")
        self.assertEqual(res_manual["run_type"], "MANUAL")
        self.assertTrue(res_manual["execution_id"].startswith("EXEC-"))

        # Test scheduled trigger
        res_sched = db_adapter.trigger_validation(project_name="Banking", dataset_name="customer", run_type="SCHEDULED")
        self.assertEqual(res_sched["run_type"], "SCHEDULED")
        self.assertTrue(res_sched["execution_id"].startswith("EXEC-"))

        # Verify both recorded in dq_job_runs
        runs = db_adapter.query_df("SELECT run_type FROM dq_job_runs WHERE execution_id IN (?, ?)",
                                  (res_manual["execution_id"], res_sched["execution_id"]))
        types = runs["run_type"].tolist()
        self.assertIn("MANUAL", types)
        self.assertIn("SCHEDULED", types)

    def test_03_scoped_execution_filtering(self):
        """Req 2: Allow execution for selected projects and datasets."""
        # Scoped to Hospital / source_table
        res = db_adapter.trigger_validation(project_name="Hospital", dataset_name="source_table")
        exec_id = res["execution_id"]

        # Verify summary row has the exact project and dataset
        sum_row = db_adapter.query_df("SELECT project_name, dataset_name FROM dq_execution_summary WHERE execution_id = ?", (exec_id,))
        self.assertFalse(sum_row.empty)
        self.assertEqual(sum_row.iloc[0]["project_name"], "Hospital")
        self.assertEqual(sum_row.iloc[0]["dataset_name"], "source_table")

    def test_04_execution_history_persistence(self):
        """Req 4: Ensure every execution is stored in history tables."""
        res = db_adapter.trigger_validation("Logistics", "shipment")
        exec_id = res["execution_id"]

        # Check summary table
        s_df = db_adapter.query_df("SELECT execution_id FROM dq_execution_summary WHERE execution_id = ?", (exec_id,))
        self.assertEqual(len(s_df), 1)

        # Check results table
        r_df = db_adapter.query_df("SELECT COUNT(*) AS c FROM dq_results WHERE execution_id = ?", (exec_id,))
        self.assertGreater(int(r_df.iloc[0]["c"]), 0)

        # Check job runs table
        j_df = db_adapter.query_df("SELECT execution_id FROM dq_job_runs WHERE execution_id = ?", (exec_id,))
        self.assertEqual(len(j_df), 1)

    def test_05_job_status_and_failure_tracking(self):
        """Req 5 & 6: Track job status, duration, and failure details for failed rules."""
        res = db_adapter.trigger_validation("Hospital", "source_table")
        exec_id = res["execution_id"]

        # Check failure details in dq_job_runs
        job_run = db_adapter.query_df("SELECT status, error_details, duration_ms FROM dq_job_runs WHERE execution_id = ?", (exec_id,))
        self.assertFalse(job_run.empty)
        # Because Hospital has 1 failure and 1 error by default
        self.assertEqual(job_run.iloc[0]["status"], "FAILED")
        self.assertIn("validation failure", str(job_run.iloc[0]["error_details"]))

    def test_06_avoid_duplicate_execution_records(self):
        """Req 8: Avoid duplicate execution IDs across runs."""
        exec_ids = set()
        for _ in range(5):
            res = db_adapter.trigger_validation("Hospital", "source_table")
            exec_ids.add(res["execution_id"])
            time.sleep(0.01)

        # All 5 execution IDs must be strictly unique
        self.assertEqual(len(exec_ids), 5)

    def test_07_dashboard_job_status_and_history(self):
        """Req 7: Display job execution status and history in the dashboard."""
        status = db_adapter.get_latest_job_status()
        self.assertIn("job_id", status)
        self.assertIn("status", status)

        runs = db_adapter.get_job_runs(limit=5)
        self.assertIsInstance(runs, pd.DataFrame)
        self.assertFalse(runs.empty)
        self.assertIn("run_id", runs.columns)
        self.assertIn("status", runs.columns)

    def test_08_job_workflow_spec_validity(self):
        """Verify databricks_job.json specification matches Databricks multi-task workflow standard."""
        import json
        job_file = os.path.join(os.path.dirname(__file__), "databricks_job.json")
        self.assertTrue(os.path.exists(job_file), "databricks_job.json missing")

        with open(job_file, "r") as f:
            spec = json.load(f)

        self.assertIn("name", spec)
        self.assertIn("tasks", spec)
        self.assertEqual(len(spec["tasks"]), 2)  # orchestrator + reconciliation
        self.assertIn("schedule", spec)
        self.assertIn("quartz_cron_expression", spec["schedule"])
        self.assertIn("parameters", spec)

        # Verify retry configuration exists on task
        orch_task = spec["tasks"][0]
        self.assertEqual(orch_task["task_key"], "dq_orchestrator")
        self.assertEqual(orch_task["max_retries"], 2)
        self.assertEqual(orch_task["min_retry_interval_millis"], 60000)

        # Verify reconciliation task dependency and execution_id passing
        recon_task = spec["tasks"][1]
        self.assertEqual(recon_task["task_key"], "dq_reconciliation")
        self.assertEqual(recon_task["run_if"], "ALL_SUCCESS")
        self.assertEqual(recon_task["depends_on"][0]["task_key"], "dq_orchestrator")
        self.assertIn("execution_id", recon_task["notebook_task"]["base_parameters"])
        self.assertEqual(recon_task["notebook_task"]["base_parameters"]["execution_id"], "{{tasks.dq_orchestrator.values.execution_id}}")

    def test_09_four_history_categories_linked(self):
        """Confirm that all 4 history categories (summary, results, reconciliation, job_runs) are linked by execution_id."""
        res = db_adapter.trigger_validation(project_name="Hospital", dataset_name="source_table", run_type="MANUAL")
        exec_id = res["execution_id"]

        # Category 1: dq_execution_summary
        sum_df = db_adapter.query_df("SELECT execution_id, dq_score FROM dq_execution_summary WHERE execution_id = ?", (exec_id,))
        self.assertEqual(len(sum_df), 1, "Execution summary missing for exec_id")

        # Category 2: dq_results
        res_df = db_adapter.query_df("SELECT COUNT(*) as c FROM dq_results WHERE execution_id = ?", (exec_id,))
        self.assertGreater(int(res_df.iloc[0]["c"]), 0, "DQ results missing for exec_id")

        # Category 3: dq_reconciliation_results
        recon_df = db_adapter.query_df("SELECT execution_id, status FROM dq_reconciliation_results WHERE execution_id = ?", (exec_id,))
        self.assertEqual(len(recon_df), 1, "Reconciliation history missing or not linked to same exec_id")

        # Category 4: dq_job_runs
        job_df = db_adapter.query_df("SELECT execution_id, status FROM dq_job_runs WHERE execution_id = ?", (exec_id,))
        self.assertEqual(len(job_df), 1, "Job run history missing or not linked to same exec_id")

if __name__ == "__main__":
    unittest.main()

