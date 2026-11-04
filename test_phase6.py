"""
Automated Test Suite for Phase 6: Alerts, Reports, and Audit History
Validates all 9 Phase 6 requirements.
"""

import unittest
import json
import pandas as pd
import db_adapter
import dash_app

class TestPhase6(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Force initialization of the local database
        db_adapter._init_local_db()

    def test_01_alerts_table_structure(self):
        """Req 1: Verify dq_alerts table exists and has all required columns."""
        df = db_adapter.query_df("PRAGMA table_info(dq_alerts)")
        cols = df["name"].tolist()
        required_cols = [
            "alert_id", "execution_id", "project_name", "dataset_name",
            "rule_name", "severity", "status", "failure_count",
            "observed_value", "expected_value", "message",
            "acknowledged_by", "acknowledged_at", "closed_by", "closed_at", "created_at"
        ]
        for col in required_cols:
            self.assertIn(col, cols, f"Column {col} missing from dq_alerts table")

    def test_02_config_audit_table_structure(self):
        """Req 7 & 8: Verify dq_config_audit table exists with audit fields."""
        df = db_adapter.query_df("PRAGMA table_info(dq_config_audit)")
        cols = df["name"].tolist()
        required_cols = [
            "audit_id", "entity_type", "entity_id", "action",
            "changed_by", "old_value", "new_value", "timestamp"
        ]
        for col in required_cols:
            self.assertIn(col, cols, f"Column {col} missing from dq_config_audit table")

    def test_03_automatic_alert_generation(self):
        """Req 2 & 3: Alerts generated automatically when rules fail with supported severities."""
        # Trigger a test validation run
        res = db_adapter.trigger_validation("Hospital", "source_table")
        exec_id = res["execution_id"]
        
        # Check alerts generated for this execution
        alerts_df = db_adapter.query_df("SELECT * FROM dq_alerts WHERE execution_id = ?", (exec_id,))
        self.assertFalse(alerts_df.empty, "No alerts generated for execution with failures")
        
        # Verify severities supported (e.g. Critical, High, Medium, Low)
        severities = alerts_df["severity"].unique().tolist()
        for sev in severities:
            self.assertIn(sev, ["Critical", "High", "Medium", "Low"], f"Unsupported severity: {sev}")

    def test_04_avoid_duplicate_alerts(self):
        """Req 4: Ensure duplicate alerts are prevented for the same execution and rule."""
        import time
        test_exec = f"EXEC-TEST-DEDUP-{int(time.time() * 1000)}"
        
        # Ensure clean state for this test execution
        db_adapter.execute_cmd("DELETE FROM dq_alerts WHERE execution_id = ?", (test_exec,))

        # First attempt should succeed
        res1 = db_adapter.generate_alert_if_not_exists(
            execution_id=test_exec,
            project_name="Hospital",
            dataset_name="source_table",
            rule_name="NULL_CHECK_TEST",
            severity="High",
            failure_count=5,
            observed_value="5",
            expected_value="0",
            message="Test duplicate alert",
        )
        self.assertTrue(res1, "First alert insertion should succeed")

        # Second attempt for same execution and rule should be rejected
        res2 = db_adapter.generate_alert_if_not_exists(
            execution_id=test_exec,
            project_name="Hospital",
            dataset_name="source_table",
            rule_name="NULL_CHECK_TEST",
            severity="High",
            failure_count=5,
            observed_value="5",
            expected_value="0",
            message="Test duplicate alert second attempt",
        )
        self.assertFalse(res2, "Duplicate alert for same execution and rule must be avoided")

        # Verify only 1 record exists
        count = db_adapter.query_df("SELECT COUNT(*) AS c FROM dq_alerts WHERE execution_id = ? AND rule_name = ?",
                                   (test_exec, "NULL_CHECK_TEST"))
        self.assertEqual(int(count.iloc[0]["c"]), 1)

        # Cleanup test row
        db_adapter.execute_cmd("DELETE FROM dq_alerts WHERE execution_id = ?", (test_exec,))

    def test_05_alert_acknowledgement_and_closure(self):
        """Req 5: Allow users to acknowledge and close alerts."""
        # Retrieve an active alert
        active_alerts = db_adapter.query_df("SELECT alert_id FROM dq_alerts WHERE status = 'Active' LIMIT 1")
        self.assertFalse(active_alerts.empty, "Need at least 1 active alert for test")
        a_id = int(active_alerts.iloc[0]["alert_id"])

        # 1. Acknowledge
        ack_res = db_adapter.acknowledge_alert(a_id, user="qa_lead")
        self.assertTrue(ack_res, "Acknowledge operation failed")
        row = db_adapter.query_df("SELECT status, acknowledged_by, acknowledged_at FROM dq_alerts WHERE alert_id = ?", (a_id,))
        self.assertEqual(row.iloc[0]["status"], "Acknowledged")
        self.assertEqual(row.iloc[0]["acknowledged_by"], "qa_lead")
        self.assertIsNotNone(row.iloc[0]["acknowledged_at"])

        # 2. Close
        close_res = db_adapter.close_alert(a_id, user="ops_manager")
        self.assertTrue(close_res, "Close operation failed")
        row2 = db_adapter.query_df("SELECT status, closed_by, closed_at FROM dq_alerts WHERE alert_id = ?", (a_id,))
        self.assertEqual(row2.iloc[0]["status"], "Closed")
        self.assertEqual(row2.iloc[0]["closed_by"], "ops_manager")
        self.assertIsNotNone(row2.iloc[0]["closed_at"])

    def test_06_multi_view_report_generation(self):
        """Req 6: Generate downloadable CSV reports for project, dataset, rule, and execution results."""
        rep_project = db_adapter.generate_report("PROJECT_WISE")
        self.assertIsInstance(rep_project, pd.DataFrame)
        self.assertFalse(rep_project.empty)
        self.assertIn("project_name", rep_project.columns)

        rep_dataset = db_adapter.generate_report("DATASET_WISE")
        self.assertIsInstance(rep_dataset, pd.DataFrame)
        self.assertFalse(rep_dataset.empty)
        self.assertIn("dataset_name", rep_dataset.columns)

        rep_rule = db_adapter.generate_report("RULE_WISE")
        self.assertIsInstance(rep_rule, pd.DataFrame)
        self.assertFalse(rep_rule.empty)
        self.assertIn("rule_name", rep_rule.columns)

        rep_exec = db_adapter.generate_report("EXECUTION_WISE")
        self.assertIsInstance(rep_exec, pd.DataFrame)
        self.assertFalse(rep_exec.empty)
        self.assertIn("execution_id", rep_exec.columns)

    def test_07_config_audit_logging(self):
        """Req 7 & 8: Track configuration changes with user, action, old & new config, timestamp."""
        test_proj = f"AuditTestProj_{pd.Timestamp.now().strftime('%H%M%S')}"
        # Add a new project
        added = db_adapter.add_new_project(test_proj, "Testing Audit Logs", user="compliance_officer")
        self.assertTrue(added)

        # Verify audit log recorded
        audits = db_adapter.get_config_audit_log(entity_type="PROJECT", search=test_proj)
        self.assertFalse(audits.empty, "Config audit log was not created for project creation")
        audit_record = audits.iloc[0]
        self.assertEqual(audit_record["action"], "CREATE")
        self.assertEqual(audit_record["changed_by"], "compliance_officer")
        self.assertIn("Testing Audit Logs", str(audit_record["new_value"]))

    def test_08_dashboard_page_integration(self):
        """Req 9: Verify Alerts & Reports and Audit History pages render cleanly."""
        alerts_page = dash_app.render_page("/alerts")
        self.assertIsNotNone(alerts_page)

        audit_page = dash_app.render_page("/audit")
        self.assertIsNotNone(audit_page)

if __name__ == "__main__":
    unittest.main()

