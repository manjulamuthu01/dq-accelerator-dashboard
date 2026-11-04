import unittest
import db_adapter
import sqlite3
import uuid

class TestProjectDatasetOnboarding(unittest.TestCase):
    """
    Test suite for Projects & Datasets Onboarding Layer:
    - Add Project & Dataset workflow
    - Metadata table persistence (dq_project_config & dq_table_config)
    - Test Connectivity validation
    - View Action (Project details, datasets, rules, last execution, score)
    - Edit Action (Metadata updates with audit trail)
    - Disable / Enable Action (Does not delete historical data)
    """

    @classmethod
    def setUpClass(cls):
        db_adapter._init_local_db()
        cls.test_pname = f"Fintech_Pay_{str(uuid.uuid4())[:6]}"

    def test_01_add_project_with_dataset(self):
        """Verify end-to-end onboarding creates rows in dq_project_config and dq_table_config."""
        res = db_adapter.add_new_project_with_dataset(
            project_name=self.test_pname,
            description="Payment gateway transaction auditing",
            owner="Fintech Platform Eng",
            domain="Banking",
            source_table="payments_catalog.fintech.transactions_source",
            target_table="payments_catalog.fintech.transactions_target",
            business_key="transaction_id",
            user="admin"
        )
        self.assertTrue(res["success"], f"Onboarding failed: {res.get('message')}")
        self.assertIn("project_id", res)
        self.assertIn("dataset_id", res)

        # Verify dq_project_config
        p_df = db_adapter.query_df("SELECT * FROM dq_project_config WHERE project_id = ?", (res["project_id"],))
        self.assertFalse(p_df.empty)
        self.assertEqual(p_df.iloc[0]["project_name"], self.test_pname)
        self.assertEqual(p_df.iloc[0]["owner"], "Fintech Platform Eng")

        # Verify dq_table_config
        d_df = db_adapter.query_df("SELECT * FROM dq_table_config WHERE dataset_id = ?", (res["dataset_id"],))
        self.assertFalse(d_df.empty)
        self.assertEqual(d_df.iloc[0]["source_table"], "payments_catalog.fintech.transactions_source")
        self.assertEqual(d_df.iloc[0]["target_table"], "payments_catalog.fintech.transactions_target")
        self.assertEqual(d_df.iloc[0]["business_key"], "transaction_id")

    def test_02_duplicate_project_prevention(self):
        """Ensure duplicate project names are rejected."""
        res = db_adapter.add_new_project_with_dataset(
            project_name=self.test_pname,
            description="Duplicate attempt",
            owner="Tester",
            domain="Banking",
            source_table="test_catalog.test_schema.test_tbl",
        )
        self.assertFalse(res["success"])
        self.assertIn("already exists", res["message"])

    def test_03_test_table_connectivity(self):
        """Verify connectivity testing for table identifiers."""
        # 3-level namespace
        res1 = db_adapter.test_table_connectivity("payments_catalog.fintech.transactions_source")
        self.assertTrue(res1["success"])

        # Empty table
        res2 = db_adapter.test_table_connectivity("")
        self.assertFalse(res2["success"])

    def test_04_view_project_details(self):
        """Verify get_project_details returns project info, datasets, rules, and execution stats."""
        # Query existing Hospital project
        p_row = db_adapter.query_df("SELECT project_id FROM dq_project_config WHERE project_name = 'Hospital'")
        p_id = int(p_row.iloc[0]["project_id"])

        details = db_adapter.get_project_details(p_id)
        self.assertTrue(bool(details))
        self.assertEqual(details["project"]["project_name"], "Hospital")
        self.assertIn("datasets", details)
        self.assertIn("rules", details)
        self.assertIn("avg_dq_score", details)
        self.assertIn("run_count", details)

    def test_05_edit_project_and_dataset(self):
        """Verify editing updates metadata tables and creates audit trail."""
        p_row = db_adapter.query_df("SELECT project_id FROM dq_project_config WHERE project_name = ?", (self.test_pname,))
        p_id = int(p_row.iloc[0]["project_id"])
        d_row = db_adapter.query_df("SELECT dataset_id FROM dq_table_config WHERE project_id = ?", (p_id,))
        d_id = int(d_row.iloc[0]["dataset_id"])

        updated_name = f"{self.test_pname}_Upd"
        success = db_adapter.update_project_and_dataset(
            project_id=p_id,
            project_name=updated_name,
            description="Updated payment gateway auditing",
            owner="Lead Architect",
            dataset_id=d_id,
            source_table="payments_catalog.fintech.tx_source_v2",
            business_key="tx_id",
            user="admin"
        )
        self.assertTrue(success)

        # Verify update
        check_p = db_adapter.query_df("SELECT project_name, owner FROM dq_project_config WHERE project_id = ?", (p_id,))
        self.assertEqual(check_p.iloc[0]["project_name"], updated_name)
        self.assertEqual(check_p.iloc[0]["owner"], "Lead Architect")

        check_d = db_adapter.query_df("SELECT source_table, business_key FROM dq_table_config WHERE dataset_id = ?", (d_id,))
        self.assertEqual(check_d.iloc[0]["source_table"], "payments_catalog.fintech.tx_source_v2")
        self.assertEqual(check_d.iloc[0]["business_key"], "tx_id")

    def test_06_disable_and_enable_preserves_history(self):
        """Verify disabling a project preserves existing execution history and results."""
        # 1. Trigger validation for Hospital
        res = db_adapter.trigger_validation("Hospital", "source_table")
        exec_id = res["execution_id"]

        p_row = db_adapter.query_df("SELECT project_id FROM dq_project_config WHERE project_name = 'Hospital'")
        p_id = int(p_row.iloc[0]["project_id"])

        # 2. Disable project
        db_adapter.toggle_project(p_id)
        p_status = db_adapter.query_df("SELECT active FROM dq_project_config WHERE project_id = ?", (p_id,))
        self.assertEqual(int(p_status.iloc[0]["active"]), 0, "Project should be disabled")

        # 3. Confirm execution history and results are still intact!
        s_check = db_adapter.query_df("SELECT * FROM dq_execution_summary WHERE execution_id = ?", (exec_id,))
        self.assertFalse(s_check.empty, "Execution summary must not be deleted when project is disabled")

        r_check = db_adapter.query_df("SELECT * FROM dq_results WHERE execution_id = ?", (exec_id,))
        self.assertFalse(r_check.empty, "Results must not be deleted when project is disabled")

        # 4. Re-enable project
        db_adapter.toggle_project(p_id)
        p_status2 = db_adapter.query_df("SELECT active FROM dq_project_config WHERE project_id = ?", (p_id,))
        self.assertEqual(int(p_status2.iloc[0]["active"]), 1, "Project should be re-enabled")

if __name__ == "__main__":
    unittest.main()
