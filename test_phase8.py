import unittest
import sqlite3
import os
import uuid
from datetime import datetime, timezone

from accelerator import (
    Config,
    RuleEngine,
    GenericOrchestrator,
    ReconciliationEngine,
    AlertManager,
    ReportGenerator,
    StorageAdapter
)
import db_adapter

DB_FILE = "dq_accelerator.db"

class TestPhase8(unittest.TestCase):
    """
    Phase 8 Test Suite:
    - Packaging & Reusability (Imports, Modules)
    - Multi-domain Synthetic Testing (Hospital, Banking, Logistics)
    - Zero-Code Onboarding verification
    - Artifact Integrity (SQL DDLs and Notebook scripts)
    """

    @classmethod
    def setUpClass(cls):
        db_adapter._init_local_db()
        cls.conn = sqlite3.connect(DB_FILE)
        cls.cursor = cls.conn.cursor()

        # Seed synthetic tables for testing
        cls._setup_synthetic_tables()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    @classmethod
    def _setup_synthetic_tables(cls):
        """Creates SQLite synthetic tables for Hospital, Banking, Logistics, and Retail."""
        c = cls.cursor

        # Hospital
        c.execute("DROP TABLE IF EXISTS syn_patients_source")
        c.execute("DROP TABLE IF EXISTS syn_patients_target")
        c.execute("""
            CREATE TABLE syn_patients_source (
                patient_id TEXT PRIMARY KEY,
                patient_name TEXT,
                patient_age INT,
                admission_date TEXT
            )
        """)
        c.execute("""
            CREATE TABLE syn_patients_target (
                patient_id TEXT PRIMARY KEY,
                patient_name TEXT,
                patient_age INT,
                admission_date TEXT
            )
        """)
        c.executemany("INSERT INTO syn_patients_source VALUES (?, ?, ?, ?)", [
            ('P001', 'John Doe', 45, '2026-11-01 08:00:00'),
            ('P002', 'Jane Smith', 32, '2026-11-01 09:15:00'),
            ('P003', None, 28, '2026-11-01 10:30:00'),
            ('P004', 'Robert Brown', -5, '2026-11-01 11:00:00'),
            ('P005', 'Emily Davis', 61, '2026-11-01 11:45:00')
        ])
        c.executemany("INSERT INTO syn_patients_target VALUES (?, ?, ?, ?)", [
            ('P001', 'John Doe', 45, '2026-11-01 08:00:00'),
            ('P002', 'Jane Smith', 32, '2026-11-01 09:15:00'),
            ('P003', 'John Doe', 28, '2026-11-01 10:30:00'),
            ('P005', 'Emily Davis', 61, '2026-11-01 11:45:00')
            # P004 missing intentionally
        ])

        # Banking
        c.execute("DROP TABLE IF EXISTS syn_transactions_source")
        c.execute("DROP TABLE IF EXISTS syn_transactions_target")
        c.execute("""
            CREATE TABLE syn_transactions_source (
                tx_id TEXT PRIMARY KEY,
                account_no TEXT,
                amount REAL,
                tx_date TEXT
            )
        """)
        c.execute("""
            CREATE TABLE syn_transactions_target (
                tx_id TEXT PRIMARY KEY,
                account_no TEXT,
                amount REAL,
                tx_date TEXT
            )
        """)
        c.executemany("INSERT INTO syn_transactions_source VALUES (?, ?, ?, ?)", [
            ('TX101', 'ACC_001', 500.0, '2026-11-01 10:00:00'),
            ('TX102', 'ACC_002', 1250.5, '2026-11-01 11:00:00'),
            ('TX103', 'ACC_001', 75.0, '2026-11-01 12:00:00'),
            ('TX104', 'ACC_003', -100.0, '2026-11-01 13:00:00')
        ])
        c.executemany("INSERT INTO syn_transactions_target VALUES (?, ?, ?, ?)", [
            ('TX101', 'ACC_001', 500.0, '2026-11-01 10:00:00'),
            ('TX102', 'ACC_002', 1250.5, '2026-11-01 11:00:00')
        ])

        # Logistics
        c.execute("DROP TABLE IF EXISTS syn_shipments_source")
        c.execute("DROP TABLE IF EXISTS syn_shipments_target")
        c.execute("""
            CREATE TABLE syn_shipments_source (
                shipment_id TEXT PRIMARY KEY,
                tracking_no TEXT,
                destination TEXT,
                weight REAL
            )
        """)
        c.execute("""
            CREATE TABLE syn_shipments_target (
                shipment_id TEXT PRIMARY KEY,
                tracking_no TEXT,
                destination TEXT,
                weight REAL
            )
        """)
        c.executemany("INSERT INTO syn_shipments_source VALUES (?, ?, ?, ?)", [
            ('SH001', 'TRK_9912', 'New York', 14.5),
            ('SH002', None, 'Chicago', 8.2),
            ('SH003', 'TRK_9914', 'Dallas', 2.0)
        ])
        c.executemany("INSERT INTO syn_shipments_target VALUES (?, ?, ?, ?)", [
            ('SH001', 'TRK_9912', 'New York', 14.5),
            ('SH002', 'TRK_9913', 'Chicago', 8.2),
            ('SH003', 'TRK_9914', 'Dallas', 2.0)
        ])

        cls.conn.commit()

    def test_01_modular_package_exports(self):
        """Req 1 & 2: Verify accelerator package cleanly exposes modular components."""
        self.assertIsNotNone(Config)
        self.assertIsNotNone(RuleEngine)
        self.assertIsNotNone(GenericOrchestrator)
        self.assertIsNotNone(ReconciliationEngine)
        self.assertIsNotNone(AlertManager)
        self.assertIsNotNone(ReportGenerator)
        self.assertIsNotNone(StorageAdapter)

        # Verify template dictionary in RuleEngine
        templates = RuleEngine.get_standard_templates()
        self.assertIn("NULL_COUNT", templates)
        self.assertIn("UNIQUE_COUNT", templates)
        self.assertIn("ROW_COUNT", templates)
        self.assertGreaterEqual(len(templates), 11)

    def test_02_sql_scripts_and_notebooks_exist(self):
        """Req 3, 4, 5: Verify all required SQL DDLs, seed scripts, and notebooks exist."""
        sql_files = [
            "sql/01_create_all_tables.sql",
            "sql/02_seed_sample_data.sql"
        ]
        for f in sql_files:
            self.assertTrue(os.path.exists(f), f"Missing SQL file: {f}")
            with open(f, "r", encoding="utf-8") as fp:
                content = fp.read()
                expected_kw = "CREATE TABLE" if "01" in f else "INSERT INTO"
                self.assertIn(expected_kw, content)

        notebook_files = [
            "notebooks/01_environment_setup.py",
            "notebooks/02_onboard_sample_projects.py",
            "notebooks/03_dq_orchestrator.py",
            "notebooks/04_dq_reconciliation.py"
        ]
        for nb in notebook_files:
            self.assertTrue(os.path.exists(nb), f"Missing notebook: {nb}")
            with open(nb, "r", encoding="utf-8") as fp:
                nb_content = fp.read()
                self.assertIn("# Databricks notebook source", nb_content)

        # App packaging
        self.assertTrue(os.path.exists("setup.py"))
        self.assertTrue(os.path.exists("app.yaml"))
        self.assertTrue(os.path.exists("requirements.txt"))

    def test_03_hospital_domain_validation_and_reconciliation(self):
        """Req 7 & 8: Test accelerator on synthetic Hospital dataset."""
        # 1. Test DQ rule rendering for null patient name
        rendered_sql = RuleEngine.render_template(
            "NULL_COUNT",
            source_table="syn_patients_source",
            column_name="patient_name"
        )
        self.cursor.execute(rendered_sql)
        null_count = self.cursor.fetchone()[0]
        self.assertEqual(null_count, 1, "Should detect 1 null patient name in Hospital source")

        # 2. Test reconciliation queries
        queries = ReconciliationEngine.build_reconciliation_queries(
            source_table="syn_patients_source",
            target_table="syn_patients_target",
            business_key="patient_id"
        )
        self.cursor.execute(queries["source_count"])
        src_cnt = self.cursor.fetchone()[0]
        self.cursor.execute(queries["target_count"])
        tgt_cnt = self.cursor.fetchone()[0]
        self.cursor.execute(queries["missing_in_target"])
        missing_cnt = self.cursor.fetchone()[0]

        summary = ReconciliationEngine.compute_reconciliation_summary(src_cnt, tgt_cnt, missing=missing_cnt)
        self.assertEqual(summary["source_count"], 5)
        self.assertEqual(summary["target_count"], 4)
        self.assertEqual(summary["missing_in_target"], 1)
        self.assertEqual(summary["status"], "FAIL")

    def test_04_banking_domain_validation_and_reconciliation(self):
        """Req 7 & 8: Test accelerator on synthetic Banking dataset."""
        # Check duplicate accounts in transactions
        rendered_sql = RuleEngine.render_template(
            "DUPLICATE_COUNT",
            source_table="syn_transactions_source",
            column_name="account_no"
        )
        self.cursor.execute(rendered_sql)
        dup_accounts = self.cursor.fetchone()[0]
        self.assertEqual(dup_accounts, 1, "Should detect 1 duplicate account_no in transactions")

        # Check reconciliation
        queries = ReconciliationEngine.build_reconciliation_queries(
            source_table="syn_transactions_source",
            target_table="syn_transactions_target",
            business_key="tx_id"
        )
        self.cursor.execute(queries["source_count"])
        src_cnt = self.cursor.fetchone()[0]
        self.cursor.execute(queries["target_count"])
        tgt_cnt = self.cursor.fetchone()[0]
        self.cursor.execute(queries["missing_in_target"])
        missing_cnt = self.cursor.fetchone()[0]

        summary = ReconciliationEngine.compute_reconciliation_summary(src_cnt, tgt_cnt, missing=missing_cnt)
        self.assertEqual(summary["source_count"], 4)
        self.assertEqual(summary["target_count"], 2)
        self.assertEqual(summary["missing_in_target"], 2)
        self.assertEqual(summary["status"], "FAIL")

    def test_05_logistics_domain_validation_and_reconciliation(self):
        """Req 7 & 8: Test accelerator on synthetic Logistics dataset."""
        # Check null tracking numbers
        rendered_sql = RuleEngine.render_template(
            "NULL_COUNT",
            source_table="syn_shipments_source",
            column_name="tracking_no"
        )
        self.cursor.execute(rendered_sql)
        null_trks = self.cursor.fetchone()[0]
        self.assertEqual(null_trks, 1, "Should detect 1 null tracking number in shipments")

        # Check reconciliation: counts match but target has non-null tracking
        queries = ReconciliationEngine.build_reconciliation_queries(
            source_table="syn_shipments_source",
            target_table="syn_shipments_target",
            business_key="shipment_id"
        )
        self.cursor.execute(queries["source_count"])
        src_cnt = self.cursor.fetchone()[0]
        self.cursor.execute(queries["target_count"])
        tgt_cnt = self.cursor.fetchone()[0]
        self.cursor.execute(queries["missing_in_target"])
        missing_cnt = self.cursor.fetchone()[0]

        summary = ReconciliationEngine.compute_reconciliation_summary(src_cnt, tgt_cnt, missing=missing_cnt)
        self.assertEqual(summary["source_count"], 3)
        self.assertEqual(summary["target_count"], 3)
        self.assertEqual(summary["missing_in_target"], 0)
        self.assertEqual(summary["status"], "PASS")

    def test_06_zero_code_onboarding_verification(self):
        """Req 9: Verify a 4th brand-new project/dataset can be onboarded purely via config without changing code."""
        c = self.cursor

        # 1. Create a brand-new synthetic table: Retail
        c.execute("DROP TABLE IF EXISTS syn_retail_orders")
        c.execute("""
            CREATE TABLE syn_retail_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT,
                amount REAL,
                status TEXT
            )
        """)
        c.executemany("INSERT INTO syn_retail_orders VALUES (?, ?, ?, ?)", [
            ('ORD_01', 'CUST_1', 120.0, 'COMPLETED'),
            ('ORD_02', None, 50.0, 'COMPLETED'),       # Null customer
            ('ORD_03', 'CUST_2', 300.0, 'SHIPPED')
        ])

        # 2. Register new project purely via metadata insert
        c.execute("""
            INSERT OR REPLACE INTO dq_project_config (project_id, project_name, description, catalog_name, schema_name, active)
            VALUES (99, 'Retail_E2E', 'Zero-code test project', 'retail_cat', 'store', 1)
        """)

        # 3. Register dataset purely via metadata insert
        c.execute("""
            INSERT OR REPLACE INTO dq_table_config (dataset_id, project_id, dataset_name, source_table, target_table, business_key, active)
            VALUES (991, 99, 'retail_orders', 'syn_retail_orders', 'syn_retail_orders', 'order_id', 1)
        """)

        # 4. Register DQ rule purely via metadata insert
        c.execute("""
            INSERT OR REPLACE INTO dq_rules (rule_id, rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active)
            VALUES (9901, 'RETAIL_CUST_NULL', 99, 991, 'NULL_COUNT', 'Completeness', 'customer_id', 0, 'High', 1)
        """)
        self.conn.commit()

        # 5. Query active datasets through generic query
        active_datasets = db_adapter.get_active_datasets()
        retail_matches = [d for d in active_datasets if d["project_name"] == "Retail_E2E"]
        self.assertEqual(len(retail_matches), 1, "Zero-code project should be discovered automatically")

        # 6. Execute rule validation dynamically using RuleEngine
        rule_rendered = RuleEngine.render_template(
            "NULL_COUNT",
            source_table="syn_retail_orders",
            column_name="customer_id"
        )
        c.execute(rule_rendered)
        failure_count = c.fetchone()[0]
        self.assertEqual(failure_count, 1, "Zero-code rule should execute and identify 1 failure")

        # 7. Record result via adapter
        exec_id = f"EXEC-RETAIL-{str(uuid.uuid4())[:6]}"
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        db_adapter.execute_cmd("""
            INSERT INTO dq_results (execution_id, project_id, dataset_id, rule_name, category, severity, status, failure_count, observed_value, expected_value, duration_ms, executed_at)
            VALUES (?, 99, 991, ?, 'Completeness', 'High', ?, ?, ?, '0', 12, ?)
        """, (exec_id, "RETAIL_CUST_NULL", "FAIL" if failure_count > 0 else "PASS", failure_count, str(failure_count), now_ts))

        # 8. Verify recorded result
        c.execute("SELECT rule_name, status, failure_count FROM dq_results WHERE execution_id = ?", (exec_id,))
        rec = c.fetchone()
        self.assertIsNotNone(rec)
        self.assertEqual(rec[0], "RETAIL_CUST_NULL")
        self.assertEqual(rec[1], "FAIL")
        self.assertEqual(rec[2], 1)

if __name__ == "__main__":
    unittest.main()
