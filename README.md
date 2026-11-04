# Dynamic Data Quality Accelerator (Databricks)

An enterprise-grade, config-driven, metadata-automated **Data Quality and Reconciliation Accelerator** for Databricks Lakehouse and Unity Catalog.

![Databricks Apps Ready](https://img.shields.io/badge/Databricks-Apps%20Ready-blue)
![Python 3.10 | 3.11](https://img.shields.io/badge/Python-3.10%20%7C%203.11-green)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Delta%20Lake-orange)
![Tests Passing](https://img.shields.io/badge/Tests-22%2F22%20Passing-brightgreen)

---

## 1. Executive Overview

The Dynamic Data Quality Accelerator eliminates hardcoded validation scripts and one-off testing silos. By centralizing validation rules, reconciliation queries, execution orchestration, alerting, and reporting into a reusable Lakehouse framework, any team can onboard new projects, tables, and rules entirely through configuration without modifying underlying Python code.

### Core Capabilities:
- **Zero-Code Onboarding**: Add new catalogs, schemas, tables, and business keys purely by inserting rows into configuration metadata tables.
- **11 Standard Rule Templates**: Completeness, Uniqueness, Freshness, Integrity, Volume, and Schema validations parameterized and rendered on-demand.
- **Scalable Reconciliation**: Driver-free source-to-target comparisons computing count differences, missing keys, extra keys, and duplicate keys.
- **Automated Alerts & Multi-View Reports**: Automated failure notifications across 4 severities (`Critical`, `High`, `Medium`, `Low`) with lifecycle transitions and downloadable CSV exports.
- **Databricks Workflows Integration**: Multi-task orchestration supporting both scheduled cron triggers and ad-hoc filtered manual triggers.
- **Databricks App UI**: 8-page responsive Dash and Plotly web dashboard providing real-time KPI monitoring, rule drafting, execution runs, and audit history.

---

## 2. Solution Directory Structure

```text
dq-accelerator-dashboard/
├── accelerator/                      # Core reusable Python package
│   ├── __init__.py                   # Package exports (Config, RuleEngine, Orchestrator, etc.)
│   ├── config.py                     # Centralized environment & catalog table names
│   ├── rule_engine.py                # 11 rule templates & dynamic SQL renderer
│   ├── orchestrator.py               # Concurrent multi-threaded execution engine
│   ├── reconciliation.py             # Source-to-target reconciliation query builder
│   ├── alerts.py                     # Alert generation & lifecycle management
│   ├── reporting.py                  # Multi-view report generation logic
│   └── storage.py                    # Dual-mode connector (Databricks SQL / SQLite)
│
├── sql/                              # Production DDL and Seed Scripts
│   ├── 01_create_all_tables.sql      # Unity Catalog DDL for all 13 metadata and result tables
│   └── 02_seed_sample_data.sql       # Seed data for Hospital, Banking, and Logistics
│
├── notebooks/                        # Databricks Production Workflows & Setup
│   ├── 01_environment_setup.py       # Automated Unity Catalog schema, tables, and views setup
│   ├── 02_onboard_sample_projects.py # Synthetic data generation & zero-code registration
│   ├── 03_dq_orchestrator.py         # Multi-task workflow task for DQ validation
│   └── 04_dq_reconciliation.py       # Multi-task workflow task for Source-to-Target reconciliation
│
├── dash_app.py                       # Databricks App UI (Dash, Plotly, Bootstrap)
├── db_adapter.py                     # Database interface with local SQLite fallback
├── rule_engine.py                    # Root backward compatibility module
├── databricks_job.json               # Databricks Workflow Job definition
├── app.yaml                          # Databricks Apps deployment manifest
├── requirements.txt                  # Python runtime dependencies
├── setup.py                          # Python package installation manifest
│
├── test_phase6.py                    # Unit tests for Alerts, Reports & Audit History (8 tests)
├── test_phase7.py                    # Unit tests for Scheduling, Jobs & Scoped Execution (8 tests)
├── test_phase8.py                    # Unit tests for Packaging, Multi-Domain & Zero-Code (6 tests)
└── README.md                         # Architecture, deployment, and operational guide
```

---

## 3. Reusable Python Package (`accelerator`)

The accelerator is packaged as a standard Python package installable via `pip install -e .` or distributable as a wheel file to Databricks cluster libraries:

```python
from accelerator import (
    Config,
    RuleEngine,
    GenericOrchestrator,
    ReconciliationEngine,
    AlertManager,
    ReportGenerator,
    StorageAdapter
)

# 1. Dynamically render validation SQL
sql = RuleEngine.render_sql("NULL_COUNT", source_table="patients_source", column_name="patient_name")

# 2. Build reconciliation queries
recon_queries = ReconciliationEngine.build_reconciliation_queries(
    source_table="patients_source",
    target_table="patients_target",
    business_key="patient_id"
)

# 3. Compute reconciliation summary
summary = ReconciliationEngine.compute_reconciliation_summary(source_count=100, target_count=98, missing=2)
```

---

## 4. Unity Catalog Metadata & Result Architecture

All metadata, rule definitions, and historical results reside in Unity Catalog Delta tables under the target catalog and schema (default: `hospital_catalog.healthcare`):

| # | Table Name | Type | Description |
|---|------------|------|-------------|
| 1 | `dq_project_config` | Table | Project registry storing name, catalog, schema, and active state. |
| 2 | `dq_table_config` | Table | Dataset registry mapping source tables, target tables, and business keys. |
| 3 | `dq_rule_templates` | Table | Catalog of 11 parameterized SQL rule templates. |
| 4 | `dq_rules` | Table | Configured rule instances bound to datasets with columns and thresholds. |
| 5 | `dq_rules_rendered` | View | Dynamic SQL view pre-rendering executable queries on demand. |
| 6 | `dq_execution_summary` | Table | Run-level rollups (duration, total rules, pass/fail counts, DQ score). |
| 7 | `dq_results` | Table | Rule-level itemized evaluation results with observed vs expected values. |
| 8 | `dq_reconciliation_config` | Table | Optional column-level comparison configuration for reconciliation. |
| 9 | `dq_reconciliation_results` | Table | High-level reconciliation metrics (source vs target counts, missing rows). |
| 10 | `dq_reconciliation_samples` | Table | Sample primary keys for records failing reconciliation. |
| 11 | `dq_alerts` | Table | Active, Acknowledged, and Closed failure alerts. |
| 12 | `dq_config_audit` | Table | Audit log recording who changed what configuration and when. |
| 13 | `dq_job_runs` | Table | History of scheduled and manual Databricks Workflow Job runs. |

---

## 5. Sample Multi-Domain Configurations

The accelerator includes pre-configured configurations and synthetic datasets for three domains:

### 1. Hospital (Healthcare)
* Source: `patients_source` | Target: `patients_target` | Key: `patient_id`
* Validations: `NULL_PATIENT_NAME` (Completeness), `AGE_NON_NEGATIVE` (Accuracy).
* Reconciliation: Detects missing patient `P004` intentionally excluded from target.

### 2. Banking (Finance)
* Source: `customer_transactions_source` | Target: `customer_transactions_target` | Key: `transaction_id`
* Validations: `UNIQUE_ACCOUNT_NO` (Uniqueness), `TRANSACTION_FRESHNESS` (Freshness).
* Reconciliation: Identifies duplicate transaction accounts and pending target batches.

### 3. Logistics (Supply Chain)
* Source: `shipments_source` | Target: `shipments_target` | Key: `shipment_id`
* Validations: `TRACKING_NO_NULL` (Completeness).
* Reconciliation: Validates destination consistency and tracking number integrity.

---

## 6. Projects & Datasets Onboarding Layer

A new user can onboard an entirely new project and dataset through the web UI without writing SQL, creating metadata tables manually, or modifying the Python orchestrator.

### UI Onboarding Workflow (Projects & Datasets):
1. **Add Project**: Enter Project Name, Description, Owner, and Business Domain. An "Example" shortcut pre-fills sample fields for quick onboarding.
2. **Register Dataset**: Provide 3-level source table (`catalog.schema.source_table`) and target table (`catalog.schema.target_table`).
3. **Configure Business Key**: Select unique primary identifier (e.g. `customer_id` or `patient_id`) used to match source and target records.
4. **Test Connectivity**: Click **Test Connectivity** to verify that the configured tables exist in Databricks Unity Catalog / local storage and the user has access.
5. **Save and Activate**: The Backend Configuration Manager automatically generates unique `project_id` and `dataset_id`, maintains their relational link, and stores the records in `dq_project_config` and `dq_table_config`.
6. **Configure DQ Rules**: Once saved, a direct link navigates the user to the **Rule Library** (`/rules`) to configure validation rules.

### Metadata Tables:
- **`dq_project_config`**: `project_id`, `project_name`, `description`, `owner`, `active`, `created_at`, `updated_at`.
- **`dq_table_config`**: `dataset_id`, `project_id`, `dataset_name`, `source_catalog`, `source_schema`, `source_table`, `target_catalog`, `target_schema`, `target_table`, `business_key`, `active`, `created_at`, `updated_at`.

### Supported Real Backend Actions:
- **View**: Displays full project overview, registered datasets, source/target tables, business keys, active DQ rules, last execution status, and historical DQ score.
- **Edit**: Updates project details or dataset mappings with an automated audit log in `dq_config_audit`.
- **Disable**: Disables future executions for that project (`active = false`) without deleting any historical execution records or results.
- **Enable**: Re-enables the project for participation in future scheduled or manual runs (`active = true`).

Alternatively, projects and datasets can be registered via SQL metadata inserts:

```sql
-- Step 1: Register Project
INSERT INTO hospital_catalog.healthcare.dq_project_config 
(project_name, description, catalog_name, schema_name, active)
VALUES ('Retail', 'E-Commerce Orders and Checkout Quality', 'retail_catalog', 'analytics', true);

-- Step 2: Register Dataset
INSERT INTO hospital_catalog.healthcare.dq_table_config 
(project_id, dataset_name, source_table, target_table, business_key, active)
SELECT project_id, 'orders', 'retail_catalog.analytics.orders_source', 'retail_catalog.analytics.orders_target', 'order_id', true
FROM hospital_catalog.healthcare.dq_project_config WHERE project_name = 'Retail';

-- Step 3: Configure Validation Rules
INSERT INTO hospital_catalog.healthcare.dq_rules 
(rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active)
SELECT 'ORDER_AMOUNT_NON_NULL', d.project_id, d.dataset_id, 'NULL_COUNT', 'Completeness', 'total_amount', 0, 'High', true
FROM hospital_catalog.healthcare.dq_table_config d WHERE d.dataset_name = 'orders';
```

---

## 7. Security, Permissions & Dependencies

### Required Permissions:
* **Unity Catalog Grants**:
  * `USE CATALOG` and `USE SCHEMA` on the accelerator catalog/schema.
  * `SELECT` on monitored source and target tables.
  * `SELECT`, `INSERT`, `UPDATE`, `CREATE TABLE` on the DQ metadata schema.
* **Databricks Workflows / Jobs**:
  * `CAN_MANAGE_RUN` or `IS_OWNER` on the Workflow Job.
* **Service Principal / Authentication**:
  * For headless production execution, execute Workflows and Apps under an Azure/AWS/GCP Service Principal with OAuth M2M tokens.

### System & Python Dependencies:
* Python `>= 3.10`
* `databricks-sdk >= 0.20.0`
* `databricks-sql-connector >= 3.0.0`
* `dash >= 2.14.0`
* `dash-bootstrap-components >= 1.5.0`
* `plotly >= 5.18.0`
* `pandas >= 2.0.0`

### Limitations & Boundaries:
* Large-scale reconciliation relies on Spark SQL `LEFT JOIN` operations; tables with tens of billions of rows should have appropriate partitioning on business keys.
* Local development mode uses SQLite for offline execution; Unity Catalog SQL dialects containing Databricks-specific functions (`DATEDIFF(hour, ...)`) run in local simulation mode.

---

## 8. Deployment Steps

### Method A: Databricks Asset Bundles (DAB) / CLI Deployment
1. **Configure Environment Variables**:
   ```bash
   export DATABRICKS_HOST="https://<workspace-instance>.azuredatabricks.net"
   export DATABRICKS_TOKEN="dapi..."
   export DQ_CATALOG_SCHEMA="hospital_catalog.healthcare"
   ```
2. **Execute Setup Scripts**:
   Run `sql/01_create_all_tables.sql` and `sql/02_seed_sample_data.sql` in Databricks SQL Editor or execute `notebooks/01_environment_setup.py`.
3. **Deploy the Databricks App**:
   ```bash
   databricks apps create dq-accelerator-dashboard
   databricks apps deploy dq-accelerator-dashboard --source-code-path .
   ```
4. **Deploy the Workflow Job**:
   ```bash
   databricks jobs create --json-file databricks_job.json
   ```

### Method B: Local Testing & Validation
1. **Activate Virtual Environment & Install Dependencies**:
   ```powershell
   .\venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   pip install -e .
   ```
2. **Run All Unit Test Suites**:
   ```powershell
   python -m unittest discover -s . -p "test_*.py" -v
   ```
3. **Launch Local Dashboard**:
   ```powershell
   python dash_app.py
   ```
   Access at `http://127.0.0.1:8000/`.

---

## 9. Final Phase 8 Verification Checklist

| # | Requirement | Status | Verification Evidence |
|---|-------------|--------|----------------------|
| **1** | Organize solution into reusable Python modules | Complete | `accelerator/` package with `Config`, `RuleEngine`, `Orchestrator`, etc. |
| **2** | Separate config, rule engine, orchestrator, recon, alerts, and reporting | Complete | Individual standalone modules in `accelerator/`. |
| **3** | Provide SQL scripts to create all required tables | Complete | `sql/01_create_all_tables.sql` (13 tables/views) and `sql/02_seed_sample_data.sql`. |
| **4** | Provide setup notebooks and execution notebooks | Complete | `notebooks/01_environment_setup.py`, `02_onboard_sample_projects.py`, `03_dq_orchestrator.py`, `04_dq_reconciliation.py`. |
| **5** | Package Databricks App and its dependencies | Complete | `setup.py`, `app.yaml`, `requirements.txt`. |
| **6** | Create installation, configuration, deployment, and user docs | Complete | Fully documented in `README.md`. |
| **7** | Provide sample configurations for Hospital, Banking, and Logistics | Complete | Configured in `sql/02_seed_sample_data.sql` and `notebooks/02_onboard_sample_projects.py`. |
| **8** | Test accelerator using at least three synthetic datasets | Complete | Hospital, Banking, and Logistics tested in `test_phase8.py`. |
| **9** | Verify new projects can be onboarded without modifying Python code | Complete | Verified dynamically in `test_06_zero_code_onboarding_verification`. |
| **10** | Document permissions, dependencies, limitations, and deployment steps | Complete | Detailed in Sections 7 and 8 of `README.md`. |
| **11** | Full Automated Test Suite Execution | Complete | **22 out of 22 tests passing** across `test_phase6.py`, `test_phase7.py`, and `test_phase8.py`. |
