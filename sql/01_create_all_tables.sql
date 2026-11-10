-- ==============================================================================
-- DYNAMIC DATA QUALITY ACCELERATOR - DDL SPECIFICATION (PHASES 1 TO 8)
-- Catalog / Schema: hospital_catalog.healthcare (Configurable)
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS hospital_catalog.healthcare;
USE CATALOG hospital_catalog;
USE SCHEMA healthcare;

-- 1. Project Configuration Table (Phase 1)
CREATE TABLE IF NOT EXISTS dq_project_config (
    project_id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name STRING NOT NULL,
    description STRING,
    owner STRING DEFAULT 'Data Engineering Team',
    business_domain STRING,
    catalog_name STRING DEFAULT 'hospital_catalog',
    schema_name STRING DEFAULT 'healthcare',
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT pk_dq_project PRIMARY KEY (project_id)
);

-- 2. Table / Dataset Configuration Table (Phase 1)
CREATE TABLE IF NOT EXISTS dq_table_config (
    dataset_id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    dataset_name STRING NOT NULL,
    source_catalog STRING DEFAULT 'hospital_catalog',
    source_schema STRING DEFAULT 'healthcare',
    source_table STRING NOT NULL,
    target_catalog STRING DEFAULT 'hospital_catalog',
    target_schema STRING DEFAULT 'healthcare',
    target_table STRING,
    business_key STRING,
    description STRING,
    connectivity_status STRING DEFAULT 'Verified',
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT pk_dq_table PRIMARY KEY (dataset_id),
    CONSTRAINT fk_dq_table_proj FOREIGN KEY (project_id) REFERENCES dq_project_config(project_id)
);

-- 3. Reusable Rule Templates Table (Phase 2)
CREATE TABLE IF NOT EXISTS dq_rule_templates (
    template_id BIGINT GENERATED ALWAYS AS IDENTITY,
    template_name STRING NOT NULL,
    category STRING NOT NULL,
    description STRING,
    requires_column BOOLEAN DEFAULT true,
    requires_target BOOLEAN DEFAULT false,
    result_type STRING DEFAULT 'COUNT',
    template_sql STRING NOT NULL,
    is_active BOOLEAN DEFAULT true,
    CONSTRAINT pk_dq_template PRIMARY KEY (template_id)
);

-- 4. Dataset Configured Rules Table (Phase 2)
CREATE TABLE IF NOT EXISTS dq_rules (
    rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
    rule_name STRING NOT NULL,
    project_id BIGINT,
    dataset_id BIGINT,
    template_name STRING,
    category STRING NOT NULL,
    column_name STRING,
    rule_parameter STRING,
    threshold DOUBLE DEFAULT 0.0,
    severity STRING DEFAULT 'High',
    validation_sql STRING,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP,
    CONSTRAINT pk_dq_rules PRIMARY KEY (rule_id)
);

-- 5. Safe Rendered Rules View (Phase 2 & 3)
CREATE OR REPLACE VIEW dq_rules_rendered AS
SELECT 
    r.rule_id,
    r.rule_name,
    r.project_id,
    r.dataset_id,
    r.severity,
    r.template_name,
    t.result_type,
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(t.template_sql, '{source_table}', d.source_table),
                    '{target_table}', d.target_table
                ),
                '{column_name}', COALESCE(r.column_name, 'id')
            ),
            '{rule_parameter}', COALESCE(r.rule_parameter, r.column_name, 'id')
        ),
        '{threshold}', CAST(COALESCE(r.threshold, 0) AS STRING)
    ) AS rendered_sql
FROM dq_rules r
JOIN dq_rule_templates t ON r.template_name = t.template_name
JOIN dq_table_config d ON r.dataset_id = d.dataset_id
WHERE r.active = true AND d.active = true;

-- 6. Execution Level Summary Table (Phase 3)
CREATE TABLE IF NOT EXISTS dq_execution_summary (
    execution_id STRING NOT NULL,
    project_id BIGINT,
    dataset_id BIGINT,
    project_name STRING NOT NULL,
    dataset_name STRING NOT NULL,
    total_rules INT DEFAULT 0,
    passed INT DEFAULT 0,
    failed INT DEFAULT 0,
    errored INT DEFAULT 0,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    duration_ms BIGINT DEFAULT 0,
    overall_status STRING DEFAULT 'PASS',
    dq_status STRING DEFAULT 'IDLE',
    dq_score DOUBLE DEFAULT 100.0,
    CONSTRAINT uq_exec_summary UNIQUE (execution_id, dataset_name)
);

-- 7. Rule Level Detailed Results Table (Phase 3)
CREATE TABLE IF NOT EXISTS dq_results (
    result_id BIGINT GENERATED ALWAYS AS IDENTITY,
    execution_id STRING NOT NULL,
    project_id BIGINT,
    dataset_id BIGINT,
    rule_id BIGINT,
    rule_name STRING NOT NULL,
    category STRING NOT NULL,
    severity STRING DEFAULT 'High',
    status STRING DEFAULT 'PASS',
    failure_count BIGINT DEFAULT 0,
    observed_value STRING,
    expected_value STRING,
    error_message STRING,
    duration_ms BIGINT DEFAULT 0,
    executed_at TIMESTAMP NOT NULL
);

-- 8. Reconciliation Configuration Table (Phase 4)
CREATE TABLE IF NOT EXISTS dq_reconciliation_config (
    config_id BIGINT GENERATED ALWAYS AS IDENTITY,
    dataset_id BIGINT NOT NULL,
    compare_columns STRING,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP
);

-- 9. Reconciliation Results Table (Phase 4)
CREATE TABLE IF NOT EXISTS dq_reconciliation_results (
    recon_id BIGINT GENERATED ALWAYS AS IDENTITY,
    execution_id STRING NOT NULL,
    project_name STRING NOT NULL,
    dataset_name STRING NOT NULL,
    source_count BIGINT DEFAULT 0,
    target_count BIGINT DEFAULT 0,
    difference BIGINT DEFAULT 0,
    status STRING DEFAULT 'PASS',
    matching_records BIGINT DEFAULT 0,
    missing_in_target BIGINT DEFAULT 0,
    extra_in_target BIGINT DEFAULT 0,
    duplicate_keys BIGINT DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    CONSTRAINT uq_recon_summary UNIQUE (execution_id, dataset_name)
);

-- 10. Reconciliation Mismatched Samples Table (Phase 4)
CREATE TABLE IF NOT EXISTS dq_reconciliation_samples (
    sample_id BIGINT GENERATED ALWAYS AS IDENTITY,
    execution_id STRING NOT NULL,
    patient_id STRING,
    patient_name STRING,
    reason STRING DEFAULT 'Missing in target'
);

-- 11. Automated Alerts Table (Phase 6)
CREATE TABLE IF NOT EXISTS dq_alerts (
    alert_id BIGINT GENERATED ALWAYS AS IDENTITY,
    execution_id STRING NOT NULL,
    project_name STRING NOT NULL,
    dataset_name STRING NOT NULL,
    rule_name STRING NOT NULL,
    severity STRING NOT NULL,
    status STRING DEFAULT 'Active',
    failure_count BIGINT DEFAULT 0,
    observed_value STRING,
    expected_value STRING,
    message STRING,
    acknowledged_by STRING,
    acknowledged_at TIMESTAMP,
    closed_by STRING,
    closed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT uq_exec_rule UNIQUE (execution_id, rule_name)
);

-- 12. Configuration Change Audit Log Table (Phase 6)
CREATE TABLE IF NOT EXISTS dq_config_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    entity_type STRING NOT NULL,
    entity_id STRING NOT NULL,
    action STRING NOT NULL,
    changed_by STRING DEFAULT 'admin',
    old_value STRING,
    new_value STRING,
    timestamp TIMESTAMP NOT NULL
);

-- 13. Databricks Workflow Job Run Tracking (Phase 7)
CREATE TABLE IF NOT EXISTS dq_job_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY,
    job_id STRING NOT NULL,
    execution_id STRING NOT NULL,
    run_type STRING DEFAULT 'MANUAL',
    project_name STRING DEFAULT 'all',
    dataset_name STRING DEFAULT 'all',
    status STRING DEFAULT 'SUCCESS',
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    duration_ms BIGINT,
    error_details STRING,
    CONSTRAINT uq_job_execution UNIQUE (job_id, execution_id)
);

