# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01 - Environment & Metadata Setup
# MAGIC **Dynamic Data Quality Accelerator (Phase 8)**
# MAGIC 
# MAGIC This notebook sets up:
# MAGIC 1. The Unity Catalog schema
# MAGIC 2. All 13 metadata, result, alert, and audit Delta tables
# MAGIC 3. The safe dynamic rendered view (`dq_rules_rendered`)
# MAGIC 4. Seeds the 11 standard reusable rule templates

# COMMAND ----------
# 1. PARAMETERS & SCHEMA SETUP
dbutils.widgets.text("catalog_name", "hospital_catalog", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "healthcare", "Target Schema Name")

CATALOG = dbutils.widgets.get("catalog_name").strip()
SCHEMA = dbutils.widgets.get("schema_name").strip()
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

print(f"Setting up environment for: {FULL_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------
# 2. CREATE METADATA TABLES

# dq_project_config
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_project_config (
    project_id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name STRING NOT NULL,
    description STRING,
    owner STRING DEFAULT 'Data Engineering Team',
    catalog_name STRING DEFAULT '{CATALOG}',
    schema_name STRING DEFAULT '{SCHEMA}',
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT pk_dq_project PRIMARY KEY (project_id)
)
""")

# dq_table_config
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_table_config (
    dataset_id BIGINT GENERATED ALWAYS AS IDENTITY,
    project_id BIGINT NOT NULL,
    dataset_name STRING NOT NULL,
    source_catalog STRING DEFAULT '{CATALOG}',
    source_schema STRING DEFAULT '{SCHEMA}',
    source_table STRING NOT NULL,
    target_catalog STRING DEFAULT '{CATALOG}',
    target_schema STRING DEFAULT '{SCHEMA}',
    target_table STRING,
    business_key STRING,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT pk_dq_table PRIMARY KEY (dataset_id)
)
""")

# dq_rule_templates
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_rule_templates (
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
)
""")

# dq_rules
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_rules (
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
)
""")

# COMMAND ----------
# 3. CREATE SAFE RENDERED RULES VIEW
spark.sql(f"""
CREATE OR REPLACE VIEW {FULL_SCHEMA}.dq_rules_rendered AS
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
                    REPLACE(t.template_sql, '{{source_table}}', d.source_table),
                    '{{target_table}}', d.target_table
                ),
                '{{column_name}}', COALESCE(r.column_name, 'id')
            ),
            '{{rule_parameter}}', COALESCE(r.rule_parameter, r.column_name, 'id')
        ),
        '{{threshold}}', CAST(COALESCE(r.threshold, 0) AS STRING)
    ) AS rendered_sql
FROM {FULL_SCHEMA}.dq_rules r
JOIN {FULL_SCHEMA}.dq_rule_templates t ON r.template_name = t.template_name
JOIN {FULL_SCHEMA}.dq_table_config d ON r.dataset_id = d.dataset_id
WHERE r.active = true AND d.active = true
""")

# COMMAND ----------
# 4. CREATE EXECUTION, RESULTS, ALERTS, AND AUDIT TABLES

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_execution_summary (
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
    dq_score DOUBLE DEFAULT 100.0
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_results (
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
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_reconciliation_config (
    config_id BIGINT GENERATED ALWAYS AS IDENTITY,
    dataset_id BIGINT NOT NULL,
    compare_columns STRING,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_reconciliation_results (
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
    executed_at TIMESTAMP NOT NULL
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_alerts (
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
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_config_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    entity_type STRING NOT NULL,
    entity_id STRING NOT NULL,
    action STRING NOT NULL,
    changed_by STRING DEFAULT 'admin',
    old_value STRING,
    new_value STRING,
    timestamp TIMESTAMP NOT NULL
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_SCHEMA}.dq_job_runs (
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
    error_details STRING
)
""")

# COMMAND ----------
# 5. SEED 11 STANDARD REUSABLE RULE TEMPLATES
templates = [
    ("NULL_COUNT", "Completeness", "Check for null values in a column", True, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} IS NULL"),
    ("NULL_PERCENT", "Completeness", "Null percentage check against a threshold", True, False, "PERCENT", "SELECT (COUNT(CASE WHEN {column_name} IS NULL THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)) AS observed_value FROM {source_table}"),
    ("UNIQUE_COUNT", "Uniqueness", "Check uniqueness of values in a column", True, False, "COUNT", "SELECT (COUNT(*) - COUNT(DISTINCT {column_name})) AS failure_count FROM {source_table}"),
    ("DUPLICATE_COUNT", "Uniqueness", "Find duplicate records based on business key", True, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM (SELECT {column_name}, COUNT(*) FROM {source_table} GROUP BY {column_name} HAVING COUNT(*) > 1) t"),
    ("ROW_COUNT", "Volume", "Record count validation against minimum threshold", False, False, "COUNT", "SELECT COUNT(*) AS observed_value FROM {source_table}"),
    ("BLANK_COUNT", "Completeness", "Check for empty string or whitespace records", True, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE TRIM(CAST({column_name} AS STRING)) = ''"),
    ("DATA_FRESHNESS", "Freshness", "Verify latest timestamp is within allowable age", True, False, "HOURS", "SELECT DATEDIFF(hour, MAX({column_name}), current_timestamp()) AS observed_value FROM {source_table}"),
    ("REFERENTIAL_INTEGRITY", "Integrity", "Ensure foreign keys exist in parent target table", True, True, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} s LEFT JOIN {target_table} t ON s.{column_name} = t.{rule_parameter} WHERE t.{rule_parameter} IS NULL"),
    ("SCHEMA_CHANGE", "Schema", "Check table schema consistency against metadata definition", False, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE 1=0"),
    ("REQUIRED_COLUMNS", "Schema", "Validate presence of mandatory columns and null safety", True, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} IS NULL"),
    ("SOURCE_TARGET_ROW_COUNT", "Reconciliation", "Compare total record count between source and target tables", False, True, "COUNT", "SELECT ABS((SELECT COUNT(*) FROM {source_table}) - (SELECT COUNT(*) FROM {target_table})) AS observed_value"),
    ("SOURCE_TARGET_MISSING_RECORDS", "Reconciliation", "Find source records missing from target table based on business key", True, True, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} s LEFT JOIN {target_table} t ON s.{column_name} = t.{rule_parameter} WHERE t.{rule_parameter} IS NULL"),
    ("NEGATIVE_VALUE_CHECK", "Accuracy", "Check for negative numerical values in a column", True, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} < 0"),
    ("DATE_VALIDATION", "Validity", "Check for invalid future dates or dates out of acceptable range", True, False, "COUNT", "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} > current_timestamp()")
]

for t in templates:
    spark.sql(f"""
    MERGE INTO {FULL_SCHEMA}.dq_rule_templates target
    USING (SELECT '{t[0]}' as template_name) source
    ON target.template_name = source.template_name
    WHEN NOT MATCHED THEN
    INSERT (template_name, category, description, requires_column, requires_target, result_type, template_sql, is_active)
    VALUES ('{t[0]}', '{t[1]}', '{t[2]}', {str(t[3]).lower()}, {str(t[4]).lower()}, '{t[5]}', '{t[6]}', true)
    """)

print("Environment setup and table initialization completed successfully!")

