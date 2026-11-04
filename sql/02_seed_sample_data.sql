-- ==============================================================================
-- DYNAMIC DATA QUALITY ACCELERATOR - SEED DATA SCRIPT
-- Seeds Rule Templates, Sample Projects (Hospital, Banking, Logistics), and Rules
-- ==============================================================================

USE CATALOG hospital_catalog;
USE SCHEMA healthcare;

-- 1. Insert 14 Reusable Rule Templates
INSERT INTO dq_rule_templates (template_name, category, description, requires_column, requires_target, result_type, template_sql, is_active)
VALUES
('NULL_COUNT', 'Completeness', 'Check for null values in a column', true, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} IS NULL', true),
('NULL_PERCENT', 'Completeness', 'Null percentage check against a threshold', true, false, 'PERCENT', 'SELECT (COUNT(CASE WHEN {column_name} IS NULL THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)) AS observed_value FROM {source_table}', true),
('UNIQUE_COUNT', 'Uniqueness', 'Check uniqueness of values in a column', true, false, 'COUNT', 'SELECT (COUNT(*) - COUNT(DISTINCT {column_name})) AS failure_count FROM {source_table}', true),
('DUPLICATE_COUNT', 'Uniqueness', 'Find duplicate records based on business key', true, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM (SELECT {column_name}, COUNT(*) FROM {source_table} GROUP BY {column_name} HAVING COUNT(*) > 1) t', true),
('ROW_COUNT', 'Volume', 'Record count validation against minimum threshold', false, false, 'COUNT', 'SELECT COUNT(*) AS observed_value FROM {source_table}', true),
('BLANK_COUNT', 'Completeness', 'Check for empty string or whitespace records', true, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} WHERE TRIM(CAST({column_name} AS STRING)) = ''', true),
('DATA_FRESHNESS', 'Freshness', 'Verify latest timestamp is within allowable age', true, false, 'HOURS', 'SELECT DATEDIFF(hour, MAX({column_name}), current_timestamp()) AS observed_value FROM {source_table}', true),
('REFERENTIAL_INTEGRITY', 'Integrity', 'Ensure foreign keys exist in parent target table', true, true, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} s LEFT JOIN {target_table} t ON s.{column_name} = t.{rule_parameter} WHERE t.{rule_parameter} IS NULL', true),
('SCHEMA_CHANGE', 'Schema', 'Check table schema consistency against metadata definition', false, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} WHERE 1=0', true),
('REQUIRED_COLUMNS', 'Schema', 'Validate presence of mandatory columns and null safety', true, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} IS NULL', true),
('SOURCE_TARGET_ROW_COUNT', 'Reconciliation', 'Compare total record count between source and target tables', false, true, 'COUNT', 'SELECT ABS((SELECT COUNT(*) FROM {source_table}) - (SELECT COUNT(*) FROM {target_table})) AS observed_value', true),
('SOURCE_TARGET_MISSING_RECORDS', 'Reconciliation', 'Find source records missing from target table based on business key', true, true, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} s LEFT JOIN {target_table} t ON s.{column_name} = t.{rule_parameter} WHERE t.{rule_parameter} IS NULL', true),
('NEGATIVE_VALUE_CHECK', 'Accuracy', 'Check for negative numerical values in a column', true, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} < 0', true),
('DATE_VALIDATION', 'Validity', 'Check for invalid future dates or dates out of acceptable range', true, false, 'COUNT', 'SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} > current_timestamp()', true);

-- 2. Insert Sample Projects (Hospital, Banking, Logistics, Sales, Customer)
INSERT INTO dq_project_config (project_name, description, owner, catalog_name, schema_name, active, created_at, updated_at)
VALUES
('Hospital', 'Healthcare Data Quality and Patient Records', 'Clinical Informatics Team', 'hospital_catalog', 'healthcare', true, current_timestamp(), current_timestamp()),
('Banking', 'Customer Financial Transactions and Account Auditing', 'Financial Platform Engineering', 'banking_catalog', 'finance', true, current_timestamp(), current_timestamp()),
('Logistics', 'Supply Chain Shipment and Fulfillment Tracking', 'Logistics Analytics Team', 'logistics_catalog', 'supplychain', true, current_timestamp(), current_timestamp()),
('Sales', 'Sales Orders and CRM Revenue Auditing', 'Commercial Operations', 'sales_catalog', 'crm', true, current_timestamp(), current_timestamp()),
('Customer', 'Customer Master and Identity Federation', 'Master Data Management', 'customer_catalog', 'masterdata', false, current_timestamp(), current_timestamp());

-- 3. Insert Dataset Configurations for Sample Projects
INSERT INTO dq_table_config (project_id, dataset_name, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_key, active, created_at, updated_at)
SELECT project_id, 'source_table', 'hospital_catalog', 'healthcare', 'hospital_catalog.healthcare.patients_source', 'hospital_catalog', 'healthcare', 'hospital_catalog.healthcare.patients_target', 'patient_id', true, current_timestamp(), current_timestamp()
FROM dq_project_config WHERE project_name = 'Hospital';

INSERT INTO dq_table_config (project_id, dataset_name, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_key, active, created_at, updated_at)
SELECT project_id, 'customer', 'banking_catalog', 'finance', 'banking_catalog.finance.customer_source', 'banking_catalog', 'finance', 'banking_catalog.finance.customer_target', 'customer_id', true, current_timestamp(), current_timestamp()
FROM dq_project_config WHERE project_name = 'Banking';

INSERT INTO dq_table_config (project_id, dataset_name, source_catalog, source_schema, source_table, target_catalog, target_schema, target_table, business_key, active, created_at, updated_at)
SELECT project_id, 'shipment', 'logistics_catalog', 'supplychain', 'logistics_catalog.supplychain.shipment_source', 'logistics_catalog', 'supplychain', 'logistics_catalog.supplychain.shipment_target', 'shipment_id', true, current_timestamp(), current_timestamp()
FROM dq_project_config WHERE project_name = 'Logistics';

-- 4. Configure Sample Rules for Each Domain
-- Hospital Rules
INSERT INTO dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'NULL_PATIENT_NAME', d.project_id, d.dataset_id, 'NULL_COUNT', 'Completeness', 'patient_name', 0, 'High', true, current_timestamp()
FROM dq_table_config d WHERE d.dataset_name = 'source_table';

INSERT INTO dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'AGE_NON_NEGATIVE', d.project_id, d.dataset_id, 'ROW_COUNT', 'Accuracy', 'patient_age', 0, 'High', true, current_timestamp()
FROM dq_table_config d WHERE d.dataset_name = 'source_table';

-- Banking Rules
INSERT INTO dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'UNIQUE_ACCOUNT_NO', d.project_id, d.dataset_id, 'UNIQUE_COUNT', 'Uniqueness', 'account_number', 0, 'Critical', true, current_timestamp()
FROM dq_table_config d WHERE d.dataset_name = 'customer';

INSERT INTO dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'TRANSACTION_FRESHNESS', d.project_id, d.dataset_id, 'DATA_FRESHNESS', 'Freshness', 'transaction_date', 12, 'High', true, current_timestamp()
FROM dq_table_config d WHERE d.dataset_name = 'customer';

-- Logistics Rules
INSERT INTO dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'TRACKING_NO_NULL', d.project_id, d.dataset_id, 'NULL_COUNT', 'Completeness', 'tracking_number', 0, 'Medium', true, current_timestamp()
FROM dq_table_config d WHERE d.dataset_name = 'shipment';

-- 5. Insert Reconciliation Configurations
INSERT INTO dq_reconciliation_config (dataset_id, compare_columns, active, created_at)
SELECT dataset_id, 'patient_name,admission_date,diagnosis', true, current_timestamp()
FROM dq_table_config WHERE dataset_name = 'source_table';

INSERT INTO dq_reconciliation_config (dataset_id, compare_columns, active, created_at)
SELECT dataset_id, 'account_number,balance,customer_status', true, current_timestamp()
FROM dq_table_config WHERE dataset_name = 'customer';

INSERT INTO dq_reconciliation_config (dataset_id, compare_columns, active, created_at)
SELECT dataset_id, 'tracking_number,status,destination', true, current_timestamp()
FROM dq_table_config WHERE dataset_name = 'shipment';

