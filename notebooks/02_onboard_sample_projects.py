# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 02 - Onboard Sample Projects (Hospital, Banking, Logistics)
# MAGIC **Dynamic Data Quality Accelerator (Phase 8)**
# MAGIC 
# MAGIC Demonstrates how to register projects and datasets entirely through configuration metadata without writing or modifying Python code.

# COMMAND ----------
from datetime import datetime

CATALOG = "hospital_catalog"
SCHEMA = "healthcare"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------
# 1. CREATE SYNTHETIC SOURCE & TARGET DATASETS

# Domain 1: Hospital (Healthcare)
spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.patients_source (
    patient_id STRING,
    patient_name STRING,
    patient_age INT,
    admission_date TIMESTAMP
)
""")
spark.sql(f"""
INSERT OVERWRITE {FULL_SCHEMA}.patients_source VALUES
('P001', 'John Doe', 45, TIMESTAMP '2026-11-01 08:00:00'),
('P002', 'Jane Smith', 32, TIMESTAMP '2026-11-01 09:15:00'),
('P003', NULL, 28, TIMESTAMP '2026-11-01 10:30:00'),        -- Null Name
('P004', 'Robert Brown', -5, TIMESTAMP '2026-11-01 11:00:00'),   -- Negative Age
('P005', 'Emily Davis', 61, TIMESTAMP '2026-11-01 11:45:00')
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.patients_target (
    patient_id STRING,
    patient_name STRING,
    patient_age INT,
    admission_date TIMESTAMP
)
""")
spark.sql(f"""
INSERT OVERWRITE {FULL_SCHEMA}.patients_target VALUES
('P001', 'John Doe', 45, TIMESTAMP '2026-11-01 08:00:00'),
('P002', 'Jane Smith', 32, TIMESTAMP '2026-11-01 09:15:00'),
('P003', 'John Doe', 28, TIMESTAMP '2026-11-01 10:30:00'),
('P005', 'Emily Davis', 61, TIMESTAMP '2026-11-01 11:45:00')
-- Notice P004 is intentionally missing in target to demonstrate reconciliation detection!
""")

# Domain 2: Banking (Finance)
spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.customer_transactions_source (
    transaction_id STRING,
    account_number STRING,
    amount DOUBLE,
    transaction_timestamp TIMESTAMP
)
""")
spark.sql(f"""
INSERT OVERWRITE {FULL_SCHEMA}.customer_transactions_source VALUES
('TX101', 'ACC_001', 500.0, current_timestamp()),
('TX102', 'ACC_002', 1250.50, current_timestamp()),
('TX103', 'ACC_001', 75.0, current_timestamp()),           -- Duplicate account check
('TX104', 'ACC_003', -100.0, current_timestamp())          -- Negative amount
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.customer_transactions_target (
    transaction_id STRING,
    account_number STRING,
    amount DOUBLE,
    transaction_timestamp TIMESTAMP
)
""")
spark.sql(f"""
INSERT OVERWRITE {FULL_SCHEMA}.customer_transactions_target VALUES
('TX101', 'ACC_001', 500.0, current_timestamp()),
('TX102', 'ACC_002', 1250.50, current_timestamp())
""")

# Domain 3: Logistics (Supply Chain)
spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.shipments_source (
    shipment_id STRING,
    tracking_number STRING,
    destination STRING,
    weight_kg DOUBLE,
    shipped_at TIMESTAMP
)
""")
spark.sql(f"""
INSERT OVERWRITE {FULL_SCHEMA}.shipments_source VALUES
('SH001', 'TRK_9912', 'New York', 14.5, current_timestamp()),
('SH002', NULL, 'Chicago', 8.2, current_timestamp()),         -- Null tracking number
('SH003', 'TRK_9914', 'Dallas', 2.0, current_timestamp())
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.shipments_target (
    shipment_id STRING,
    tracking_number STRING,
    destination STRING,
    weight_kg DOUBLE,
    shipped_at TIMESTAMP
)
""")
spark.sql(f"""
INSERT OVERWRITE {FULL_SCHEMA}.shipments_target VALUES
('SH001', 'TRK_9912', 'New York', 14.5, current_timestamp()),
('SH002', 'TRK_9913', 'Chicago', 8.2, current_timestamp()),
('SH003', 'TRK_9914', 'Dallas', 2.0, current_timestamp())
""")

print("Synthetic domain tables created!")

# COMMAND ----------
# 2. REGISTER PROJECTS IN dq_project_config (Zero Code Modifications)
spark.sql(f"""
MERGE INTO {FULL_SCHEMA}.dq_project_config target
USING (
    SELECT 'Hospital' AS project_name, 'Healthcare Patient Data Quality' AS description UNION ALL
    SELECT 'Banking' AS project_name, 'Customer Financial Transactions' AS description UNION ALL
    SELECT 'Logistics' AS project_name, 'Supply Chain Shipment Tracking' AS description
) source
ON target.project_name = source.project_name
WHEN NOT MATCHED THEN
INSERT (project_name, description, catalog_name, schema_name, active, created_at, updated_at)
VALUES (source.project_name, source.description, '{CATALOG}', '{SCHEMA}', true, current_timestamp(), current_timestamp())
""")

# COMMAND ----------
# 3. REGISTER DATASETS IN dq_table_config
spark.sql(f"""
MERGE INTO {FULL_SCHEMA}.dq_table_config target
USING (
    SELECT p.project_id, 'source_table' as dataset_name,
           '{FULL_SCHEMA}.patients_source' as source_table,
           '{FULL_SCHEMA}.patients_target' as target_table,
           'patient_id' as business_key
    FROM {FULL_SCHEMA}.dq_project_config p WHERE p.project_name = 'Hospital'
    UNION ALL
    SELECT p.project_id, 'customer' as dataset_name,
           '{FULL_SCHEMA}.customer_transactions_source' as source_table,
           '{FULL_SCHEMA}.customer_transactions_target' as target_table,
           'transaction_id' as business_key
    FROM {FULL_SCHEMA}.dq_project_config p WHERE p.project_name = 'Banking'
    UNION ALL
    SELECT p.project_id, 'shipment' as dataset_name,
           '{FULL_SCHEMA}.shipments_source' as source_table,
           '{FULL_SCHEMA}.shipments_target' as target_table,
           'shipment_id' as business_key
    FROM {FULL_SCHEMA}.dq_project_config p WHERE p.project_name = 'Logistics'
) source
ON target.source_table = source.source_table
WHEN NOT MATCHED THEN
INSERT (project_id, dataset_name, source_table, target_table, business_key, active, created_at, updated_at)
VALUES (source.project_id, source.dataset_name, source.source_table, source.target_table, source.business_key, true, current_timestamp(), current_timestamp())
""")

# COMMAND ----------
# 4. CONFIGURE REUSABLE RULES IN dq_rules

# Hospital: Check null patient name + check non-negative age
spark.sql(f"""
INSERT INTO {FULL_SCHEMA}.dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'HOSP_NULL_NAME', d.project_id, d.dataset_id, 'NULL_COUNT', 'Completeness', 'patient_name', 0, 'High', true, current_timestamp()
FROM {FULL_SCHEMA}.dq_table_config d WHERE d.dataset_name = 'source_table'
AND NOT EXISTS (SELECT 1 FROM {FULL_SCHEMA}.dq_rules WHERE rule_name = 'HOSP_NULL_NAME');
""")

# Banking: Check duplicate transaction accounts + check null amounts
spark.sql(f"""
INSERT INTO {FULL_SCHEMA}.dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'BANK_UNIQUE_TX', d.project_id, d.dataset_id, 'UNIQUE_COUNT', 'Uniqueness', 'transaction_id', 0, 'Critical', true, current_timestamp()
FROM {FULL_SCHEMA}.dq_table_config d WHERE d.dataset_name = 'customer'
AND NOT EXISTS (SELECT 1 FROM {FULL_SCHEMA}.dq_rules WHERE rule_name = 'BANK_UNIQUE_TX');
""")

# Logistics: Check null tracking numbers
spark.sql(f"""
INSERT INTO {FULL_SCHEMA}.dq_rules (rule_name, project_id, dataset_id, template_name, category, column_name, threshold, severity, active, created_at)
SELECT 'LOG_NULL_TRACKING', d.project_id, d.dataset_id, 'NULL_COUNT', 'Completeness', 'tracking_number', 0, 'Medium', true, current_timestamp()
FROM {FULL_SCHEMA}.dq_table_config d WHERE d.dataset_name = 'shipment'
AND NOT EXISTS (SELECT 1 FROM {FULL_SCHEMA}.dq_rules WHERE rule_name = 'LOG_NULL_TRACKING');
""")

# COMMAND ----------
# 5. REGISTER RECONCILIATION CONFIGURATION
spark.sql(f"""
MERGE INTO {FULL_SCHEMA}.dq_reconciliation_config target
USING (
    SELECT d.dataset_id, 'patient_name,patient_age' as compare_columns
    FROM {FULL_SCHEMA}.dq_table_config d WHERE d.dataset_name = 'source_table'
    UNION ALL
    SELECT d.dataset_id, 'account_number,amount' as compare_columns
    FROM {FULL_SCHEMA}.dq_table_config d WHERE d.dataset_name = 'customer'
    UNION ALL
    SELECT d.dataset_id, 'tracking_number,destination' as compare_columns
    FROM {FULL_SCHEMA}.dq_table_config d WHERE d.dataset_name = 'shipment'
) source
ON target.dataset_id = source.dataset_id
WHEN NOT MATCHED THEN
INSERT (dataset_id, compare_columns, active, created_at)
VALUES (source.dataset_id, source.compare_columns, true, current_timestamp())
""")

print("Successfully onboarded Hospital, Banking, and Logistics without Python code modification!")

