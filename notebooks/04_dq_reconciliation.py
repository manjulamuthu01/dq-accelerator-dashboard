# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 04 - Generic DQ Reconciliation Job Task
# MAGIC **Dynamic Data Quality Accelerator (Phases 4, 7 & 8)**
# MAGIC 
# MAGIC Executes generic source-to-target reconciliation across active datasets or scoped by widgets (`project_name`, `dataset_name`).
# MAGIC Compares source vs target record counts, detects missing rows in target, extra rows, and duplicate keys.
# MAGIC Inherits the exact same `execution_id` from `dq_orchestrator` via TaskValues/widgets to link history tables.
# MAGIC Propagates fatal infrastructure exceptions so Databricks automatically triggers task retries.

# COMMAND ----------
from datetime import datetime, timezone
import uuid
import os

# --------------------------------------------------
# 1. CONFIGURATION & WIDGETS
# --------------------------------------------------

SCHEMA = os.environ.get("DQ_CATALOG_SCHEMA", "hospital_catalog.healthcare")

PROJECT_CONFIG_TABLE        = SCHEMA + ".dq_project_config"
TABLE_CONFIG_TABLE          = SCHEMA + ".dq_table_config"
RECON_CONFIG_TABLE          = SCHEMA + ".dq_reconciliation_config"
RECON_RESULTS_TABLE         = SCHEMA + ".dq_reconciliation_results"
RECON_SAMPLES_TABLE         = SCHEMA + ".dq_reconciliation_samples"
ALERTS_TABLE                = SCHEMA + ".dq_alerts"

try:
    dbutils.widgets.text("project_name", "all", "Project filter (name or all)")
    dbutils.widgets.text("dataset_name", "all", "Dataset filter (name or all)")
    dbutils.widgets.text("execution_id", "", "Shared Execution ID from dq_orchestrator")
    PROJECT_FILTER = (dbutils.widgets.get("project_name") or "all").strip()
    DATASET_FILTER = (dbutils.widgets.get("dataset_name") or "all").strip()
    in_exec_id = (dbutils.widgets.get("execution_id") or "").strip()
except Exception:
    PROJECT_FILTER = "all"
    DATASET_FILTER = "all"
    in_exec_id = ""

# Link to the exact same execution_id emitted by dq_orchestrator
execution_id = ""
if in_exec_id and not in_exec_id.startswith("{{"):
    execution_id = in_exec_id
else:
    try:
        execution_id = dbutils.jobs.taskValues.get(taskKey="dq_orchestrator", key="execution_id", default="")
    except Exception:
        execution_id = ""

if not execution_id:
    # Fallback only if run standalone outside workflow
    execution_id = "EXEC-" + datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + str(uuid.uuid4())[:8].upper()

run_started_at = datetime.now(timezone.utc)

print("=" * 60)
print(f"RECONCILIATION TASK (Linked Workflow): {execution_id}")
print(f"Scope: project_name={PROJECT_FILTER}, dataset_name={DATASET_FILTER}")
print("=" * 60)

# --------------------------------------------------
# 2. DISCOVER ACTIVE DATASETS FOR RECONCILIATION
# --------------------------------------------------

discovery_sql = (
    f"SELECT p.project_id, p.project_name, "
    f"t.dataset_id, t.dataset_name, "
    f"t.source_table, t.target_table, t.business_key "
    f"FROM {TABLE_CONFIG_TABLE} t "
    f"JOIN {PROJECT_CONFIG_TABLE} p ON p.project_id = t.project_id "
    f"WHERE t.active = true AND p.active = true "
    f"ORDER BY p.project_name, t.dataset_name"
)

# Fatal discovery failure MUST raise to trigger Databricks task retry
try:
    active_datasets = [row.asDict() for row in spark.sql(discovery_sql).collect()]
except Exception as e:
    print(f"[FATAL] Discovery query failed: {e}")
    raise RuntimeError(f"Reconciliation metadata discovery failed, triggering task retry: {e}")

if PROJECT_FILTER.lower() != "all":
    active_datasets = [d for d in active_datasets if d["project_name"].lower() == PROJECT_FILTER.lower()]

if DATASET_FILTER.lower() != "all":
    active_datasets = [d for d in active_datasets if d["dataset_name"].lower() == DATASET_FILTER.lower()]

print(f"Active datasets selected for reconciliation: {len(active_datasets)}")

# --------------------------------------------------
# 3. EXECUTE RECONCILIATION PER DATASET
# --------------------------------------------------

total_reconciled = 0
total_passed = 0
total_failed = 0

for d in active_datasets:
    proj_name = d["project_name"]
    ds_name = d["dataset_name"]
    src_tbl = d["source_table"]
    tgt_tbl = d["target_table"]
    b_key = d["business_key"] or "id"

    print(f"\nProcessing reconciliation for {proj_name} / {ds_name}...")
    print(f"  Source: {src_tbl} -> Target: {tgt_tbl} (Key: {b_key})")

    try:
        # Check source count
        src_df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {src_tbl}")
        src_count = src_df.collect()[0]["cnt"]

        # Check target count
        tgt_df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {tgt_tbl}")
        tgt_count = tgt_df.collect()[0]["cnt"]

        # Missing in target
        missing_sql = (
            f"SELECT COUNT(*) AS cnt FROM {src_tbl} s "
            f"LEFT JOIN {tgt_tbl} t ON s.{b_key} = t.{b_key} "
            f"WHERE t.{b_key} IS NULL"
        )
        missing_count = spark.sql(missing_sql).collect()[0]["cnt"]

        # Extra in target
        extra_sql = (
            f"SELECT COUNT(*) AS cnt FROM {tgt_tbl} t "
            f"LEFT JOIN {src_tbl} s ON t.{b_key} = s.{b_key} "
            f"WHERE s.{b_key} IS NULL"
        )
        extra_count = spark.sql(extra_sql).collect()[0]["cnt"]

        # Duplicate keys in target
        dup_sql = (
            f"SELECT COUNT(*) AS cnt FROM ("
            f"SELECT {b_key}, COUNT(*) FROM {tgt_tbl} "
            f"GROUP BY {b_key} HAVING COUNT(*) > 1) d"
        )
        dup_count = spark.sql(dup_sql).collect()[0]["cnt"]

        diff = abs(src_count - tgt_count)
        matching = max(0, min(src_count, tgt_count) - missing_count)
        status = "PASS" if (diff == 0 and missing_count == 0 and extra_count == 0 and dup_count == 0) else "FAIL"

        print(f"  Result: {status} | Source: {src_count} | Target: {tgt_count} | Missing: {missing_count} | Extra: {extra_count} | Dupes: {dup_count}")

        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Insert into dq_reconciliation_results (linked to shared execution_id)
        insert_res_sql = (
            f"INSERT INTO {RECON_RESULTS_TABLE} "
            f"(execution_id, project_name, dataset_name, source_count, target_count, difference, status, "
            f"matching_records, missing_in_target, extra_in_target, duplicate_keys, executed_at) "
            f"VALUES ('{execution_id}', '{proj_name}', '{ds_name}', {src_count}, {tgt_count}, {diff}, "
            f"'{status}', {matching}, {missing_count}, {extra_count}, {dup_count}, TIMESTAMP '{now_ts}')"
        )
        spark.sql(insert_res_sql)

        # Sample missing keys if any
        if missing_count > 0:
            samples_sql = (
                f"SELECT CAST(s.{b_key} AS STRING) AS key_val FROM {src_tbl} s "
                f"LEFT JOIN {tgt_tbl} t ON s.{b_key} = t.{b_key} "
                f"WHERE t.{b_key} IS NULL LIMIT 10"
            )
            sample_rows = spark.sql(samples_sql).collect()
            for s_row in sample_rows:
                k_val = str(s_row["key_val"]).replace("'", "''")
                insert_sample_sql = (
                    f"INSERT INTO {RECON_SAMPLES_TABLE} "
                    f"(execution_id, patient_id, patient_name, reason) "
                    f"VALUES ('{execution_id}', '{k_val}', 'Sample Key', 'Missing in target')"
                )
                spark.sql(insert_sample_sql)

        # Trigger alert if reconciliation failed
        if status == "FAIL":
            total_failed += 1
            alert_rule = f"RECONCILIATION_{ds_name.upper()}"
            alert_msg = f"Reconciliation failed: Source={src_count}, Target={tgt_count}, Missing={missing_count}, Extra={extra_count}, Dupes={dup_count}"
            insert_alert_sql = (
                f"INSERT INTO {ALERTS_TABLE} "
                f"(execution_id, project_name, dataset_name, rule_name, severity, status, failure_count, observed_value, expected_value, message, created_at) "
                f"VALUES ('{execution_id}', '{proj_name}', '{ds_name}', '{alert_rule}', 'Critical', 'Active', {missing_count + diff}, "
                f"'{src_count} vs {tgt_count}', 'Exact Match (Diff 0)', '{alert_msg}', TIMESTAMP '{now_ts}')"
            )
            spark.sql(insert_alert_sql)
        else:
            total_passed += 1

        total_reconciled += 1

    except Exception as e:
        print(f"[FATAL] Error reconciling {proj_name}/{ds_name}: {e}")
        raise RuntimeError(f"Reconciliation query execution failure on {proj_name}/{ds_name}: {e}")

print("\n" + "=" * 60)
print(f"RECONCILIATION COMPLETE: {total_reconciled} datasets evaluated under {execution_id} ({total_passed} PASS, {total_failed} FAIL)")
print("=" * 60)
