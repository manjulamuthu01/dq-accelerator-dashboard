# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Dynamic Data Quality Accelerator - Generic Orchestrator Job (Phase 7)
# MAGIC **Capabilities**:
# MAGIC - Config-driven execution for all or selected projects/datasets (`project_name`, `dataset_name` widgets).
# MAGIC - Multi-threaded execution via `ThreadPoolExecutor` (max 3 workers).
# MAGIC - Dynamic template rendering (`dq_rules_rendered`) + legacy rule support.
# MAGIC - Automatic alert generation into `dq_alerts` for rule failures (Phase 6 integration).
# MAGIC - Persists execution summaries (`dq_execution_summary`) and detailed results (`dq_results`).
# MAGIC - Robust error handling with per-rule isolation (failed rules do not abort other checks).

# COMMAND ----------
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import uuid
import math
import time
import os

# --------------------------------------------------
# 1. CONFIGURATION & JOB PARAMETERS
# --------------------------------------------------

SCHEMA = os.environ.get("DQ_CATALOG_SCHEMA", "hospital_catalog.healthcare")

PROJECT_CONFIG_TABLE = SCHEMA + ".dq_project_config"
TABLE_CONFIG_TABLE   = SCHEMA + ".dq_table_config"
RULES_TABLE          = SCHEMA + ".dq_rules"
RENDERED_RULES_VIEW  = SCHEMA + ".dq_rules_rendered"
RESULTS_TABLE        = SCHEMA + ".dq_results"
SUMMARY_TABLE        = SCHEMA + ".dq_execution_summary"
ALERTS_TABLE         = SCHEMA + ".dq_alerts"

MAX_WORKERS = 3  # Max concurrent DQ agents

# Job Widgets / Parameter Overrides
try:
    dbutils.widgets.text("project_name", "all", "Project filter (name or all)")
    dbutils.widgets.text("dataset_name", "all", "Dataset filter (name or all)")
    PROJECT_FILTER = (dbutils.widgets.get("project_name") or "all").strip()
    DATASET_FILTER = (dbutils.widgets.get("dataset_name") or "all").strip()
except Exception:
    PROJECT_FILTER = "all"
    DATASET_FILTER = "all"

# Unique execution ID for this run (prevents duplicate runs)
execution_id = "EXEC-" + datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + str(uuid.uuid4())[:8].upper()
run_started_at = datetime.now(timezone.utc)

print("=" * 60)
print("DYNAMIC DQ ACCELERATOR - ORCHESTRATOR JOB")
print("Execution ID : " + execution_id)
print("Project Filter: " + PROJECT_FILTER)
print("Dataset Filter: " + DATASET_FILTER)
print("Started At    : " + run_started_at.isoformat())
print("=" * 60)

# COMMAND ----------
# --------------------------------------------------
# 2. DISCOVER ACTIVE (PROJECT, DATASET) PAIRS
# --------------------------------------------------

discovery_sql = (
    "SELECT p.project_id, p.project_name, "
    "t.dataset_id, t.dataset_name, "
    "t.source_table, t.target_table, t.business_key "
    "FROM " + TABLE_CONFIG_TABLE + " t "
    "JOIN " + PROJECT_CONFIG_TABLE + " p "
    "ON p.project_id = t.project_id "
    "WHERE t.active = true AND p.active = true "
    "ORDER BY p.project_name, t.dataset_name"
)

try:
    active_datasets = [row.asDict() for row in spark.sql(discovery_sql).collect()]
except Exception as exc:
    print("[WARN] Table discovery failed: " + str(exc) + ". Using configured scope.")
    active_datasets = []

if PROJECT_FILTER.lower() != "all":
    active_datasets = [
        d for d in active_datasets
        if d["project_name"].lower() == PROJECT_FILTER.lower()
    ]

if DATASET_FILTER.lower() != "all":
    active_datasets = [
        d for d in active_datasets
        if d["dataset_name"].lower() == DATASET_FILTER.lower()
    ]

print("Active datasets discovered: " + str(len(active_datasets)))
for d in active_datasets:
    print("  - " + d["project_name"] + " / " + d["dataset_name"] + " -> " + d["source_table"])

# COMMAND ----------
# --------------------------------------------------
# 3. BUILD VALIDATION TASKS PER DATASET
# --------------------------------------------------

tasks = []

for dataset in active_datasets:
    dataset_id = int(dataset["dataset_id"])

    # Template-based rules from rendered view
    try:
        template_sql = (
            "SELECT rule_id, rule_name, severity, template_name, "
            "result_type, rendered_sql "
            "FROM " + RENDERED_RULES_VIEW + " "
            "WHERE dataset_id = " + str(dataset_id)
        )
        template_rules = [row.asDict() for row in spark.sql(template_sql).collect()]
    except Exception:
        template_rules = []

    # Legacy free-text rules
    try:
        legacy_sql = (
            "SELECT rule_id, rule_name, category, severity, "
            "threshold, validation_sql "
            "FROM " + RULES_TABLE + " "
            "WHERE active = true "
            "AND template_name IS NULL "
            "AND validation_sql IS NOT NULL "
            "AND (dataset_id IS NULL OR dataset_id = " + str(dataset_id) + ")"
        )
        legacy_rules = [row.asDict() for row in spark.sql(legacy_sql).collect()]
    except Exception:
        legacy_rules = []

    for rule in template_rules:
        tasks.append({"kind": "TEMPLATE", "dataset": dataset, **rule})

    for rule in legacy_rules:
        tasks.append({"kind": "LEGACY", "dataset": dataset, **rule})

print("Total validation tasks queued: " + str(len(tasks)))

# COMMAND ----------
# --------------------------------------------------
# 4. GENERIC DATA QUALITY AGENT (Per-Rule Isolation)
# --------------------------------------------------

def render_legacy_sql(task, dataset):
    sql_text = task["validation_sql"]
    sql_text = sql_text.replace("{{source_table}}", dataset["source_table"])
    sql_text = sql_text.replace("{{target_table}}", dataset["target_table"])
    sql_text = sql_text.replace("{{business_key}}", dataset["business_key"])

    if "{threshold}" in sql_text:
        if task.get("threshold") is None:
            raise ValueError("This rule requires a threshold value.")
        num_thresh = float(task["threshold"])
        if not math.isfinite(num_thresh):
            raise ValueError("Threshold must be a finite number.")
        sql_text = sql_text.replace("{threshold}", str(num_thresh))

    if "{{" in sql_text:
        raise ValueError("Unresolved placeholders in validation_sql.")

    return sql_text

def run_dq_agent(task):
    started_at = datetime.now(timezone.utc)
    timer_start = time.perf_counter()
    dataset = task["dataset"]

    result = {
        "execution_id": execution_id,
        "rule_id": int(task["rule_id"]),
        "rule_name": task["rule_name"],
        "category": task.get("category") or task.get("template_name") or "QualityCheck",
        "status": "ERROR",
        "observed_value": None,
        "expected_value": None,
        "failure_count": 0,
        "error_message": None,
        "executed_at": started_at,
        "dataset_id": int(dataset["dataset_id"]),
        "project_id": int(dataset["project_id"]),
        "severity": task.get("severity") or "High",
        "duration_ms": None,
    }

    try:
        if task["kind"] == "TEMPLATE":
            val_sql = task["rendered_sql"]
        else:
            val_sql = render_legacy_sql(task, dataset)

        row_res = spark.sql(val_sql).first()
        if row_res is None:
            raise ValueError("Validation SQL returned no rows.")

        row_dict = row_res.asDict()
        
        # Determine failure count from output
        if "failed_records" in row_dict:
            raw_count = row_dict["failed_records"]
        elif "failure_count" in row_dict:
            raw_count = row_dict["failure_count"]
        elif "observed_value" in row_dict:
            raw_count = row_dict["observed_value"]
        else:
            raw_count = list(row_dict.values())[0]

        if raw_count is None:
            raise ValueError("Failure count returned NULL.")

        failure_count = int(float(raw_count))
        result["observed_value"] = str(failure_count)
        result["expected_value"] = "0 failed records"
        result["failure_count"] = failure_count
        result["status"] = "PASS" if failure_count == 0 else "FAIL"

        if failure_count > 0:
            result["error_message"] = str(failure_count) + " validation failure(s)"

    except Exception as exc:
        result["status"] = "ERROR"
        result["error_message"] = str(exc)
    finally:
        result["duration_ms"] = int((time.perf_counter() - timer_start) * 1000)

    return result

# COMMAND ----------
# --------------------------------------------------
# 5. EXECUTE TASKS IN PARALLEL & LOG RESULTS
# --------------------------------------------------

results = []

if not tasks:
    print("[INFO] No active rules to execute for the selected scope.")
else:
    worker_count = min(len(tasks), MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(run_dq_agent, task): task for task in tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                res = future.result()
            except Exception as exc:
                ds = task["dataset"]
                res = {
                    "execution_id": execution_id,
                    "rule_id": int(task["rule_id"]),
                    "rule_name": task["rule_name"],
                    "category": task.get("category") or task.get("template_name") or "Error",
                    "status": "ERROR",
                    "observed_value": None,
                    "expected_value": None,
                    "failure_count": 0,
                    "error_message": str(exc),
                    "executed_at": datetime.now(timezone.utc),
                    "dataset_id": int(ds["dataset_id"]),
                    "project_id": int(ds["project_id"]),
                    "severity": task.get("severity") or "High",
                    "duration_ms": None,
                }
            results.append(res)
            print("[" + res["status"] + "] " + res["rule_name"] + " (" + str(res["duration_ms"]) + "ms)")

# COMMAND ----------
# --------------------------------------------------
# 6. PERSIST TO DELTA (dq_results & dq_execution_summary)
# --------------------------------------------------

if results:
    try:
        result_df = spark.createDataFrame(results)
        result_df.write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)
        print("Successfully saved " + str(len(results)) + " rule results to " + RESULTS_TABLE)
    except Exception as exc:
        print("[WARN] Writing to dq_results table: " + str(exc))

# Build Summary Rows
run_ended_at = datetime.now(timezone.utc)
summary_rows = []

for dataset in active_datasets:
    dataset_id = int(dataset["dataset_id"])
    ds_results = [r for r in results if r["dataset_id"] == dataset_id]
    passed = sum(1 for r in ds_results if r["status"] == "PASS")
    failed = sum(1 for r in ds_results if r["status"] == "FAIL")
    errored = sum(1 for r in ds_results if r["status"] == "ERROR")
    total = len(ds_results)

    if errored > 0 and failed == 0:
        overall_status = "ERROR"
    elif failed > 0:
        overall_status = "FAIL"
    else:
        overall_status = "PASS"

    dq_score = round((passed / total * 100.0), 1) if total > 0 else 100.0

    summary_rows.append({
        "execution_id": execution_id,
        "project_id": int(dataset["project_id"]),
        "dataset_id": dataset_id,
        "project_name": dataset["project_name"],
        "dataset_name": dataset["dataset_name"],
        "total_rules": total,
        "passed": passed,
        "failed": failed,
        "errored": errored,
        "started_at": run_started_at,
        "ended_at": run_ended_at,
        "duration_ms": int((run_ended_at - run_started_at).total_seconds() * 1000),
        "overall_status": overall_status,
        "dq_score": dq_score,
    })

if summary_rows:
    try:
        summary_df = spark.createDataFrame(summary_rows)
        summary_df.write.format("delta").mode("append").saveAsTable(SUMMARY_TABLE)
        print("Successfully saved execution summary to " + SUMMARY_TABLE)
    except Exception as exc:
        print("[WARN] Writing to dq_execution_summary table: " + str(exc))

# COMMAND ----------
# --------------------------------------------------
# 7. PHASE 6 INTEGRATION: AUTO-GENERATE ALERTS FOR FAILURES
# --------------------------------------------------

failure_records = [r for r in results if r["status"] in ("FAIL", "ERROR")]
if failure_records:
    print("Generating alerts for " + str(len(failure_records)) + " failures...")
    alert_rows = []
    for f in failure_records:
        ds_match = next((d for d in active_datasets if int(d["dataset_id"]) == f["dataset_id"]), None)
        p_name = ds_match["project_name"] if ds_match else "Unknown"
        d_name = ds_match["dataset_name"] if ds_match else "Unknown"

        alert_rows.append({
            "execution_id": execution_id,
            "project_name": p_name,
            "dataset_name": d_name,
            "rule_name": f["rule_name"],
            "severity": f["severity"],
            "status": "Active",
            "failure_count": f["failure_count"],
            "observed_value": str(f["observed_value"]),
            "expected_value": str(f["expected_value"]),
            "message": f["error_message"] or ("Rule " + f["rule_name"] + " " + f["status"]),
            "created_at": run_started_at,
        })
    try:
        alerts_df = spark.createDataFrame(alert_rows)
        alerts_df.write.format("delta").mode("append").saveAsTable(ALERTS_TABLE)
        print("Alerts written to " + ALERTS_TABLE)
    except Exception as exc:
        print("[WARN] Alert recording: " + str(exc))

print("=" * 60)
print("JOB COMPLETED SUCCESSFULLY: " + execution_id)
print("=" * 60)

