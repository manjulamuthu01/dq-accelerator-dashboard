# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 03 - Generic DQ Validation Orchestrator Job Task
# MAGIC **Dynamic Data Quality Accelerator (Phases 3, 7 & 8)**
# MAGIC 
# MAGIC Executes generic, config-driven DQ validations across active datasets or scoped by widgets (`project_name`, `dataset_name`).
# MAGIC Runs parallel agents with failure isolation and persists results, summaries, and alerts.
# MAGIC Emits `execution_id` via Databricks Jobs TaskValues to downstream tasks (e.g. `dq_reconciliation`).
# MAGIC Propagates fatal infrastructure exceptions so Databricks automatically triggers task retries.

# COMMAND ----------
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import uuid
import math
import time
import os

# --------------------------------------------------
# 1. CONFIGURATION & WIDGETS
# --------------------------------------------------

SCHEMA = os.environ.get("DQ_CATALOG_SCHEMA", "hospital_catalog.healthcare")

PROJECT_CONFIG_TABLE = SCHEMA + ".dq_project_config"
TABLE_CONFIG_TABLE   = SCHEMA + ".dq_table_config"
RULES_TABLE          = SCHEMA + ".dq_rules"
RENDERED_RULES_VIEW  = SCHEMA + ".dq_rules_rendered"
RESULTS_TABLE        = SCHEMA + ".dq_results"
SUMMARY_TABLE        = SCHEMA + ".dq_execution_summary"
ALERTS_TABLE         = SCHEMA + ".dq_alerts"

MAX_WORKERS = 3

try:
    dbutils.widgets.text("project_name", "all", "Project filter (name or all)")
    dbutils.widgets.text("dataset_name", "all", "Dataset filter (name or all)")
    dbutils.widgets.text("execution_id", "", "Execution ID (optional)")
    dbutils.widgets.text("skip_recon_on_dq_fail", "false", "Skip downstream reconciliation if DQ fails (true/false)")

    PROJECT_FILTER = (dbutils.widgets.get("project_name") or "all").strip()
    DATASET_FILTER = (dbutils.widgets.get("dataset_name") or "all").strip()
    in_exec_id = (dbutils.widgets.get("execution_id") or "").strip()
    skip_recon_cfg = (dbutils.widgets.get("skip_recon_on_dq_fail") or "false").strip().lower() == "true"
except Exception:
    PROJECT_FILTER = "all"
    DATASET_FILTER = "all"
    in_exec_id = ""
    skip_recon_cfg = False

if in_exec_id and not in_exec_id.startswith("{{"):
    execution_id = in_exec_id
else:
    execution_id = "EXEC-" + datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + str(uuid.uuid4())[:8].upper()

run_started_at = datetime.now(timezone.utc)

# Share execution_id with downstream tasks (e.g. dq_reconciliation)
try:
    dbutils.jobs.taskValues.set(key="execution_id", value=execution_id)
except Exception:
    pass

print("=" * 60)
print(f"DQ VALIDATION TASK: {execution_id}")
print(f"Scope: project_name={PROJECT_FILTER}, dataset_name={DATASET_FILTER}")
print("=" * 60)

# --------------------------------------------------
# 2. DISCOVER ACTIVE DATASETS
# --------------------------------------------------
discovery_sql = f"""
SELECT p.project_id, p.project_name,
       t.dataset_id, t.dataset_name,
       t.source_table, t.target_table, t.business_key
FROM {TABLE_CONFIG_TABLE} t
JOIN {PROJECT_CONFIG_TABLE} p ON p.project_id = t.project_id
WHERE t.active = true AND p.active = true
ORDER BY p.project_name, t.dataset_name
"""

# Fatal infrastructure exceptions MUST raise so Databricks task retries apply!
try:
    active_datasets = [row.asDict() for row in spark.sql(discovery_sql).collect()]
except Exception as e:
    print(f"[FATAL] Metadata discovery failed: {e}")
    raise RuntimeError(f"Transient or infrastructure error during dataset discovery: {e}")

if PROJECT_FILTER.lower() != "all":
    active_datasets = [d for d in active_datasets if d["project_name"].lower() == PROJECT_FILTER.lower()]

if DATASET_FILTER.lower() != "all":
    active_datasets = [d for d in active_datasets if d["dataset_name"].lower() == DATASET_FILTER.lower()]

print(f"Active datasets queued: {len(active_datasets)}")

# --------------------------------------------------
# 3. COMPILE VALIDATION TASKS
# --------------------------------------------------
tasks = []
for dataset in active_datasets:
    dataset_id = int(dataset["dataset_id"])
    template_sql = f"""
    SELECT rule_id, rule_name, severity, template_name, result_type, rendered_sql
    FROM {RENDERED_RULES_VIEW}
    WHERE dataset_id = {dataset_id}
    """
    try:
        rules = [row.asDict() for row in spark.sql(template_sql).collect()]
        for r in rules:
            tasks.append({"dataset": dataset, **r})
    except Exception as e:
        print(f"[WARN] Error compiling rules for dataset {dataset['dataset_name']}: {e}")

print(f"Total validation tasks queued: {len(tasks)}")

# --------------------------------------------------
# 4. PARALLEL AGENT EXECUTION
# --------------------------------------------------
def run_agent(task):
    t_start = time.perf_counter()
    ds = task["dataset"]
    rule_name = task["rule_name"]
    result_type = task.get("result_type", "COUNT")

    try:
        df = spark.sql(task["rendered_sql"])
        row = df.collect()[0].asDict()
        duration_ms = int((time.perf_counter() - t_start) * 1000)

        if result_type in ("COUNT", "FAILURE_COUNT"):
            val = row.get("failure_count") if "failure_count" in row else list(row.values())[0]
            val = 0 if val is None or (isinstance(val, float) and math.isnan(val)) else int(val)
            status = "PASS" if val == 0 else "FAIL"
            err = None if status == "PASS" else f"{val} records failed rule validation"
            return {
                "rule_id": task["rule_id"],
                "rule_name": rule_name,
                "project_id": ds["project_id"],
                "dataset_id": ds["dataset_id"],
                "category": task.get("category", "General"),
                "severity": task.get("severity", "High"),
                "status": status,
                "failure_count": val,
                "observed_value": str(val),
                "expected_value": "0",
                "error_message": err,
                "duration_ms": duration_ms,
            }
        elif result_type in ("PERCENT", "HOURS"):
            val = list(row.values())[0]
            val = 0.0 if val is None or (isinstance(val, float) and math.isnan(val)) else float(val)
            thresh = float(task.get("threshold", 0))
            status = "PASS" if val <= thresh else "FAIL"
            err = None if status == "PASS" else f"Observed {val} exceeded threshold {thresh}"
            return {
                "rule_id": task["rule_id"],
                "rule_name": rule_name,
                "project_id": ds["project_id"],
                "dataset_id": ds["dataset_id"],
                "category": task.get("category", "General"),
                "severity": task.get("severity", "High"),
                "status": status,
                "failure_count": 1 if status == "FAIL" else 0,
                "observed_value": str(val),
                "expected_value": f"<= {thresh}",
                "error_message": err,
                "duration_ms": duration_ms,
            }
        else:
            return {
                "rule_id": task["rule_id"],
                "rule_name": rule_name,
                "project_id": ds["project_id"],
                "dataset_id": ds["dataset_id"],
                "category": task.get("category", "General"),
                "severity": task.get("severity", "High"),
                "status": "PASS",
                "failure_count": 0,
                "observed_value": "0",
                "expected_value": "0",
                "error_message": None,
                "duration_ms": duration_ms,
            }
    except Exception as e:
        duration_ms = int((time.perf_counter() - t_start) * 1000)
        return {
            "rule_id": task["rule_id"],
            "rule_name": rule_name,
            "project_id": ds["project_id"],
            "dataset_id": ds["dataset_id"],
            "category": task.get("category", "General"),
            "severity": task.get("severity", "High"),
            "status": "ERROR",
            "failure_count": 1,
            "observed_value": "ERROR",
            "expected_value": "0",
            "error_message": str(e),
            "duration_ms": duration_ms,
        }

results = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(run_agent, t) for t in tasks]
    for f in as_completed(futures):
        results.append(f.result())

run_ended_at = datetime.now(timezone.utc)

# --------------------------------------------------
# 5. PERSISTENCE (Delta Tables with Idempotency)
# --------------------------------------------------
now_ts = run_started_at.strftime("%Y-%m-%d %H:%M:%S")

try:
    if results:
        res_rows = [{**r, "execution_id": execution_id, "executed_at": run_started_at} for r in results]
        spark.createDataFrame(res_rows).write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)

    summary_rows = []
    for dataset in active_datasets:
        ds_id = int(dataset["dataset_id"])
        ds_results = [r for r in results if r["dataset_id"] == ds_id]
        passed = sum(1 for r in ds_results if r["status"] == "PASS")
        failed = sum(1 for r in ds_results if r["status"] == "FAIL")
        errored = sum(1 for r in ds_results if r["status"] == "ERROR")
        total = len(ds_results)
        status = "PASS" if (failed == 0 and errored == 0) else ("ERROR" if errored > 0 and failed == 0 else "FAIL")
        score = round(passed / total * 100.0, 1) if total > 0 else 100.0

        summary_rows.append({
            "execution_id": execution_id,
            "project_id": int(dataset["project_id"]),
            "dataset_id": ds_id,
            "project_name": dataset["project_name"],
            "dataset_name": dataset["dataset_name"],
            "total_rules": total,
            "passed": passed,
            "failed": failed,
            "errored": errored,
            "started_at": run_started_at,
            "ended_at": run_ended_at,
            "duration_ms": int((run_ended_at - run_started_at).total_seconds() * 1000),
            "overall_status": status,
            "dq_score": score,
        })

    if summary_rows:
        spark.createDataFrame(summary_rows).write.format("delta").mode("append").saveAsTable(SUMMARY_TABLE)

    # Trigger alerts for failures
    failures = [r for r in results if r["status"] in ("FAIL", "ERROR")]
    if failures:
        alert_rows = []
        for f in failures:
            ds_match = next((d for d in active_datasets if int(d["dataset_id"]) == f["dataset_id"]), None)
            alert_rows.append({
                "execution_id": execution_id,
                "project_name": ds_match["project_name"] if ds_match else "Unknown",
                "dataset_name": ds_match["dataset_name"] if ds_match else "Unknown",
                "rule_name": f["rule_name"],
                "severity": f["severity"],
                "status": "Active",
                "failure_count": f["failure_count"],
                "observed_value": str(f["observed_value"]),
                "expected_value": str(f["expected_value"]),
                "message": f["error_message"],
                "created_at": run_started_at,
            })
        spark.createDataFrame(alert_rows).write.format("delta").mode("append").saveAsTable(ALERTS_TABLE)
        print(f"Auto-generated {len(alert_rows)} alerts in {ALERTS_TABLE}")
except Exception as e:
    print(f"[FATAL] Failed writing validation results to Delta Lake: {e}")
    raise RuntimeError(f"Database write failure in dq_orchestrator, triggering retry: {e}")

# --------------------------------------------------
# 6. TASK STATUS & DOWNSTREAM GATING
# --------------------------------------------------
has_critical_failure = any(r["status"] in ("FAIL", "ERROR") and r.get("severity") == "Critical" for r in results)
dq_overall_status = "FAIL" if any(r["status"] in ("FAIL", "ERROR") for r in results) else "PASS"

try:
    dbutils.jobs.taskValues.set(key="dq_overall_status", value=dq_overall_status)
    dbutils.jobs.taskValues.set(key="has_critical_failure", value=has_critical_failure)
except Exception:
    pass

if skip_recon_cfg and has_critical_failure:
    print("[WARNING] Critical DQ validation failure detected and skip_recon_on_dq_fail=true.")
    raise RuntimeError("DQ Validation detected Critical failures; skipping downstream reconciliation per workflow configuration.")

print(f"Validation task {execution_id} finished successfully! Overall status: {dq_overall_status}")
