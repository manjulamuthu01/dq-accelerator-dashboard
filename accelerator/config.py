"""
Configuration and Environment Management Module
"""

import os
from typing import Dict, Any

class Config:
    SCHEMA = os.environ.get("DQ_CATALOG_SCHEMA", "hospital_catalog.healthcare")
    DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
    DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")
    DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
    DQ_JOB_ID = os.environ.get("DQ_JOB_ID", "798854749746341")
    MAX_WORKERS = int(os.environ.get("DQ_MAX_WORKERS", "3"))

    # Unity Catalog Tables
    T_PROJECTS = SCHEMA + ".dq_project_config"
    T_DATASETS = SCHEMA + ".dq_table_config"
    T_RULES = SCHEMA + ".dq_rules"
    T_TEMPLATES = SCHEMA + ".dq_rule_templates"
    T_RENDERED_VIEW = SCHEMA + ".dq_rules_rendered"
    T_RESULTS = SCHEMA + ".dq_results"
    T_SUMMARY = SCHEMA + ".dq_execution_summary"
    T_RECON_CONFIG = SCHEMA + ".dq_reconciliation_config"
    T_RECON_RESULTS = SCHEMA + ".dq_reconciliation_results"
    T_RECON_SAMPLES = SCHEMA + ".dq_reconciliation_samples"
    T_ALERTS = SCHEMA + ".dq_alerts"
    T_CONFIG_AUDIT = SCHEMA + ".dq_config_audit"
    T_JOB_RUNS = SCHEMA + ".dq_job_runs"

    SEVERITIES = ["Critical", "High", "Medium", "Low"]
    STATUSES = ["PASS", "FAIL", "ERROR"]

    @classmethod
    def get_table_name(cls, table_key: str) -> str:
        return getattr(cls, f"T_{table_key.upper()}", f"{cls.SCHEMA}.{table_key}")

