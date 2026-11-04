"""
Reporting and Export Engine (Phase 6 & 8)
Generates downloadable intelligence across multiple operational dimensions.
"""

from typing import Dict, Any, List
import pandas as pd

class ReportGenerator:
    REPORT_TYPES = {
        "PROJECT_WISE": "Project-wise Summary Report",
        "DATASET_WISE": "Dataset-wise Definitions & Health",
        "RULE_WISE": "Rule-wise Detailed Validation Results",
        "EXECUTION_WISE": "Execution-wise Run History & Durations",
    }

    @staticmethod
    def get_report_sql(report_type: str, tables: Dict[str, str]) -> str:
        """Returns optimized SQL query string for specified report type."""
        t_proj = tables.get("T_PROJECTS", "dq_project_config")
        t_ds = tables.get("T_DATASETS", "dq_table_config")
        t_rules = tables.get("T_RULES", "dq_rules")
        t_sum = tables.get("T_SUMMARY", "dq_execution_summary")
        t_res = tables.get("T_RESULTS", "dq_results")

        if report_type == "PROJECT_WISE":
            return f"""
            SELECT p.project_id, p.project_name, p.description, p.catalog_name, p.schema_name,
                   CASE WHEN p.active = 1 THEN 'Active' ELSE 'Inactive' END AS status,
                   COUNT(DISTINCT d.dataset_id) AS total_datasets,
                   COUNT(DISTINCT r.rule_id) AS configured_rules,
                   ROUND(AVG(s.dq_score), 1) AS avg_dq_score
            FROM {t_proj} p
            LEFT JOIN {t_ds} d ON p.project_id = d.project_id
            LEFT JOIN {t_rules} r ON p.project_id = r.project_id
            LEFT JOIN {t_sum} s ON p.project_name = s.project_name
            GROUP BY p.project_id, p.project_name, p.description, p.catalog_name, p.schema_name, p.active
            ORDER BY p.project_id ASC
            """
        elif report_type == "DATASET_WISE":
            return f"""
            SELECT d.dataset_id, p.project_name, d.dataset_name, d.source_table, d.target_table,
                   d.business_key, CASE WHEN d.active = 1 THEN 'Active' ELSE 'Inactive' END AS status,
                   COUNT(r.rule_id) AS active_rules,
                   d.created_at
            FROM {t_ds} d
            JOIN {t_proj} p ON d.project_id = p.project_id
            LEFT JOIN {t_rules} r ON d.dataset_id = r.dataset_id AND r.active = 1
            GROUP BY d.dataset_id, p.project_name, d.dataset_name, d.source_table, d.target_table, d.business_key, d.active, d.created_at
            ORDER BY p.project_name, d.dataset_name
            """
        elif report_type == "RULE_WISE":
            return f"""
            SELECT r.execution_id, p.project_name, d.dataset_name, r.rule_name, r.category,
                   r.severity, r.status, r.failure_count, r.observed_value, r.expected_value,
                   r.error_message, r.duration_ms, r.executed_at
            FROM {t_res} r
            LEFT JOIN {t_proj} p ON r.project_id = p.project_id
            LEFT JOIN {t_ds} d ON r.dataset_id = d.dataset_id
            ORDER BY r.result_id DESC
            """
        else:  # EXECUTION_WISE
            return f"""
            SELECT execution_id, project_name, dataset_name, total_rules, passed, failed, errored,
                   overall_status, dq_score, started_at, ended_at, duration_ms
            FROM {t_sum}
            ORDER BY started_at DESC
            """

