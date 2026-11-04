"""
Reusable Data Quality Rule Engine (Phase 2 & 8)
Encapsulates all 11 DQ rule templates and dynamic SQL rendering logic.
"""

from typing import Dict, Any, Optional

RULE_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "NULL_COUNT": {
        "category": "Completeness",
        "description": "Check for null values in a column",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} IS NULL",
    },
    "NULL_PERCENT": {
        "category": "Completeness",
        "description": "Null percentage check against a threshold",
        "requires_column": True,
        "requires_target": False,
        "result_type": "PERCENT",
        "default_threshold": 0.0,
        "sql_template": "SELECT (COUNT(CASE WHEN {column_name} IS NULL THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)) AS observed_value FROM {source_table}",
    },
    "UNIQUE_COUNT": {
        "category": "Uniqueness",
        "description": "Check uniqueness of values in a column",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT (COUNT(*) - COUNT(DISTINCT {column_name})) AS failure_count FROM {source_table}",
    },
    "DUPLICATE_COUNT": {
        "category": "Uniqueness",
        "description": "Find duplicate records based on business key",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM (SELECT {column_name}, COUNT(*) FROM {source_table} GROUP BY {column_name} HAVING COUNT(*) > 1) t",
    },
    "ROW_COUNT": {
        "category": "Volume",
        "description": "Record count validation against minimum threshold",
        "requires_column": False,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 1,
        "sql_template": "SELECT COUNT(*) AS observed_value FROM {source_table}",
    },
    "BLANK_COUNT": {
        "category": "Completeness",
        "description": "Check for empty string or whitespace records",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE TRIM(CAST({column_name} AS STRING)) = ''",
    },
    "DATA_FRESHNESS": {
        "category": "Freshness",
        "description": "Verify latest timestamp is within allowable age",
        "requires_column": True,
        "requires_target": False,
        "result_type": "HOURS",
        "default_threshold": 24,
        "sql_template": "SELECT DATEDIFF(hour, MAX({column_name}), current_timestamp()) AS observed_value FROM {source_table}",
    },
    "REFERENTIAL_INTEGRITY": {
        "category": "Integrity",
        "description": "Ensure foreign keys exist in parent target table",
        "requires_column": True,
        "requires_target": True,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} s LEFT JOIN {target_table} t ON s.{column_name} = t.{rule_parameter} WHERE t.{rule_parameter} IS NULL",
    },
    "SCHEMA_CHANGE": {
        "category": "Schema",
        "description": "Check table schema consistency against metadata definition",
        "requires_column": False,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE 1=0",
    },
    "REQUIRED_COLUMNS": {
        "category": "Schema",
        "description": "Validate presence of mandatory columns and null safety",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} IS NULL",
    },
    "SOURCE_TARGET_ROW_COUNT": {
        "category": "Reconciliation",
        "description": "Compare total record count between source and target tables",
        "requires_column": False,
        "requires_target": True,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT ABS((SELECT COUNT(*) FROM {source_table}) - (SELECT COUNT(*) FROM {target_table})) AS observed_value",
    },
    "SOURCE_TARGET_MISSING_RECORDS": {
        "category": "Reconciliation",
        "description": "Find source records missing from target table based on business key",
        "requires_column": True,
        "requires_target": True,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} s LEFT JOIN {target_table} t ON s.{column_name} = t.{rule_parameter} WHERE t.{rule_parameter} IS NULL",
    },
    "NEGATIVE_VALUE_CHECK": {
        "category": "Accuracy",
        "description": "Check for negative numerical values in a column",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} < 0",
    },
    "DATE_VALIDATION": {
        "category": "Validity",
        "description": "Check for invalid future dates or dates outside acceptable range",
        "requires_column": True,
        "requires_target": False,
        "result_type": "COUNT",
        "default_threshold": 0,
        "sql_template": "SELECT COUNT(*) AS failure_count FROM {source_table} WHERE {column_name} > current_timestamp()",
    },
}

class RuleEngine:
    @staticmethod
    def get_standard_templates() -> Dict[str, Dict[str, Any]]:
        """Returns the dictionary of 11 standard reusable rule templates."""
        return RULE_TEMPLATES

    @staticmethod
    def render_sql(template_name: str,
                   source_table: str = "source_table",
                   target_table: Optional[str] = "target_table",
                   column_name: Optional[str] = "id",
                   rule_parameter: Optional[str] = "id",
                   threshold: Optional[float] = 0) -> str:
        """Renders executable SQL query for a given rule template."""
        tmpl = RULE_TEMPLATES.get(template_name)
        if not tmpl:
            return f"-- Template '{template_name}' not found."
        
        query = tmpl["sql_template"]
        query = query.replace("{source_table}", source_table or "source_table")
        query = query.replace("{target_table}", target_table or "target_table")
        query = query.replace("{column_name}", column_name or "id")
        query = query.replace("{rule_parameter}", rule_parameter or (column_name or "id"))
        query = query.replace("{threshold}", str(threshold if threshold is not None else 0))
        return query

    render_template = render_sql

    @staticmethod
    def evaluate_result(result_type: str, observed_value: Any, threshold: float = 0.0) -> tuple[str, int, str]:
        """Evaluates status, failure count, and message based on threshold."""
        try:
            val = float(observed_value) if observed_value is not None else 0.0
            if result_type in ("COUNT", "FAILURE_COUNT"):
                failures = int(val)
                status = "PASS" if failures <= threshold else "FAIL"
                msg = None if status == "PASS" else f"{failures} records failed validation (threshold: {threshold})"
                return status, failures, msg
            elif result_type in ("PERCENT", "HOURS"):
                status = "PASS" if val <= threshold else "FAIL"
                failures = 1 if status == "FAIL" else 0
                msg = None if status == "PASS" else f"Observed value {val} exceeded allowable threshold {threshold}"
                return status, failures, msg
            else:
                return "PASS", 0, None
        except Exception as e:
            return "ERROR", 1, f"Evaluation error: {str(e)}"

# Convenience function for root backward compatibility
render_sql = RuleEngine.render_sql

