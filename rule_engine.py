"""
Dynamic Data Quality Accelerator - Reusable Rule Engine (Phase 2 & 3)
Provides template registry, SQL rendering, and validation logic.
"""

from typing import Dict, Any, Optional

RULE_TEMPLATES = {
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

def _clean_str(val: Any, fallback: str) -> str:
    if val is None:
        return fallback
    if isinstance(val, float) and (val != val):  # NaN check
        return fallback
    s = str(val).strip()
    return fallback if (not s or s.lower() == "nan" or s.lower() == "none") else s

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
    
    src_str = _clean_str(source_table, "source_table")
    tgt_str = _clean_str(target_table, src_str)
    col_str = _clean_str(column_name, "id")
    param_str = _clean_str(rule_parameter, col_str)
    thresh_str = "0" if (threshold is None or (isinstance(threshold, float) and threshold != threshold)) else str(threshold)

    query = tmpl["sql_template"]
    query = query.replace("{source_table}", src_str)
    query = query.replace("{target_table}", tgt_str)
    query = query.replace("{column_name}", col_str)
    query = query.replace("{rule_parameter}", param_str)
    query = query.replace("{threshold}", thresh_str)
    return query

