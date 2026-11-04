"""
Source-to-Target Reconciliation Engine (Phase 4 & 8)
Scalable reconciliation comparisons without collecting big data to driver.
"""

from typing import Dict, Any, List, Optional
import pandas as pd

class ReconciliationEngine:
    @staticmethod
    def build_reconciliation_queries(source_table: str,
                                     target_table: str,
                                     business_key: str,
                                     compare_columns: Optional[List[str]] = None) -> Dict[str, str]:
        """
        Generates scalable Spark SQL / SQL queries for reconciliation tasks.
        """
        cols = compare_columns or []
        col_checks = ""
        if cols:
            col_checks = " OR " + " OR ".join([f"s.{c} <=> t.{c} = false" for c in cols])

        return {
            "source_count": f"SELECT COUNT(*) AS total_count FROM {source_table}",
            "target_count": f"SELECT COUNT(*) AS total_count FROM {target_table}",
            "missing_in_target": (
                f"SELECT COUNT(*) AS failure_count FROM {source_table} s "
                f"LEFT JOIN {target_table} t ON s.{business_key} = t.{business_key} "
                f"WHERE t.{business_key} IS NULL"
            ),
            "extra_in_target": (
                f"SELECT COUNT(*) AS failure_count FROM {target_table} t "
                f"LEFT JOIN {source_table} s ON t.{business_key} = s.{business_key} "
                f"WHERE s.{business_key} IS NULL"
            ),
            "duplicate_keys": (
                f"SELECT COUNT(*) AS failure_count FROM ("
                f"SELECT {business_key}, COUNT(*) FROM {target_table} "
                f"GROUP BY {business_key} HAVING COUNT(*) > 1) d"
            ),
            "sample_missing": (
                f"SELECT s.{business_key} FROM {source_table} s "
                f"LEFT JOIN {target_table} t ON s.{business_key} = t.{business_key} "
                f"WHERE t.{business_key} IS NULL LIMIT 10"
            ),
        }

    @staticmethod
    def compute_reconciliation_summary(source_count: int,
                                       target_count: int,
                                       missing: int = 0,
                                       extra: int = 0,
                                       duplicate: int = 0) -> Dict[str, Any]:
        """Computes high-level reconciliation summary stats."""
        diff = abs(source_count - target_count)
        matching = max(0, min(source_count, target_count) - missing)
        status = "PASS" if (diff == 0 and missing == 0 and extra == 0 and duplicate == 0) else "FAIL"
        
        return {
            "source_count": source_count,
            "target_count": target_count,
            "difference": diff,
            "matching_records": matching,
            "missing_in_target": missing,
            "extra_in_target": extra,
            "duplicate_keys": duplicate,
            "status": status,
        }

