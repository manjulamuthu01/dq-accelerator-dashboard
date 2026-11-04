"""
Automated Alert Management Module (Phase 6 & 8)
Handles failure notifications, deduplication, and resolution lifecycle.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List

class AlertManager:
    @staticmethod
    def format_alert_payload(execution_id: str,
                             project_name: str,
                             dataset_name: str,
                             rule_name: str,
                             severity: str,
                             failure_count: int,
                             observed_value: str,
                             expected_value: str,
                             message: str) -> Dict[str, Any]:
        """Creates a standardized alert payload dictionary."""
        now = datetime.now().strftime("%d %b %Y, %I:%M %p")
        return {
            "execution_id": execution_id,
            "project_name": project_name,
            "dataset_name": dataset_name,
            "rule_name": rule_name,
            "severity": severity,
            "status": "Active",
            "failure_count": failure_count,
            "observed_value": str(observed_value),
            "expected_value": str(expected_value),
            "message": message,
            "acknowledged_by": None,
            "acknowledged_at": None,
            "closed_by": None,
            "closed_at": None,
            "created_at": now,
        }

    @staticmethod
    def acknowledge_payload(alert_id: int, user: str = "admin") -> Dict[str, Any]:
        now = datetime.now().strftime("%d %b %Y, %I:%M %p")
        return {
            "alert_id": alert_id,
            "status": "Acknowledged",
            "acknowledged_by": user,
            "acknowledged_at": now,
        }

    @staticmethod
    def close_payload(alert_id: int, user: str = "admin") -> Dict[str, Any]:
        now = datetime.now().strftime("%d %b %Y, %I:%M %p")
        return {
            "alert_id": alert_id,
            "status": "Closed",
            "closed_by": user,
            "closed_at": now,
        }

