"""
Dynamic Data Quality Accelerator (Enterprise Edition)
Reusable framework for data quality validation, reconciliation, alerting, and monitoring.
"""

from accelerator.config import Config
from accelerator.rule_engine import RuleEngine, RULE_TEMPLATES
from accelerator.orchestrator import GenericOrchestrator
from accelerator.reconciliation import ReconciliationEngine
from accelerator.alerts import AlertManager
from accelerator.reporting import ReportGenerator
from accelerator.storage import StorageAdapter

__version__ = "1.0.0"
__all__ = [
    "Config",
    "RuleEngine",
    "RULE_TEMPLATES",
    "GenericOrchestrator",
    "ReconciliationEngine",
    "AlertManager",
    "ReportGenerator",
    "StorageAdapter",
]

