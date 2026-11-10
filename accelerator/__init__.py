"""
Dynamic Data Quality Accelerator - Package Init
Only exposes GenericOrchestrator which is the only module with real implementation.
All other logic (alerts, reporting, reconciliation, storage, config) lives in db_adapter.py.
"""

from accelerator.orchestrator import GenericOrchestrator
from accelerator.databricks_connection import (
    DatabricksConnectionManager,
    UserWorkspaceSession,
    get_connection_manager,
)

__version__ = "1.0.0"
__all__ = [
    "GenericOrchestrator",
    "DatabricksConnectionManager",
    "UserWorkspaceSession",
    "get_connection_manager",
]
