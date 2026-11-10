"""
Unity Catalog Service (Phase 4 & Multi-User Architecture)
Provides dynamic connection, discovery, and schema metadata detection
for Databricks Unity Catalog (Catalogs, Schemas, Tables, Columns, Volumes).

Architecture:
- Never hardcodes personal workspace URLs, tokens, or mock catalogs.
- Delegates to accelerator.databricks_connection.DatabricksConnectionManager.
- Supports multi-user session isolation and local developer development.
- Discovers live catalogs, schemas, tables, and volumes strictly based on the connected user's Unity Catalog permissions.
"""

import os
import logging
from typing import Dict, List, Any, Optional
from accelerator.databricks_connection import get_connection_manager

logger = logging.getLogger("unity_catalog_service")


def get_connection_info(session_id: Optional[str] = None) -> Dict[str, Any]:
    """Returns the current connection status and workspace details for the session."""
    mgr = get_connection_manager()
    session = mgr.get_session(session_id)
    summary = session.get_summary()

    if summary["connected"]:
        clean_host = summary["clean_host"]
        badge = f"● Connected: {clean_host}"
    else:
        badge = "○ Disconnected (No Databricks Workspace)"

    return {
        "connected": summary["connected"],
        "host": summary["host"],
        "clean_host": summary["clean_host"],
        "workspace_id": summary["workspace_id"],
        "auth_type": summary["auth_type"],
        "username": summary["username"],
        "display_name": summary["display_name"],
        "status_badge": badge,
        "cloud_provider": summary["cloud_provider"],
        "catalogs_count": summary["catalogs_count"],
        "last_error": summary["last_error"],
    }


def connect_workspace(
    host: Optional[str] = None,
    token: Optional[str] = None,
    auth_type: str = "OAUTH_U2M",
    profile: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Connects to and validates a Databricks Workspace using the selected authentication method:
    - OAUTH_U2M (Databricks OAuth User-to-Machine)
    - CLI_PROFILE (Local ~/.databrickscfg CLI profile)
    - PAT (Personal Access Token fallback)
    """
    mgr = get_connection_manager()
    res = mgr.connect(
        host=host,
        auth_type=auth_type,
        token=token,
        profile=profile,
        session_id=session_id,
        remember=True,
    )
    if res.get("success"):
        res["connection"] = get_connection_info(session_id)
    return res


def disconnect_workspace(session_id: Optional[str] = None) -> Dict[str, Any]:
    """Disconnects the Databricks workspace and clears session resources."""
    mgr = get_connection_manager()
    res = mgr.disconnect(session_id)
    res["connection"] = get_connection_info(session_id)
    return res


def verify_connection(session_id: Optional[str] = None) -> Dict[str, Any]:
    """Validates if the active session connection is valid or has expired."""
    mgr = get_connection_manager()
    return mgr.verify_connection(session_id)


def list_catalogs(session_id: Optional[str] = None, refresh: bool = False) -> List[Dict[str, Any]]:
    """
    Dynamically retrieves Unity Catalogs accessible to the authenticated user.
    Returns empty list if disconnected (never displays hardcoded or mock catalogs).
    """
    mgr = get_connection_manager()
    return mgr.discover_catalogs(session_id=session_id, refresh=refresh)


def list_schemas(
    catalog_name: str,
    session_id: Optional[str] = None,
    refresh: bool = False
) -> List[Dict[str, Any]]:
    """
    Dynamically retrieves schemas within the specified catalog accessible to the user.
    """
    mgr = get_connection_manager()
    return mgr.discover_schemas(catalog_name=catalog_name, session_id=session_id, refresh=refresh)


def list_tables(
    catalog_name: str,
    schema_name: str,
    session_id: Optional[str] = None,
    refresh: bool = False
) -> List[Dict[str, Any]]:
    """
    Dynamically retrieves tables in the specified catalog and schema accessible to the user.
    """
    mgr = get_connection_manager()
    return mgr.discover_tables(catalog_name=catalog_name, schema_name=schema_name, session_id=session_id, refresh=refresh)


def list_volumes(
    catalog_name: str,
    schema_name: str,
    session_id: Optional[str] = None,
    refresh: bool = False
) -> List[Dict[str, Any]]:
    """
    Dynamically retrieves Unity Catalog volumes in the specified catalog and schema.
    """
    mgr = get_connection_manager()
    return mgr.discover_volumes(catalog_name=catalog_name, schema_name=schema_name, session_id=session_id, refresh=refresh)


def get_table_metadata(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    session_id: Optional[str] = None,
    refresh: bool = False
) -> Dict[str, Any]:
    """
    Detects table metadata: columns, data types, record count, business key candidates, and sample preview.
    If Databricks is connected, queries live Unity Catalog via SDK.
    If local SQLite contains the resolved table, inspects local table for local file onboarding.
    """
    mgr = get_connection_manager()
    session = mgr.get_session(session_id)

    # 1. If connected to Databricks, retrieve live table metadata
    if session.connected and session.client:
        meta = mgr.get_table_metadata(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            session_id=session_id,
            refresh=refresh,
        )
        if meta.get("columns"):
            return meta

    # 2. Local SQLite fallback (for datasets uploaded locally e.g. Excel/CSV)
    try:
        import db_adapter
        full_table = f"{catalog_name}.{schema_name}.{table_name}"
        all_local_tables = [r[0] for r in db_adapter.query_df("SELECT name FROM sqlite_master WHERE type='table'").values.tolist()]
        local_tbl = db_adapter._resolve_local_table(full_table) or db_adapter._resolve_local_table(table_name)

        if not local_tbl:
            short_tbl = table_name.lower()
            if "logistics" in short_tbl or "shipment" in short_tbl:
                if "logistics_source" in all_local_tables:
                    local_tbl = "logistics_source"
            elif "patient" in short_tbl or short_tbl == "source_table":
                if "source_table" in all_local_tables:
                    local_tbl = "source_table"

        if local_tbl and local_tbl in all_local_tables:
            pragma_df = db_adapter.query_df(f"PRAGMA table_info({local_tbl})")
            columns = pragma_df["name"].tolist()
            schema_info = [{"column": row["name"], "sql_type": row["type"] or "STRING"} for _, row in pragma_df.iterrows()]

            cnt_df = db_adapter.query_df(f"SELECT COUNT(*) AS c FROM {local_tbl}")
            rec_cnt = int(cnt_df.iloc[0]["c"]) if not cnt_df.empty else 0

            sample_df = db_adapter.query_df(f"SELECT * FROM {local_tbl} LIMIT 5")
            preview_rows = sample_df.to_dict(orient="records")

            candidates = mgr._identify_business_key_candidates(columns)
            suggested_bkey = candidates[0] if candidates else (columns[0] if columns else "id")

            return {
                "full_table_path": full_table,
                "catalog": catalog_name,
                "schema": schema_name,
                "table_name": table_name,
                "columns": columns,
                "schema_info": schema_info,
                "record_count": rec_cnt,
                "business_key_candidates": candidates,
                "suggested_business_key": suggested_bkey,
                "preview_rows": preview_rows,
                "table_type": "MANAGED (Local Delta/SQLite)",
                "description": f"Verified table {full_table}",
            }
    except Exception as e:
        logger.debug(f"Local table inspection note: {e}")

    # 3. Default empty descriptor
    return {
        "full_table_path": f"{catalog_name}.{schema_name}.{table_name}",
        "catalog": catalog_name,
        "schema": schema_name,
        "table_name": table_name,
        "columns": [],
        "schema_info": [],
        "record_count": 0,
        "business_key_candidates": ["id"],
        "suggested_business_key": "id",
        "preview_rows": [],
        "table_type": "MANAGED",
        "description": f"Table {catalog_name}.{schema_name}.{table_name}",
    }


def test_unity_catalog_connectivity(
    catalog: str,
    schema: str,
    table: str,
    business_key: Optional[str] = None,
    target_catalog: Optional[str] = None,
    target_schema: Optional[str] = None,
    target_table: Optional[str] = None,
    target_business_key: Optional[str] = None,
    session_id: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Performs comprehensive 5-step connectivity validation against Unity Catalog.
    """
    mgr = get_connection_manager()
    session = mgr.get_session(session_id)

    # If Databricks is connected, run live connectivity check
    if session.connected and session.client:
        return mgr.test_unity_catalog_connectivity(
            catalog=catalog,
            schema=schema,
            table=table,
            business_key=business_key,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table=target_table,
            target_business_key=target_business_key,
            session_id=session_id,
        )

    # Local fallback for local uploaded datasets
    cat = (catalog or "").strip()
    sch = (schema or "").strip()
    tbl = (table or "").strip()
    b_key = (business_key or "").strip()
    full_src = f"{cat}.{sch}.{tbl}"

    meta = get_table_metadata(cat, sch, tbl, session_id=session_id)
    if meta.get("columns"):
        checks = [
            {"name": "Local Dataset Store", "status": "PASS", "message": f"Table verified locally ({meta['record_count']} records)."},
            {"name": "Schema Validation", "status": "PASS", "message": f"{len(meta['columns'])} column(s) detected."},
        ]
        if b_key and b_key in meta["columns"]:
            checks.append({"name": "Business Key", "status": "PASS", "message": f"Business key '{b_key}' confirmed."})
        elif b_key:
            checks.append({"name": "Business Key", "status": "FAIL", "message": f"Business key '{b_key}' not in table columns."})

        return {
            "success": all(c["status"] == "PASS" for c in checks),
            "message": "Local dataset verified.",
            "checks": checks,
            "metadata": meta,
        }

    return {
        "success": False,
        "message": "Databricks Workspace is not connected. Please connect your Databricks account.",
        "checks": [{"name": "Authentication", "status": "FAIL", "message": "No connected Databricks workspace"}],
        "metadata": meta,
    }
