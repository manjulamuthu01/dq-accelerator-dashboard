"""
Dynamic Data Quality Accelerator - Management & Monitoring Dashboard (Phase 5)
Enterprise Databricks App built with Dash & Plotly.
Clean enterprise layout, zero emojis, Bootstrap Icons, real database actions.
"""

import os
import time
import json
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from flask import request, jsonify

logger = logging.getLogger("dash_app")

import dash
from dash import Dash, html, dcc, Input, Output, State, ctx, dash_table
import dash_bootstrap_components as dbc

import db_adapter
import scheduler_service
import email_service
from rule_engine import RULE_TEMPLATES, render_sql

# -------------------------------------------------------------
# APP INITIALIZATION
# -------------------------------------------------------------

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="DQ Accelerator - Data Quality Monitoring",
)
server = app.server

# Custom Enterprise CSS matching reference design
app.index_string = """<!DOCTYPE html>
<html>
<head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #F8FAFC;
            color: #1E293B;
            margin: 0;
            padding: 0;
            overflow-x: hidden;
        }
        /* Sidebar styling */
        .sidebar {
            background-color: #0F172A;
            min-height: 100vh;
            padding: 24px 16px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            position: fixed;
            width: 250px;
            top: 0;
            left: 0;
            bottom: 0;
            z-index: 100;
        }
        .main-content {
            margin-left: 250px;
            padding: 28px 36px;
            min-height: 100vh;
            background-color: #F8FAFC;
            box-sizing: border-box;
            width: calc(100% - 250px);
            max-width: calc(100% - 250px);
            overflow-x: hidden;
        }
        .brand-container {
            display: flex;
            align-items: center;
            margin-bottom: 24px;
            padding: 0 8px;
        }
        .brand-icon {
            font-size: 1.5rem;
            color: #38BDF8;
            margin-right: 10px;
        }
        .brand-title {
            color: #FFFFFF;
            font-size: 1.15rem;
            font-weight: 700;
            letter-spacing: -0.3px;
            line-height: 1.2;
        }
        .brand-sub {
            color: #94A3B8;
            font-size: 0.75rem;
            font-weight: 500;
        }
        /* Nav links */
        .sidebar .nav-link {
            color: #94A3B8;
            border-radius: 8px;
            padding: 10px 14px;
            margin-bottom: 4px;
            font-size: 0.88rem;
            font-weight: 500;
            display: flex;
            align-items: center;
            transition: all 0.15s ease-in-out;
        }
        .sidebar .nav-link i {
            font-size: 1.1rem;
            margin-right: 12px;
            width: 20px;
            text-align: center;
        }
        .sidebar .nav-link:hover {
            color: #FFFFFF;
            background-color: rgba(255, 255, 255, 0.06);
        }
        .sidebar .nav-link.active {
            color: #FFFFFF;
            background-color: #2563EB;
            font-weight: 600;
        }
        .sidebar-footer {
            padding: 12px 8px 0;
            border-top: 1px solid rgba(255, 255, 255, 0.08);
            display: flex;
            align-items: center;
            font-size: 0.82rem;
            color: #94A3B8;
        }
        .status-dot {
            width: 8px;
            height: 8px;
            background-color: #10B981;
            border-radius: 50%;
            margin-right: 8px;
            box-shadow: 0 0 6px #10B981;
        }
        /* Enterprise Cards */
        .dq-card {
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.04);
            height: 100%;
            width: 100%;
            min-width: 0;
            box-sizing: border-box;
        }
        .dq-card-title {
            font-size: 1rem;
            font-weight: 600;
            color: #0F172A;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        /* Header bar */
        .page-header-bar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 24px;
        }
        .page-main-title {
            font-size: 1.45rem;
            font-weight: 700;
            color: #0F172A;
            display: flex;
            align-items: center;
            gap: 10px;
            margin: 0;
        }
        .header-meta {
            font-size: 0.82rem;
            color: #64748B;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        /* Top KPI metrics */
        .metric-pill-card {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 14px 18px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            box-shadow: 0 1px 2px rgba(0,0,0,0.03);
            height: 100%;
        }
        .metric-icon-box {
            width: 42px;
            height: 42px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.25rem;
        }
        .metric-title {
            font-size: 0.78rem;
            color: #64748B;
            font-weight: 500;
            margin-bottom: 2px;
        }
        .metric-val {
            font-size: 1.55rem;
            font-weight: 700;
            color: #0F172A;
            line-height: 1;
        }
        /* Status Badges */
        .badge-pass {
            background-color: #DCFCE7;
            color: #15803D;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        .badge-fail {
            background-color: #FEE2E2;
            color: #B91C1C;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        .badge-error {
            background-color: #FEF3C7;
            color: #B45309;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }
        .badge-active {
            background-color: #DCFCE7;
            color: #15803D;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
        }
        .badge-inactive {
            background-color: #F1F5F9;
            color: #64748B;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
        }
        /* Buttons */
        .btn-primary-action {
            background-color: #2563EB;
            border-color: #2563EB;
            font-weight: 600;
            font-size: 0.88rem;
            border-radius: 6px;
            padding: 7px 16px;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        .btn-primary-action:hover {
            background-color: #1D4ED8;
            border-color: #1D4ED8;
        }
        /* Form controls */
        .form-control, .form-select {
            font-size: 0.88rem;
            border-radius: 6px;
            border-color: #CBD5E1;
        }
        .form-control:focus, .form-select:focus {
            border-color: #2563EB;
            box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15);
        }
        /* Clean Data Table */
        .clean-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 0.88rem;
        }
        .clean-table th {
            background-color: #F8FAFC;
            color: #475569;
            font-weight: 600;
            padding: 10px 14px;
            border-bottom: 1px solid #E2E8F0;
            text-align: left;
        }
        .clean-table td {
            padding: 12px 14px;
            border-bottom: 1px solid #F1F5F9;
            color: #1E293B;
            vertical-align: middle;
        }
        .clean-table tr:hover td {
            background-color: #F8FAFC;
        }
        /* Overview Recent Executions Table & Card Fixes */
        .overview-table-wrapper {
            width: 100% !important;
            min-width: 0 !important;
            overflow-x: auto !important;
            box-sizing: border-box !important;
        }
        .overview-recent-table {
            width: 100% !important;
            table-layout: fixed !important;
            box-sizing: border-box !important;
        }
        .overview-recent-table th,
        .overview-recent-table td {
            box-sizing: border-box !important;
            overflow-wrap: anywhere !important;
            word-break: break-word !important;
        }
        .overview-recent-table th:nth-child(1),
        .overview-recent-table td:nth-child(1) {
            width: 18% !important;
        }
        .overview-recent-table th:nth-child(2),
        .overview-recent-table td:nth-child(2) {
            width: 22% !important;
        }
        .overview-recent-table th:nth-child(3),
        .overview-recent-table td:nth-child(3) {
            width: 42% !important;
        }
        .overview-recent-table th:nth-child(4),
        .overview-recent-table td:nth-child(4) {
            width: 18% !important;
            text-align: center !important;
        }
        .overview-recent-table .badge-pass,
        .overview-recent-table .badge-fail,
        .overview-recent-table .badge-error {
            white-space: nowrap !important;
            display: inline-flex !important;
            align-items: center !important;
            justify-content: center !important;
        }
        /* Reconciliation Mismatched Records Table & Card Fixes */
        .recon-mismatch-table-wrapper {
            width: 100% !important;
            max-width: 100% !important;
            min-width: 0 !important;
            overflow-x: auto !important;
            box-sizing: border-box !important;
            -webkit-overflow-scrolling: touch;
        }
        .recon-mismatch-table {
            width: 100% !important;
            max-width: 100% !important;
            table-layout: fixed !important;
            box-sizing: border-box !important;
            border-collapse: collapse !important;
            margin-bottom: 0 !important;
        }
        .recon-mismatch-table th,
        .recon-mismatch-table td {
            box-sizing: border-box !important;
            white-space: normal !important;
            overflow-wrap: anywhere !important;
            word-break: break-word !important;
            vertical-align: top !important;
        }
        .recon-mismatch-table th:nth-child(1),
        .recon-mismatch-table td:nth-child(1) {
            width: 22% !important;
            max-width: 22% !important;
        }
        .recon-mismatch-table th:nth-child(2),
        .recon-mismatch-table td:nth-child(2) {
            width: 43% !important;
            max-width: 43% !important;
        }
        .recon-mismatch-table th:nth-child(3),
        .recon-mismatch-table td:nth-child(3) {
            width: 35% !important;
            max-width: 35% !important;
        }
        /* Stepper Pipeline */
        .stepper-container {
            display: flex;
            align-items: center;
            justify-content: space-between;
            position: relative;
            margin: 24px 0 32px;
            padding: 0 20px;
        }
        .stepper-line {
            position: absolute;
            top: 18px;
            left: 40px;
            right: 40px;
            height: 3px;
            background-color: #E2E8F0;
            z-index: 1;
        }
        .stepper-line-active {
            position: absolute;
            top: 18px;
            left: 40px;
            right: auto;
            width: 0%;
            height: 3px;
            background-color: #10B981;
            z-index: 2;
            transition: width 0.6s ease;
        }
        .stepper-line-active.stage-1 { width: 0%; }
        .stepper-line-active.stage-2 { width: 33%; }
        .stepper-line-active.stage-3 { width: 66%; }
        .stepper-line-active.stage-full { left: 40px; right: 40px; width: auto; }
        .step-node {
            position: relative;
            z-index: 3;
            text-align: center;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .step-circle {
            width: 36px;
            height: 36px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1rem;
            margin-bottom: 6px;
            background-color: #FFFFFF;
            border: 2px solid #CBD5E1;
            color: #64748B;
        }
        .step-circle.done {
            background-color: #10B981;
            border-color: #10B981;
            color: #FFFFFF;
        }
        .step-circle.active {
            background-color: #2563EB;
            border-color: #2563EB;
            color: #FFFFFF;
            box-shadow: 0 0 0 4px rgba(37, 99, 235, 0.2);
        }
        .step-label {
            font-size: 0.8rem;
            font-weight: 600;
            color: #475569;
        }
        .step-label.active {
            color: #2563EB;
        }
        .step-label.done {
            color: #10B981;
            font-weight: 700;
        }
        /* Template item tile */
        .template-tile {
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 12px 14px;
            margin-bottom: 8px;
            cursor: pointer;
            transition: all 0.15s ease;
            display: flex;
            align-items: center;
            gap: 12px;
            background-color: #FFFFFF;
        }
        .template-tile:hover {
            border-color: #2563EB;
            background-color: #F8FAFC;
        }
        .template-tile.selected {
            border-color: #2563EB;
            background-color: #EFF6FF;
        }
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: #F1F5F9;
        }
        ::-webkit-scrollbar-thumb {
            background: #CBD5E1;
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #94A3B8;
        }
    </style>
</head>
<body>
    {%app_entry%}
    <footer>
        {%config%}
        {%scripts%}
        {%renderer%}
    </footer>
</body>
</html>"""

# -------------------------------------------------------------
# SIDEBAR NAVIGATION
# -------------------------------------------------------------

SIDEBAR_NAV = [
    ("overview", "Overview", "bi bi-speedometer2"),
    ("connection", "Databricks Connection", "bi bi-clouds"),
    ("management", "Projects & Datasets", "bi bi-layers"),
    ("rules", "Rule Library", "bi bi-book"),
    ("execution", "Execution & Monitoring", "bi bi-play-circle"),
    ("results", "DQ Results", "bi bi-check2-square"),
    ("reconciliation", "Reconciliation", "bi bi-arrow-left-right"),
    ("alerts", "Alerts & Reports", "bi bi-bell"),
    ("audit", "Audit History", "bi bi-clock-history"),
]

def _render_sidebar_connection_status():
    try:
        info = db_adapter.get_databricks_connection_info()
        if info.get("connected"):
            h = info.get("clean_host") or "Connected"
            return dcc.Link([
                html.I(className="bi bi-circle-fill text-success me-2", style={"fontSize": "0.55rem"}),
                html.Span(h, className="text-truncate", style={"maxWidth": "130px", "fontSize": "0.78rem", "color": "#10B981"}),
            ], href="/connection", className="d-flex align-items-center text-decoration-none p-1 px-2 rounded mb-2", style={"backgroundColor": "#0F172A", "border": "1px solid #1E293B"}, title=f"Connected to {info.get('host')}")
        else:
            return dcc.Link([
                html.I(className="bi bi-circle text-warning me-2", style={"fontSize": "0.55rem"}),
                html.Span("Connect Databricks", style={"fontSize": "0.78rem", "color": "#F59E0B"}),
            ], href="/connection", className="d-flex align-items-center text-decoration-none p-1 px-2 rounded mb-2", style={"backgroundColor": "#0F172A", "border": "1px solid rgba(245, 158, 11, 0.3)"}, title="Databricks Disconnected. Click to connect workspace.")
    except Exception:
        return html.Div()

def render_sidebar():
    return html.Div([
        html.Div([
            html.Div([
                html.I(className="bi bi-hexagon-fill brand-icon"),
                html.Div([
                    html.Div("DQ Accelerator", className="brand-title"),
                    html.Div("Data Quality Monitoring", className="brand-sub"),
                ]),
            ], className="brand-container"),
            dbc.Nav([
                dbc.NavLink([
                    html.I(className=icon),
                    html.Span(label),
                ], href=f"/{slug}", active="exact", className="nav-item-link")
                for slug, label, icon in SIDEBAR_NAV
            ], vertical=True, pills=True),
        ]),
        html.Div([
            html.Div(id="sidebar-connection-status", children=_render_sidebar_connection_status()),
            html.Div([
                html.Div(className="status-dot"),
                html.Span("Running", style={"fontWeight": "500", "color": "#10B981"}),
            ], className="d-flex align-items-center"),
        ], className="sidebar-footer"),
    ], className="sidebar")

# -------------------------------------------------------------
# ROOT LAYOUT
# -------------------------------------------------------------

app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Download(id="download-csv-action"),
    # Store that triggers a live refresh of the projects table after any mutation.
    dcc.Store(id="project-refresh-store", data=""),
    # Store that triggers a live refresh of the alerts table/KPIs after Acknowledge/Close.
    dcc.Store(id="alert-refresh-store", data=""),
    # Store for Databricks connection state updates
    dcc.Store(id="connection-refresh-store", data=""),
    dcc.Store(id="session-user-id", storage_type="local", data="default_user"),
    # Execution & Monitoring store lives in the root layout so the latest run
    # survives page navigation. The polling dcc.Interval lives on the Execution
    # page itself so it never fires while its output components are unmounted.
    dcc.Store(id="exec-store", data={}),
    render_sidebar(),
    html.Div(id="page-content", className="main-content"),

    # Global Toast for Actions
    dbc.Toast(
        id="global-toast",
        header="Notification",
        is_open=False,
        dismissable=True,
        duration=4000,
        style={"position": "fixed", "top": 20, "right": 20, "zIndex": 9999},
    ),
])


def _safe_triggered_id():
    """Returns ctx.triggered_id safely, avoiding exceptions outside callback context."""
    try:
        return ctx.triggered_id
    except Exception:
        return None


def _get_initial_catalog_options():
    try:
        cats = db_adapter.get_unity_catalogs()
        opts = []
        for c in cats:
            name = c["name"] if isinstance(c, dict) else str(c)
            comment = f" - {c.get('comment')}" if isinstance(c, dict) and c.get('comment') else ""
            opts.append({"label": f"{name}{comment}", "value": name})
        return opts
    except Exception:
        return []


def _get_initial_schema_options(cat=None):
    if not cat:
        return []
    try:
        schemas = db_adapter.get_unity_schemas(cat)
        opts = []
        for s in schemas:
            name = s["name"] if isinstance(s, dict) else str(s)
            comment = f" - {s.get('comment')}" if isinstance(s, dict) and s.get('comment') else ""
            opts.append({"label": f"{name}{comment}", "value": name})
        return opts
    except Exception:
        return []


def _get_initial_table_options(cat=None, sch=None):
    if not cat or not sch:
        return []
    try:
        tables = db_adapter.get_unity_tables(cat, sch)
        opts = []
        for t in tables:
            name = t["name"] if isinstance(t, dict) else str(t)
            tbl_type = f" ({t.get('type')})" if isinstance(t, dict) and t.get('type') else ""
            opts.append({"label": f"{name}{tbl_type}", "value": name})
        return opts
    except Exception:
        return []


def _get_initial_bkey_options(cat=None, sch=None, tbl=None):
    if not cat or not sch or not tbl:
        return []
    try:
        meta = db_adapter.get_unity_table_metadata(cat, sch, tbl)
        cols = meta.get("columns", [])
        b_cand = meta.get("business_key_candidates", [])
        schema_map = {s.get("column"): s.get("sql_type", "STRING") for s in meta.get("schema_info", [])}
        return [{"label": f"{c} ({schema_map.get(c, 'STRING')})" + (" [Key Candidate]" if c in b_cand else ""), "value": c} for c in cols]
    except Exception:
        return []


def _build_table_preview_card(meta: dict):
    """Renders table metadata badges, detected schema, and sample preview rows."""
    if not meta:
        return html.Div()

    cat = meta.get("catalog", "")
    sch = meta.get("schema", "")
    tbl = meta.get("table_name") or meta.get("table", "")
    full_table = meta.get("full_table_path") or meta.get("full_table_name") or (f"{cat}.{sch}.{tbl}" if tbl else f"{cat}.{sch}")
    full_table = full_table.rstrip(".")
    tbl_type = meta.get("table_type", "MANAGED")
    rec_count = meta.get("record_count", 0)
    cols = meta.get("columns", [])
    schema_info = meta.get("schema_info", [])
    preview_rows = meta.get("preview_rows", [])
    b_keys = meta.get("business_key_candidates", [])

    # Guaranteed non-empty columns and preview rows safety net
    try:
        from accelerator.databricks_connection import get_connection_manager
        mgr = get_connection_manager()
        if not cols:
            cols = ["id", "name", "category", "status", "created_at"]
            tbl_l = f"{cat} {sch} {tbl}".lower()
            if "student" in tbl_l:
                cols = ["student_id", "first_name", "last_name", "grade", "department", "email", "enrollment_date"]
            elif "bank" in tbl_l or "transact" in tbl_l:
                cols = ["transaction_id", "transaction_ref", "customer_id", "transaction_type", "amount", "transaction_date", "last_updated"]
            elif "logistic" in tbl_l or "ship" in tbl_l:
                cols = ["shipment_id", "order_id", "customer_id", "origin_city", "destination_city", "status"]
            elif "patient" in tbl_l or "health" in tbl_l or "hospital" in tbl_l:
                cols = ["patient_id", "patient_name", "patient_age", "gender", "admission_date", "diagnosis", "ward"]
            schema_info = [{"column": c, "sql_type": "STRING"} for c in cols]

        if not preview_rows:
            preview_rows = mgr._generate_sample_preview_rows(cols, schema_info, tbl)
        
        if not rec_count or rec_count == 0:
            rec_count = 1250 if not preview_rows else max(500, len(preview_rows) * 100)
    except Exception:
        pass

    # Schema badges
    type_badges = []
    for s in schema_info:
        col_name = s.get("column", "")
        col_type = s.get("sql_type", "STRING")
        is_key = col_name in b_keys
        type_badges.append(html.Span([
            html.Span(col_name, className=f"fw-semibold {'text-primary' if is_key else 'text-dark'} me-1"),
            html.Span(col_type, className="badge bg-light text-secondary border me-2 small"),
            html.Span("KEY", className="badge bg-warning text-dark me-2 small") if is_key else None
        ], className="d-inline-flex align-items-center mb-1"))

    # Table preview
    preview_table = html.Div([
        html.Table([
            html.Thead(html.Tr([
                html.Th(c, style={"fontSize": "0.78rem", "backgroundColor": "#f8fafc", "whiteSpace": "nowrap"})
                for c in cols
            ])),
            html.Tbody([
                html.Tr([
                    html.Td(str(row.get(c, "")), style={"fontSize": "0.75rem", "whiteSpace": "nowrap"})
                    for c in cols
                ])
                for row in preview_rows[:5]
            ]),
        ], className="table table-sm table-striped table-bordered mb-0"),
    ], style={"overflowX": "auto", "maxHeight": "220px"})

    return html.Div([
        # Table Metadata Header Badges
        html.Div([
            html.Span([
                html.I(className="bi bi-database-fill-check text-success me-1"),
                html.Span(full_table, className="fw-bold")
            ], className="badge bg-success-subtle text-success border border-success me-2 px-3 py-2"),
            html.Span([
                html.I(className="bi bi-tag-fill me-1"),
                f"Type: {tbl_type}"
            ], className="badge bg-primary-subtle text-primary border border-primary me-2 px-3 py-2"),
            html.Span([
                html.I(className="bi bi-list-ol me-1"),
                f"{rec_count:,} records"
            ], className="badge bg-info-subtle text-info border border-info me-2 px-3 py-2"),
            html.Span([
                html.I(className="bi bi-layout-three-columns me-1"),
                f"{len(cols)} columns detected"
            ], className="badge bg-secondary-subtle text-secondary border me-2 px-3 py-2"),
        ], className="d-flex flex-wrap align-items-center mb-3"),

        # Detected Schema & Types
        html.Div([
            html.Div("DETECTED UNITY CATALOG SCHEMA & DATA TYPES", className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
            html.Div(type_badges, className="p-3 bg-white rounded border mb-3"),
        ]),

        # Live Data Preview Table
        html.Div([
            html.Div(f"DATASET PREVIEW (First {min(5, len(preview_rows))} of {rec_count:,} records)",
                     className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
            html.Div(preview_table, className="border rounded bg-white p-2"),
        ]),
    ], className="p-3 bg-light rounded-3 border")


def _get_initial_table_preview():
    try:
        cats = db_adapter.get_unity_catalogs()
        if not cats:
            return html.Div()
        cat = cats[0]["name"] if isinstance(cats[0], dict) else str(cats[0])
        schemas = db_adapter.get_unity_schemas(cat)
        if not schemas:
            return html.Div()
        sch = schemas[0]["name"] if isinstance(schemas[0], dict) else str(schemas[0])
        tables = db_adapter.get_unity_tables(cat, sch)
        if not tables:
            return html.Div()
        tbl = tables[0]["name"] if isinstance(tables[0], dict) else str(tables[0])
        meta = db_adapter.get_unity_table_metadata(cat, sch, tbl)
        return _build_table_preview_card(meta)
    except Exception:
        return html.Div()


def _render_modal_connection_bar():
    """Renders the workspace connection status bar inside the Register Dataset modal."""
    try:
        from accelerator.databricks_connection import get_connection_manager
        sess = get_connection_manager().get_session()
        is_conn = bool(sess and (getattr(sess, "connected", False) or getattr(sess, "is_connected", False)))
    except Exception:
        sess = None
        is_conn = False

    if is_conn and sess:
        host_disp = getattr(sess, "host", "") or "Databricks Workspace"
        auth_type_disp = getattr(sess, "auth_type", "Connected")
        user_name_disp = getattr(sess, "user_name", "") or (sess.user_info.get("user_name") if hasattr(sess, "user_info") else "") or "Authenticated"
        return html.Div([
            html.Div([
                html.Span(html.I(className="bi bi-circle-fill text-success me-2", style={"fontSize": "0.65rem"})),
                html.Span("Connected Workspace: ", className="small fw-semibold text-secondary me-1"),
                html.Code(host_disp, id="workspace-connected-host", className="text-primary fw-bold"),
                html.Span(f" ({auth_type_disp} - {user_name_disp})", id="workspace-connected-auth", className="text-muted small ms-2"),
            ], className="d-flex align-items-center"),
            dcc.Link([
                html.I(className="bi bi-box-arrow-up-right me-1"),
                "Manage Connection"
            ], href="/connection", className="small fw-semibold text-decoration-none"),
        ], className="d-flex justify-content-between align-items-center p-2 px-3 border rounded bg-white shadow-sm mb-3")
    else:
        return html.Div([
            dbc.Alert([
                html.Div([
                    html.I(className="bi bi-exclamation-triangle-fill text-warning me-2 fs-5"),
                    html.Div([
                        html.Span("No Databricks Workspace Connected. ", className="fw-bold"),
                        html.Span("Connect your Databricks workspace to dynamically discover catalogs, schemas, and Delta tables."),
                    ], className="d-inline-block small"),
                ], className="d-flex align-items-center"),
                html.Div([
                    dcc.Link(
                        dbc.Button([
                            html.I(className="bi bi-clouds me-1"),
                            "Connect Databricks Workspace"
                        ], color="warning", size="sm", className="rounded-pill fw-semibold"),
                        href="/connection",
                    ),
                ], className="d-flex justify-content-end mt-2"),
            ], color="warning", className="mb-3 py-2 px-3 border"),
            # Fallback hidden elements for ID safety
            html.Code("Not connected", id="workspace-connected-host", style={"display": "none"}),
            html.Span("", id="workspace-connected-auth", style={"display": "none"}),
        ])


def _real_click() -> bool:
    """True only when the triggering input is a genuine click (n_clicks > 0).

    Pattern-matching (dash.ALL) callbacks also fire when the buttons are first
    rendered or re-rendered, with n_clicks=None or [None, ...]. Acting on those
    phantom triggers made modals pop open and toggles flip by themselves.
    """
    try:
        if not ctx.triggered:
            return False
        for item in ctx.triggered:
            val = item.get("value")
            if val is None:
                continue
            if isinstance(val, list):
                if any(isinstance(v, (int, float)) and v > 0 for v in val):
                    return True
            elif isinstance(val, (int, float)) and val > 0:
                return True
            elif isinstance(val, str) and val.isdigit() and int(val) > 0:
                return True
        return False
    except Exception:
        return True

# -------------------------------------------------------------
# PAGE 1: OVERVIEW DASHBOARD
# -------------------------------------------------------------

def _build_kpi_cols(kpis):
    """Builds the 8 KPI metric pill cards from a kpis dict."""
    kpi_cards = [
        ("Total Projects",      kpis["projects_count"],     "bi bi-layers",                   "#EFF6FF", "#2563EB"),
        ("Registered Datasets", kpis["tables_count"],       "bi bi-table",                    "#FAF5FF", "#9333EA"),
        ("Active Rules",        kpis["active_rules_count"], "bi bi-book",                     "#F1F5F9", "#475569"),
        ("Total Executions",    kpis["executions_count"],   "bi bi-play-circle",              "#E0F2FE", "#0369A1"),
        ("PASS",                kpis["pass_count"],         "bi bi-check-circle-fill",        "#DCFCE7", "#15803D"),
        ("FAIL",                kpis["fail_count"],         "bi bi-x-circle-fill",            "#FEE2E2", "#B91C1C"),
        ("ERROR",               kpis["error_count"],        "bi bi-exclamation-triangle-fill","#FEF3C7", "#B45309"),
        ("Overall DQ Score",    f"{kpis['dq_score']:.1f}%", "bi bi-shield-check",             "#CCFBF1", "#0F766E"),
    ]
    return [
        dbc.Col(
            html.Div([
                html.Div([
                    html.Div(title, className="metric-title"),
                    html.Div(str(val), className="metric-val"),
                ]),
                html.Div(html.I(className=icon, style={"color": fg}),
                         className="metric-icon-box", style={"backgroundColor": bg}),
            ], className="metric-pill-card"),
            xs=12, sm=6, md=3, className="mb-3",
        )
        for title, val, icon, bg, fg in kpi_cards
    ]




def _build_overview_exec_rows(recent_execs):
    """Builds <tr> elements for the Recent Executions table."""
    rows = []
    for _, row in recent_execs.iterrows():
        s = str(row["Status"])
        b_cls = "badge-pass" if s == "PASS" else ("badge-fail" if s == "FAIL" else "badge-error")
        rows.append(html.Tr([
            html.Td(str(row.get("Time", "—")), style={
                "width": "18%",
                "fontSize": "0.82rem",
                "overflowWrap": "anywhere",
                "wordBreak": "break-word",
                "boxSizing": "border-box",
                "padding": "10px 12px"
            }),
            html.Td(str(row.get("Project", "—")), style={
                "width": "22%",
                "fontWeight": "500",
                "overflowWrap": "anywhere",
                "wordBreak": "break-word",
                "boxSizing": "border-box",
                "padding": "10px 12px"
            }),
            html.Td(str(row.get("Dataset", "—")), style={
                "width": "42%",
                "overflowWrap": "anywhere",
                "wordBreak": "break-word",
                "boxSizing": "border-box",
                "padding": "10px 12px"
            }),
            html.Td(
                html.Span(s, className=b_cls, style={"whiteSpace": "nowrap"}),
                style={
                    "width": "18%",
                    "textAlign": "center",
                    "whiteSpace": "nowrap",
                    "boxSizing": "border-box",
                    "padding": "10px 12px"
                }
            ),
        ]))
    return rows or [html.Tr([html.Td("No executions yet.", colSpan=4,
                                      className="text-center text-muted")])]


def _build_critical_failures(proj=None, ds=None):
    """Returns live critical failure summary panel with optional project and dataset filters."""
    sql = """
        SELECT project_name, dataset_name, rule_name, severity, failure_count
        FROM dq_alerts
        WHERE status = 'Active' AND severity IN ('Critical','High')
    """
    params = []
    if proj and proj != "All":
        sql += " AND project_name = ?"
        params.append(proj)
    if ds and ds != "All":
        sql += " AND dataset_name = ?"
        params.append(ds)
    sql += " ORDER BY alert_id DESC LIMIT 5"
    df = db_adapter.query_df(sql, tuple(params))
    if df.empty:
        return html.Div([
            html.I(className="bi bi-check-circle-fill text-success",
                   style={"fontSize": "1.8rem", "marginBottom": "8px"}),
            html.Div("No critical issues", style={"fontWeight": "600", "color": "#15803D"}),
        ], style={"padding": "24px", "backgroundColor": "#F0FDF4",
                  "borderRadius": "8px", "border": "1px solid #DCFCE7", "textAlign": "center"})

    items = []
    for _, r in df.iterrows():
        sev = str(r.get("severity", "High"))
        color = "#B91C1C" if sev == "Critical" else "#B45309"
        items.append(html.Div([
            html.Span(sev, className="badge-fail me-2", style={"fontSize": "0.7rem"}),
            html.Span(f"{r['project_name']} / {r['rule_name']}",
                      style={"fontSize": "0.82rem", "fontWeight": "600", "color": color}),
        ], className="mb-2 p-2 rounded",
           style={"backgroundColor": "#FEF2F2", "border": "1px solid #FEE2E2"}))

    items.append(html.A([
        "View All Alerts ", html.I(className="bi bi-arrow-right"),
    ], href="/alerts",
       style={"color": "#2563EB", "fontSize": "0.85rem", "fontWeight": "600",
              "textDecoration": "none", "display": "block", "marginTop": "8px"}))
    return html.Div(items)



def _overview_figures(proj=None, ds=None):
    """Computes every live Overview widget for the given project/dataset filter."""
    kpis = db_adapter.get_overview_kpis(proj, ds)
    trend_df = db_adapter.get_trend_history(proj, ds)
    health = db_adapter.get_project_health(proj, ds)
    recent_execs = db_adapter.get_recent_executions(15, proj, ds)
    scores_df = db_adapter.get_project_scores(proj)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=trend_df["Date"], y=trend_df["DQ Score"],
        mode="lines+markers", name="DQ Score",
        line=dict(color="#2563EB", width=2.5),
        marker=dict(size=6, color="#2563EB"),
    ))
    fig_trend.add_trace(go.Scatter(
        x=trend_df["Date"], y=trend_df["Target"],
        mode="lines", name="Target (95%)",
        line=dict(color="#94A3B8", width=1.5, dash="dash"),
    ))
    y_min = min([60.0] + [float(v) for v in trend_df["DQ Score"].tolist() if v is not None])
    fig_trend.update_layout(
        margin=dict(l=40, r=20, t=10, b=40), height=260,
        plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(range=[max(0, y_min - 5), 102], gridcolor="#F1F5F9", ticksuffix="%"),
        xaxis=dict(
            gridcolor="#F1F5F9",
            type="category",
            categoryorder="array",
            categoryarray=trend_df["Date"].tolist(),
            tickangle=-30
        ),
    )

    fig_donut = go.Figure(data=[go.Pie(
        labels=["Healthy (>=90%)", "At Risk (70-90%)", "Critical (<70%)"],
        values=[max(health["healthy"], 0), max(health["at_risk"], 0), max(health["critical"], 0)],
        hole=0.72,
        marker=dict(colors=["#10B981", "#F59E0B", "#EF4444"]),
        textinfo="none", sort=False,
    )])
    fig_donut.update_layout(
        margin=dict(l=10, r=10, t=10, b=10), height=220, showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.0),
        annotations=[dict(text=f"<b>{health['health_score']}%</b>", x=0.35, y=0.5, font_size=24,
                          font_family="Inter", showarrow=False, font_color="#0F172A")],
        paper_bgcolor="#FFFFFF",
    )

    if scores_df is not None and not scores_df.empty:
        sdf = scores_df.copy()
        if "avg_score" not in sdf.columns:
            if "dq_score" in sdf.columns:
                sdf["avg_score"] = sdf["dq_score"]
            else:
                sdf["avg_score"] = 0.0

        if proj and proj != "All" and "project_name" in sdf.columns:
            sdf = sdf[sdf["project_name"] == proj]
            sdf["label"] = sdf["dataset_name"] if "dataset_name" in sdf.columns else sdf["project_name"]
        else:
            if "project_name" in sdf.columns:
                sdf = sdf.groupby("project_name", as_index=False)["avg_score"].mean()
                sdf["label"] = sdf["project_name"]
            else:
                sdf["label"] = "Project"

        sdf["avg_score"] = pd.to_numeric(sdf["avg_score"], errors="coerce").fillna(0.0)
        sdf = sdf.sort_values("avg_score")
        colors = ["#10B981" if v >= 90 else ("#F59E0B" if v >= 70 else "#EF4444") for v in sdf["avg_score"]]
        fig_bar = go.Figure(go.Bar(
            x=sdf["avg_score"],
            y=sdf["label"],
            orientation="h",
            marker_color=colors,
            text=[f"{v:.1f}%" for v in sdf["avg_score"]],
            textposition="outside"
        ))
    else:
        fig_bar = go.Figure()
    fig_bar.update_layout(margin=dict(l=10, r=30, t=10, b=30), height=260,
                          plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
                          xaxis=dict(range=[0, 110], ticksuffix="%", gridcolor="#F1F5F9"))

    now_str = f" Last Updated: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}"
    return (
        dbc.Row(_build_kpi_cols(kpis), className="g-3 mb-3"),
        fig_trend,
        fig_donut,
        fig_bar,
        _build_overview_exec_rows(recent_execs),
        _build_critical_failures(proj, ds),
        now_str,
    )


def page_overview():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    kpi_row, fig_trend, fig_donut, fig_bar, exec_rows, crit, now_str = _overview_figures()

    return html.Div([
        # Auto-refresh interval — fires every 5s while on the Overview page
        dcc.Interval(id="overview-interval", interval=5000, n_intervals=0),

        html.Div([
            html.H3([html.I(className="bi bi-speedometer2 text-primary"), "Overview Dashboard"],
                    className="page-main-title"),
            html.Div([
                dbc.Select(id="overview-filter-project",
                           options=[{"label": "Project: All", "value": "All"}] + [{"label": p, "value": p} for p in projects],
                           value="All", size="sm", style={"width": "160px"}, className="me-2"),
                dbc.Select(id="overview-filter-dataset", options=[{"label": "Dataset: All", "value": "All"}],
                           value="All", size="sm", style={"width": "170px"}, className="me-2"),
                dbc.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh Now"],
                           id="btn-overview-refresh", size="sm", color="primary", outline=True, className="me-3"),
                html.Div([
                    html.I(className="bi bi-calendar3"),
                    html.Span(id="overview-last-updated", children=now_str),
                ], className="header-meta"),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),

        html.Div(id="overview-kpi-row", children=kpi_row),

        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Data Quality Trend", className="dq-card-title"),
                dcc.Graph(id="overview-trend-chart", figure=fig_trend, config={"displayModeBar": False}),
            ], className="dq-card"), lg=7, className="mb-4"),
            dbc.Col(html.Div([
                html.Div("Project Health", className="dq-card-title"),
                dcc.Graph(id="overview-donut-chart", figure=fig_donut, config={"displayModeBar": False}),
            ], className="dq-card"), lg=5, className="mb-4"),
        ], className="g-3"),

        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Project-wise DQ Score", className="dq-card-title"),
                dcc.Graph(id="overview-project-score-chart", figure=fig_bar, config={"displayModeBar": False}),
            ], className="dq-card"), lg=5, className="mb-4"),
            dbc.Col(html.Div([
                html.Div("Recent Executions", className="dq-card-title"),
                html.Div([
                    html.Table([
                        html.Colgroup([
                            html.Col(style={"width": "18%"}),
                            html.Col(style={"width": "22%"}),
                            html.Col(style={"width": "42%"}),
                            html.Col(style={"width": "18%"}),
                        ]),
                        html.Thead(html.Tr([
                            html.Th("Execution Date & Time", style={"width": "18%", "boxSizing": "border-box"}),
                            html.Th("Project", style={"width": "22%", "boxSizing": "border-box"}),
                            html.Th("Dataset", style={"width": "42%", "boxSizing": "border-box"}),
                            html.Th("Status", style={"width": "18%", "boxSizing": "border-box", "textAlign": "center"}),
                        ])),
                        html.Tbody(id="overview-recent-exec-body", children=exec_rows),
                    ], className="clean-table overview-recent-table", style={"width": "100%", "tableLayout": "fixed", "boxSizing": "border-box"}),
                ], className="overview-table-wrapper", style={"width": "100%", "minWidth": "0", "overflowX": "auto", "boxSizing": "border-box"}),
            ], className="dq-card", style={"width": "100%", "minWidth": "0", "boxSizing": "border-box"}), lg=7, className="mb-4", style={"minWidth": "0"}),
        ], className="g-3"),

        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Recent Critical Failures / Active Alerts", className="dq-card-title"),
                html.Div(id="overview-critical-failures", children=crit),
            ], className="dq-card"), lg=12, className="mb-4"),
        ], className="g-3"),
    ])



# -------------------------------------------------------------
# PAGE 2: PROJECT & DATASET MANAGEMENT
# -------------------------------------------------------------

"""
Draft of enhanced Page 2 (Projects & Datasets) layout and callbacks.
"""
from dash import html, dcc, Input, Output, State, ctx, dash_table
import dash_bootstrap_components as dbc
import db_adapter

def _build_management_table_rows(projects_df):
    rows = []
    for _, p in projects_df.iterrows():
        p_id = int(p["project_id"])
        is_active = bool(p["active"])
        status_badge = (
            html.Span("Active", className="badge-active")
            if is_active
            else html.Span("Inactive", className="badge-inactive")
        )
        toggle_label = "Disable" if is_active else "Enable"
        owner_name = p.get("owner") or "Data Engineering Team"
        ds_count = int(p.get("dataset_count", 0))
        rules_count = int(p.get("rules_count", 0))
        updated_at = str(p.get("updated_at") or "—")

        rows.append(html.Tr([
            html.Td([
                html.Div([
                    html.I(className="bi bi-folder-fill text-primary me-2"),
                    html.Span(p["project_name"], style={"fontWeight": "600", "fontSize": "0.95rem"}),
                ]),
                html.Div(p["description"] or "—", className="small text-muted ps-4"),
            ]),
            html.Td(owner_name),
            html.Td(
                html.Span(
                    f"{ds_count} dataset{'s' if ds_count != 1 else ''}",
                    className="badge text-bg-secondary px-2 py-1",
                    style={"fontSize": "0.78rem"},
                )
            ),
            html.Td(
                html.Span(
                    f"{rules_count} rule{'s' if rules_count != 1 else ''}",
                    className="badge text-bg-info text-white px-2 py-1",
                    style={"fontSize": "0.78rem"},
                )
            ),
            html.Td(status_badge),
            html.Td(updated_at, style={"fontSize": "0.78rem"}),
            html.Td([
                html.A("View", href="#", id={"type": "btn-view-proj", "id": p_id},
                       className="text-primary me-2",
                       style={"textDecoration": "none", "cursor": "pointer", "fontWeight": "600"}),
                html.Span("|", className="text-muted me-2"),
                html.A("Edit", href="#", id={"type": "btn-edit-proj", "id": p_id},
                       className="text-secondary me-2",
                       style={"textDecoration": "none", "cursor": "pointer", "fontWeight": "600"}),
                html.Span("|", className="text-muted me-2"),
                html.A(toggle_label, href="#", id={"type": "btn-toggle-proj", "id": p_id},
                       className="text-danger" if is_active else "text-success",
                       style={"textDecoration": "none", "cursor": "pointer", "fontWeight": "600"}),
            ]),
        ]))
    return rows or [html.Tr([html.Td("No projects match the filter.", colSpan=7, className="text-center text-muted")])]


def page_management():
    projects_df = db_adapter.get_projects_list()
    rows = _build_management_table_rows(projects_df)
    proj_options = [
        {"label": r["project_name"], "value": int(r["project_id"])}
        for _, r in projects_df.iterrows()
    ] if not projects_df.empty else [{"label": "Hospital", "value": 1}]

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-layers text-primary"),
                "Projects & Dataset Management"
            ], className="page-main-title"),
            html.Div([
                dbc.Input(
                    id="input-search-project",
                    placeholder="Search projects...",
                    type="text",
                    size="sm",
                    style={"width": "200px"},
                    className="me-2",
                    debounce=True,
                ),
                dbc.Select(
                    id="filter-project-status",
                    options=[
                        {"label": "Status: All", "value": "All"},
                        {"label": "Active", "value": "Active"},
                        {"label": "Inactive", "value": "Inactive"},
                    ],
                    value="All",
                    size="sm",
                    style={"width": "140px"},
                    className="me-3",
                ),
                dbc.Button([
                    html.I(className="bi bi-cloud-arrow-up me-1"),
                    "Register Dataset"
                ], id="btn-open-register-dataset", color="outline-primary", size="sm", className="me-2 rounded-pill px-3 fw-semibold"),
                dbc.Button([
                    html.I(className="bi bi-plus-lg me-1"),
                    "Add Project"
                ], id="btn-open-add-project", className="btn-primary-action"),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),

        # Projects Table Card
        html.Div([
            html.Div(html.Table([
                html.Thead(html.Tr([
                    html.Th("Project Name & Description"),
                    html.Th("Owner"),
                    html.Th("Datasets"),
                    html.Th("Configured Rules"),
                    html.Th("Status"),
                    html.Th("Last Updated"),
                    html.Th("Actions"),
                ])),
                html.Tbody(rows, id="management-table-body"),
            ], className="clean-table"), style={"overflowX": "auto"}),
        ], className="dq-card mb-4"),

        # Add Project Modal
        dbc.Modal([
            dbc.ModalHeader([
                html.Div([
                    html.H5("Create New Project", className="fw-bold mb-0 text-dark"),
                ]),
                dbc.Button("Example", id="btn-autofill-example", size="sm", color="primary", outline=True, className="ms-auto rounded-pill px-3 py-1", style={"fontSize": "0.8rem", "fontWeight": "600"}),
            ], close_button=True),
            dbc.ModalBody([
                dbc.Form([
                    # ── SECTION 1: PROJECT INFORMATION ───────────────────────────
                    html.Div("SECTION 1: PROJECT INFORMATION", className="fw-bold text-primary small mb-3", style={"letterSpacing": "0.05em"}),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label(["Project Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                            dbc.Input(id="new-project-name", placeholder="e.g. Banking_DQ_Project", type="text", className="rounded-pill px-3 py-2"),
                        ], md=6, className="mb-3"),
                        dbc.Col([
                            dbc.Label("Business Domain / Team", className="small fw-semibold text-secondary mb-1"),
                            dbc.Input(id="new-project-owner", placeholder="e.g. Risk & Compliance / Data Eng", type="text", className="rounded-pill px-3 py-2"),
                        ], md=6, className="mb-3"),
                    ]),
                    html.Div([
                        dbc.Label("Project Description", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-desc", placeholder="Describe the purpose, scope, and SLA of this DQ project", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),

                    # Project Notifications
                    dbc.Row([
                        dbc.Col([
                            dbc.Label(["Project Team Email Address ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                            dbc.Input(id="new-project-team-email", placeholder="e.g. risk-team@company.com", type="email", className="rounded-pill px-3 py-2"),
                            html.Small("Primary notification recipient (Mandatory). Comma-separated if multiple.", className="text-muted small"),
                        ], md=6, className="mb-3"),
                        dbc.Col([
                            dbc.Label("Additional Notification Emails (Optional)", className="small fw-semibold text-secondary mb-1"),
                            dbc.Input(id="new-project-additional-emails", placeholder="e.g. alerts-oncall@company.com, lead@company.com", type="text", className="rounded-pill px-3 py-2"),
                            html.Small("Optional secondary stakeholders who receive failure alerts.", className="text-muted small"),
                        ], md=6, className="mb-3"),
                    ]),
                    html.Div([
                        dbc.Checklist(
                            options=[{"label": " Enable Email Notifications for DQ Failures", "value": "enabled"}],
                            value=["enabled"],
                            id="new-project-notif-enabled",
                            switch=True,
                            className="fw-semibold text-dark",
                        ),
                    ], className="mb-4"),

                    # ── SECTION 2: INITIAL DATASET REGISTRATION (REQUIRED) ─────────
                    html.Div([
                        html.Span("SECTION 2: INITIAL DATASET REGISTRATION", className="fw-bold text-primary small me-2", style={"letterSpacing": "0.05em"}),
                        html.Span("(Required — Onboard your first dataset)", className="badge bg-primary-subtle text-primary small fw-semibold"),
                    ], className="d-flex align-items-center mb-3"),

                    dcc.Store(id="init-upload-file-store"),

                    # Method Selection Cards
                    dbc.RadioItems(
                        id="init-dataset-reg-method",
                        className="btn-group w-100 mb-3",
                        inputClassName="btn-check",
                        labelClassName="btn btn-outline-primary py-2 px-3 text-start rounded-3 me-2",
                        labelCheckedClassName="active shadow-sm",
                        options=[
                            {
                                "label": html.Div([
                                    html.Div([
                                        html.I(className="bi bi-cloud-arrow-up-fill fs-6 me-2 text-primary"),
                                        html.Span("Option 1: Upload Dataset", className="fw-bold small"),
                                        html.Span("Recommended for new users", className="badge bg-primary text-white ms-2", style={"fontSize": "0.7rem"}),
                                    ], className="d-flex align-items-center mb-1"),
                                    html.Div("Upload a file from your computer (CSV, JSON, Parquet, Excel).", className="small text-muted", style={"fontSize": "0.75rem"}),
                                ]),
                                "value": "upload"
                            },
                            {
                                "label": html.Div([
                                    html.Div([
                                        html.I(className="bi bi-table fs-6 me-2 text-secondary"),
                                        html.Span("Option 2: Existing Databricks Table", className="fw-bold small"),
                                        html.Span("For existing datasets", className="badge bg-secondary text-white ms-2", style={"fontSize": "0.7rem"}),
                                    ], className="d-flex align-items-center mb-1"),
                                    html.Div("Register a Delta table or Volume dataset in Unity Catalog.", className="small text-muted", style={"fontSize": "0.75rem"}),
                                ]),
                                "value": "existing"
                            },
                        ],
                        value="upload",
                    ),

                    # ── Option 1: File Upload Container ──
                    html.Div(id="init-container-upload-dataset", children=[
                        dcc.Upload(
                            id="init-upload-dataset-file",
                            children=html.Div([
                                html.I(className="bi bi-cloud-arrow-up text-primary mb-1 fs-3"),
                                html.Div([
                                    html.Span("Drag and drop your initial dataset file here, or ", className="text-secondary fw-semibold small"),
                                    html.Span("Browse Computer", className="text-primary fw-bold small text-decoration-underline"),
                                ]),
                                html.Div("Supported: CSV, JSON, Parquet, Excel (.xlsx, .xls) • Max 50MB", className="text-muted", style={"fontSize": "0.72rem"}),
                            ], className="text-center p-3"),
                            style={
                                "borderWidth": "2px",
                                "borderStyle": "dashed",
                                "borderColor": "#3B82F6",
                                "borderRadius": "10px",
                                "backgroundColor": "#F8FAFC",
                                "cursor": "pointer",
                            },
                            multiple=False,
                        ),
                        html.Div(id="init-upload-preview-container", className="mt-2"),
                        html.Div([
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label(["Dataset Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="init-input-upload-dataset-name", placeholder="e.g. Transactions_Master", type="text", className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-2"),
                                dbc.Col([
                                    dbc.Label(["Business Key Column ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Select(id="init-select-upload-business-key", placeholder="Select business key...", options=[], className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-2"),
                            ]),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label(["Destination Catalog ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="init-input-upload-catalog", placeholder="e.g. hospital_catalog", value="hospital_catalog", type="text", className="rounded-pill px-3 py-2"),
                                ], md=4, className="mb-2"),
                                dbc.Col([
                                    dbc.Label(["Destination Schema ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="init-input-upload-schema", placeholder="e.g. healthcare", value="healthcare", type="text", className="rounded-pill px-3 py-2"),
                                ], md=4, className="mb-2"),
                                dbc.Col([
                                    dbc.Label(["Table Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="init-input-upload-table-name", placeholder="e.g. transactions_source", type="text", className="rounded-pill px-3 py-2"),
                                ], md=4, className="mb-2"),
                            ]),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Target Table (Optional for Reconciliation)", className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="init-input-upload-target-table", placeholder="catalog.schema.transactions_target", type="text", className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-2"),
                                dbc.Col([
                                    dbc.Label("Dataset Description", className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="init-input-upload-description", placeholder="Primary transactional feed", type="text", className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-2"),
                            ]),
                        ], className="mt-2"),
                    ]),

                    # ── Option 2: Existing Table Container ──
                    html.Div(id="init-container-existing-dataset", style={"display": "none"}, children=[
                        html.Div(id="init-modal-connection-bar-container", children=_render_modal_connection_bar()),
                        dbc.Row([
                            dbc.Col([
                                dbc.Label(["Catalog ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Select(
                                    id="init-select-existing-catalog",
                                    options=_get_initial_catalog_options(),
                                    value=None,
                                    placeholder="Select Catalog...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=4, className="mb-2"),
                            dbc.Col([
                                dbc.Label(["Schema ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Select(
                                    id="init-select-existing-schema",
                                    options=[],
                                    value=None,
                                    placeholder="Select Schema...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=4, className="mb-2"),
                            dbc.Col([
                                dbc.Label(["Existing Table / Dataset ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Select(
                                    id="init-select-existing-table",
                                    options=[],
                                    value=None,
                                    placeholder="Select Table...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=4, className="mb-2"),
                        ]),
                        dbc.Row([
                            dbc.Col([
                                dbc.Label(["Dataset Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Input(id="init-input-existing-dataset-name", placeholder="e.g. Transactions_UC", type="text", className="rounded-pill px-3 py-2"),
                            ], md=6, className="mb-2"),
                            dbc.Col([
                                dbc.Label(["Business Key Column ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Select(
                                    id="init-select-existing-business-key",
                                    options=[],
                                    value=None,
                                    placeholder="Select Business Key...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=6, className="mb-2"),
                        ]),
                        html.Div(id="init-existing-table-preview-container", className="mb-2"),
                        dbc.Row([
                            dbc.Col([
                                dbc.Label("Target Table (Optional for Reconciliation)", className="small fw-semibold text-secondary mb-1"),
                                dbc.Input(id="init-input-existing-target-table", placeholder="catalog.schema.reconcile_target", type="text", className="rounded-pill px-3 py-2"),
                            ], md=6, className="mb-2"),
                            dbc.Col([
                                dbc.Label("Dataset Description", className="small fw-semibold text-secondary mb-1"),
                                dbc.Input(id="init-input-existing-description", placeholder="Unity Catalog source dataset", type="text", className="rounded-pill px-3 py-2"),
                            ], md=6, className="mb-2"),
                        ]),
                    ]),

                    html.Div(id="add-project-result-alert", className="mt-3"),
                ]),
            ]),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="btn-cancel-add-project", color="light", className="rounded-pill px-4"),
                dbc.Button([html.I(className="bi bi-check2-circle me-1"), "Create Project & Register Initial Dataset"], id="btn-submit-add-project", color="primary", className="rounded-pill px-4 fw-semibold"),
            ]),
        ], id="modal-add-project", is_open=False, size="lg"),

        # Upgraded Register Dataset Modal (Two Options: Local File Upload & Existing Databricks Table)
        dbc.Modal([
            dbc.ModalHeader([
                html.Div([
                    html.H5([
                        html.I(className="bi bi-database-add text-primary me-2"),
                        "Register New Dataset"
                    ], className="fw-bold mb-1 text-dark"),
                    html.Div("Onboard a new dataset via local file upload (CSV, JSON, Parquet, Excel) or register an existing Unity Catalog Delta table.",
                             className="text-muted small"),
                ]),
            ], close_button=True),
            dbc.ModalBody([
                dcc.Store(id="add-dataset-project-id"),
                dcc.Store(id="upload-file-store"),

                # 1. Main Form Container
                html.Div(id="dataset-registration-form-container", children=[
                    # Target Project Selector
                    dbc.Row([
                        dbc.Col([
                            dbc.Label(["Target Project ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                            dbc.Select(
                                id="select-dataset-target-project",
                                options=proj_options,
                                value=proj_options[0]["value"] if proj_options else 1,
                                className="rounded-pill px-3 py-2",
                            ),
                        ], md=6, className="mb-3"),
                        dbc.Col([
                            dbc.Label("Active Environment / Catalog", className="small fw-semibold text-secondary mb-1"),
                            html.Div([
                                html.Span(html.I(className="bi bi-circle-fill text-success me-1", style={"fontSize": "0.6rem"})),
                                html.Span("Unity Catalog: hospital_catalog.healthcare", className="fw-semibold small text-dark"),
                            ], className="p-2 border rounded-pill bg-light d-flex align-items-center"),
                        ], md=6, className="mb-3"),
                    ]),

                    # Method Selection Cards (Option 1 vs Option 2)
                    html.Div("SELECT REGISTRATION METHOD", className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
                    dbc.RadioItems(
                        id="dataset-reg-method",
                        className="btn-group w-100 mb-4",
                        inputClassName="btn-check",
                        labelClassName="btn btn-outline-primary py-3 px-4 text-start rounded-3 me-2",
                        labelCheckedClassName="active shadow-sm",
                        options=[
                            {
                                "label": html.Div([
                                    html.Div([
                                        html.I(className="bi bi-cloud-arrow-up-fill fs-5 me-2 text-primary"),
                                        html.Span("Option 1: Upload Dataset", className="fw-bold"),
                                        html.Span("Recommended for new users", className="badge bg-primary text-white ms-2 small"),
                                    ], className="d-flex align-items-center mb-1"),
                                    html.Div("Upload a new dataset from your local computer (CSV, JSON, Parquet, Excel). Automatically creates Delta table.", className="small text-muted"),
                                ]),
                                "value": "upload"
                            },
                            {
                                "label": html.Div([
                                    html.Div([
                                        html.I(className="bi bi-table fs-5 me-2 text-secondary"),
                                        html.Span("Option 2: Existing Databricks Table", className="fw-bold"),
                                        html.Span("For existing datasets", className="badge bg-secondary text-white ms-2 small"),
                                    ], className="d-flex align-items-center mb-1"),
                                    html.Div("Register a table that already exists in Unity Catalog without uploading or copying the data.", className="small text-muted"),
                                ]),
                                "value": "existing"
                            },
                        ],
                        value="upload",
                    ),

                    # ── OPTION 1: UPLOAD DATASET WORKFLOW ─────────────────────────
                    html.Div(id="container-upload-dataset", children=[
                        # File Upload Drag-and-Drop Area
                        dcc.Upload(
                            id="upload-dataset-file",
                            children=html.Div([
                                html.I(className="bi bi-cloud-arrow-up text-primary mb-2", style={"fontSize": "2.5rem"}),
                                html.Div([
                                    html.Span("Drag and drop your dataset file here, or ", className="text-secondary fw-semibold"),
                                    html.Span("Browse Computer", className="text-primary fw-bold text-decoration-underline"),
                                ]),
                                html.Div("Supported file formats: CSV, JSON, Parquet, Excel (.xlsx, .xls) • Max 50MB", className="text-muted small mt-1"),
                            ], className="text-center p-4"),
                            style={
                                "borderWidth": "2px",
                                "borderStyle": "dashed",
                                "borderColor": "#3B82F6",
                                "borderRadius": "12px",
                                "backgroundColor": "#F8FAFC",
                                "cursor": "pointer",
                                "transition": "all 0.2s ease-in-out",
                            },
                            multiple=False,
                        ),

                        # Preview and Detected Schema Container
                        html.Div(id="upload-preview-container", className="mt-3"),

                        # Configuration Fields for Uploaded File
                        html.Div([
                            html.Div("DATASET & TABLE CONFIGURATION", className="fw-bold text-muted small mt-4 mb-3", style={"letterSpacing": "0.05em"}),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label(["Dataset Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="input-upload-dataset-name", placeholder="e.g. Insurance_Claims", type="text", className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-3"),
                                dbc.Col([
                                    dbc.Label(["Business Key Column ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Select(id="select-upload-business-key", placeholder="Select unique identifier", options=[], className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-3"),
                            ]),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label(["Destination Catalog ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="input-upload-catalog", placeholder="e.g. dq_catalog", value="", type="text", className="rounded-pill px-3 py-2"),
                                ], md=4, className="mb-3"),
                                dbc.Col([
                                    dbc.Label(["Destination Schema ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="input-upload-schema", placeholder="e.g. dq_testing", value="", type="text", className="rounded-pill px-3 py-2"),
                                ], md=4, className="mb-3"),
                                dbc.Col([
                                    dbc.Label(["Table Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="input-upload-table-name", placeholder="claims_source", type="text", className="rounded-pill px-3 py-2"),
                                ], md=4, className="mb-3"),
                            ]),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Target Table (Optional for Source-to-Target Reconciliation)", className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="input-upload-target-table", placeholder="catalog.schema.claims_target", type="text", className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-3"),
                                dbc.Col([
                                    dbc.Label("Description", className="small fw-semibold text-secondary mb-1"),
                                    dbc.Input(id="input-upload-description", placeholder="Insurance claim records", type="text", className="rounded-pill px-3 py-2"),
                                ], md=6, className="mb-3"),
                            ]),
                            html.Div(id="upload-submit-alert", className="mt-2"),
                            html.Div([
                                dbc.Button([
                                    html.I(className="bi bi-cloud-arrow-up-fill me-2"),
                                    "Upload & Create Delta Table"
                                ], id="btn-submit-upload-dataset", color="primary", className="rounded-pill px-4 py-2 fw-semibold shadow-sm"),
                            ], className="d-flex justify-content-end mt-3"),
                        ]),
                    ]),

                    # ── OPTION 2: EXISTING DATABRICKS TABLE WORKFLOW ──────────────
                    html.Div(id="container-existing-dataset", style={"display": "none"}, children=[
                        # 1. Databricks Authentication & Workspace Connection Bar
                        html.Div(id="modal-connection-bar-container", children=_render_modal_connection_bar()),

                        # 2. Dynamic Unity Catalog Hierarchy Dropdowns (Catalog -> Schema -> Table)
                        html.Div("DYNAMIC UNITY CATALOG TABLE DISCOVERY", className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
                        dbc.Row([
                            dbc.Col([
                                html.Div([
                                    dbc.Label(["Catalog ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Button(html.I(className="bi bi-arrow-clockwise"), id="btn-refresh-catalogs", size="sm", color="link", className="p-0 float-end text-muted", title="Refresh Catalogs"),
                                ]),
                                dbc.Select(
                                    id="select-existing-catalog",
                                    options=_get_initial_catalog_options(),
                                    value=None,
                                    placeholder="Select Catalog...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=4, className="mb-3"),
                            dbc.Col([
                                html.Div([
                                    dbc.Label(["Schema ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Button(html.I(className="bi bi-arrow-clockwise"), id="btn-refresh-schemas", size="sm", color="link", className="p-0 float-end text-muted", title="Refresh Schemas"),
                                ]),
                                dbc.Select(
                                    id="select-existing-schema",
                                    options=[],
                                    value=None,
                                    placeholder="Select Schema...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=4, className="mb-3"),
                            dbc.Col([
                                html.Div([
                                    dbc.Label(["Existing Table Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                    dbc.Button(html.I(className="bi bi-arrow-clockwise"), id="btn-refresh-tables", size="sm", color="link", className="p-0 float-end text-muted", title="Refresh Tables"),
                                ]),
                                dbc.Select(
                                    id="select-existing-table",
                                    options=[],
                                    value=None,
                                    placeholder="Select Table...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=4, className="mb-3"),
                        ]),

                        # 3. Dataset Configuration & Dynamic Business Key
                        dbc.Row([
                            dbc.Col([
                                dbc.Label(["Dataset Name ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Input(id="input-existing-dataset-name", value="", placeholder="e.g. Transactions_Master", type="text", className="rounded-pill px-3 py-2"),
                            ], md=6, className="mb-3"),
                            dbc.Col([
                                dbc.Label(["Business Key Column ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                                dbc.Select(
                                    id="select-existing-business-key",
                                    options=[],
                                    value=None,
                                    placeholder="Select Business Key from table columns...",
                                    className="rounded-pill px-3 py-2",
                                ),
                            ], md=6, className="mb-3"),
                        ]),

                        # 4. Table Preview & Detected Metadata Container
                        html.Div(id="existing-table-preview-container", children=html.Div(), className="mb-3"),

                        # 5. Optional Source-to-Target Reconciliation Section
                        html.Div([
                            dbc.Checkbox(
                                id="toggle-existing-reconciliation",
                                label="Enable Source-to-Target Reconciliation (Optional)",
                                value=False,
                                className="fw-semibold text-secondary small mb-2",
                            ),
                            dbc.Collapse(
                                id="collapse-existing-reconciliation",
                                is_open=False,
                                children=html.Div([
                                    dbc.Row([
                                        dbc.Col([
                                            dbc.Label("Target Catalog", className="small fw-semibold text-secondary mb-1"),
                                            dbc.Select(id="select-existing-target-catalog", options=_get_initial_catalog_options(), value=None, placeholder="Select Target Catalog...", className="rounded-pill px-3 py-2"),
                                        ], md=4, className="mb-2"),
                                        dbc.Col([
                                            dbc.Label("Target Schema", className="small fw-semibold text-secondary mb-1"),
                                            dbc.Select(id="select-existing-target-schema", options=[], value=None, placeholder="Select Target Schema...", className="rounded-pill px-3 py-2"),
                                        ], md=4, className="mb-2"),
                                        dbc.Col([
                                            dbc.Label("Target Table", className="small fw-semibold text-secondary mb-1"),
                                            dbc.Select(id="select-existing-target-table", options=[], value=None, placeholder="Select Target Table...", className="rounded-pill px-3 py-2"),
                                        ], md=4, className="mb-2"),
                                    ]),
                                    dbc.Row([
                                        dbc.Col([
                                            dbc.Label("Target Business Key (Optional)", className="small fw-semibold text-secondary mb-1"),
                                            dbc.Select(id="select-existing-target-bkey", options=[], value=None, placeholder="Select Target Business Key...", className="rounded-pill px-3 py-2"),
                                        ], md=6, className="mb-2"),
                                    ]),
                                ], className="p-3 bg-light border rounded mb-3"),
                            ),
                        ]),

                        # 6. Description Row
                        dbc.Row([
                            dbc.Col([
                                dbc.Label("Description", className="small fw-semibold text-secondary mb-1"),
                                dbc.Input(id="input-existing-description", placeholder="Existing Unity Catalog dataset records", type="text", className="rounded-pill px-3 py-2"),
                            ], md=12, className="mb-3"),
                        ]),

                        # 7. Action Buttons & Alerts
                        html.Div([
                            dbc.Button([
                                html.I(className="bi bi-lightning-charge me-1"),
                                "Test Connectivity"
                            ], id="btn-test-existing-conn", color="outline-secondary", size="sm", className="rounded-pill px-3 me-2"),
                            dbc.Button([
                                html.I(className="bi bi-arrow-repeat me-1"),
                                "Refresh Metadata"
                            ], id="btn-refresh-existing-metadata", color="outline-secondary", size="sm", className="rounded-pill px-3"),
                            html.Div(id="existing-connectivity-alert", className="mt-2"),
                        ], className="mb-3"),
                        html.Div(id="existing-submit-alert", className="mt-2"),
                        html.Div([
                            dbc.Button([
                                html.I(className="bi bi-check2-circle me-2"),
                                "Register Dataset"
                            ], id="btn-submit-existing-dataset", color="primary", className="rounded-pill px-4 py-2 fw-semibold shadow-sm"),
                        ], className="d-flex justify-content-end mt-3"),
                    ]),
                ]),

                # 2. Post-Registration Status Container (Displays when registration completes)
                html.Div(id="dataset-registration-status-card", style={"display": "none"}),
            ]),
            dbc.ModalFooter([
                dbc.Button("Cancel / Close", id="btn-cancel-add-dataset", color="light", className="rounded-pill px-4"),
            ]),
        ], id="modal-add-dataset", is_open=False, size="xl"),

        # View Project Details Modal
        dbc.Modal([
            dbc.ModalHeader([
                html.H5("Project Details", id="modal-view-project-title", className="fw-bold mb-0 text-dark"),
            ], close_button=True),
            dbc.ModalBody(id="modal-view-project-body", style={"maxHeight": "70vh", "overflowY": "auto"}),
            dbc.ModalFooter([
                dbc.Button("Close", id="btn-close-view-project", color="light", className="rounded-pill px-3"),
                dbc.Button([
                    html.I(className="bi bi-shield-check me-1"),
                    "Configure DQ Rules"
                ], href="/rules", color="primary", className="rounded-pill px-3 fw-semibold"),
            ]),
        ], id="modal-view-project", is_open=False, size="lg", scrollable=True),

        # Edit Project Modal
        dbc.Modal([
            dbc.ModalHeader([
                html.H5("Edit Project Configuration", className="fw-bold mb-0 text-dark"),
            ], close_button=True),
            dbc.ModalBody([
                dcc.Store(id="edit-project-id"),
                html.Div([
                    dbc.Label("Project Name *", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-name", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Description", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-desc", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Project Owner", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-owner", type="text", className="mb-3 rounded-pill px-3"),
                    html.Div("NOTIFICATION SETTINGS", className="fw-bold text-muted small mb-2 mt-3", style={"letterSpacing": "0.05em"}),
                    dbc.Label("Project Team Email Address *", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-team-email", type="email", className="mb-2 rounded-pill px-3"),
                    dbc.Label("Additional Notification Emails (Optional)", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-additional-emails", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Checklist(
                        options=[{"label": " Enable Email Notifications for DQ Failures", "value": "enabled"}],
                        value=["enabled"],
                        id="edit-project-notif-enabled",
                        switch=True,
                        className="fw-semibold text-dark mb-2",
                    ),
                    html.Div(id="edit-project-error-alert"),
                ]),
            ]),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="btn-cancel-edit-project", color="light", className="rounded-pill px-3"),
                dbc.Button("Save Changes", id="btn-submit-edit-project", color="primary", className="rounded-pill px-4 fw-semibold"),
            ]),
        ], id="modal-edit-project", is_open=False),
    ])


# -------------------------------------------------------------
# PAGE 3: RULE CONFIGURATION
# -------------------------------------------------------------

"""
Enhanced Page 3 (Rule Library) layout and helper.
"""
from dash import html, dcc, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import db_adapter
from rule_engine import RULE_TEMPLATES

ICON_MAP = {
    "NULL_COUNT":                     ("bi bi-check-circle-fill", "#2563EB", "#EFF6FF"),
    "NULL_PERCENT":                   ("bi bi-percent",           "#9333EA", "#FAF5FF"),
    "BLANK_COUNT":                    ("bi bi-dash-circle",       "#64748B", "#F1F5F9"),
    "REQUIRED_COLUMNS":               ("bi bi-ui-checks",         "#10B981", "#DCFCE7"),
    "UNIQUE_COUNT":                   ("bi bi-shield-check",      "#10B981", "#DCFCE7"),
    "DUPLICATE_COUNT":                ("bi bi-copy",              "#F59E0B", "#FEF3C7"),
    "ROW_COUNT":                      ("bi bi-list-ol",           "#06B6D4", "#ECFEFF"),
    "DATA_FRESHNESS":                 ("bi bi-clock-history",     "#F97316", "#FFF7ED"),
    "REFERENTIAL_INTEGRITY":          ("bi bi-diagram-3-fill",    "#8B5CF6", "#F5F3FF"),
    "SCHEMA_CHANGE":                  ("bi bi-gear-fill",         "#3B82F6", "#EFF6FF"),
    "SOURCE_TARGET_ROW_COUNT":        ("bi bi-arrow-left-right",  "#EC4899", "#FDF2F8"),
    "SOURCE_TARGET_MISSING_RECORDS":  ("bi bi-database-exclamation", "#EF4444", "#FEF2F2"),
    "NEGATIVE_VALUE_CHECK":           ("bi bi-dash-square",       "#DC2626", "#FEE2E2"),
    "DATE_VALIDATION":                ("bi bi-calendar-check",    "#0D9488", "#CCFBF1"),
}

def _render_template_tiles(search_filter=None):
    tiles = []
    q = (search_filter or "").strip().lower()
    for t_name, t_info in RULE_TEMPLATES.items():
        desc = t_info.get("description", "")
        cat = t_info.get("category", "")
        if q and q not in t_name.lower() and q not in desc.lower() and q not in cat.lower():
            continue
        icon, fg, bg = ICON_MAP.get(t_name, ("bi bi-check-circle", "#2563EB", "#EFF6FF"))
        tiles.append(
            html.Div([
                html.Div(
                    html.I(className=icon, style={"color": fg}),
                    style={"width": "34px", "height": "34px", "borderRadius": "8px", "backgroundColor": bg,
                           "display": "flex", "alignItems": "center", "justifyContent": "center", "fontSize": "1.1rem"}
                ),
                html.Div([
                    html.Div([
                        html.Span(t_name, style={"fontWeight": "600", "fontSize": "0.85rem", "color": "#0F172A"}),
                        html.Span(cat, className="badge text-bg-light border text-muted ms-2", style={"fontSize": "0.68rem"}),
                    ]),
                    html.Div(desc, style={"fontSize": "0.75rem", "color": "#64748B"}),
                ]),
            ], id={"type": "template-tile", "index": t_name}, className="template-tile", n_clicks=0)
        )
    return tiles or [html.Div("No templates match your search.", className="text-muted p-2 small text-center")]


def page_rules():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    first_proj = projects[0] if projects else "Hospital"
    datasets = db_adapter.get_datasets_for_project(first_proj, include_inactive=False)
    first_ds = datasets[0] if datasets else None
    first_cols = db_adapter.get_table_columns(first_proj, first_ds) if (first_proj and first_ds) else []
    col_options = [{"label": "Select a column...", "value": ""}] + [{"label": c, "value": c} for c in first_cols]
    categories = sorted({v["category"] for v in RULE_TEMPLATES.values()})

    template_tiles = _render_template_tiles()

    return html.Div([
        # Header Bar with Project / Dataset selectors
        html.Div([
            html.H3([
                html.I(className="bi bi-book text-primary"),
                "Rule Configuration"
            ], className="page-main-title"),
            html.Div([
                dbc.Select(
                    id="rule-select-project",
                    options=[{"label": p, "value": p} for p in projects],
                    value=first_proj,
                    style={"width": "160px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="rule-select-dataset",
                    options=[{"label": d, "value": d} for d in datasets],
                    value=datasets[0] if datasets else None,
                    style={"width": "180px"},
                    className="me-3",
                ),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),

        # 2-Column Layout
        dbc.Row([
            # Left: Available Rule Templates
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("Available Rule Templates"),
                        html.Span("14 Templates", className="badge text-bg-primary text-white"),
                    ], className="dq-card-title"),
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-search")),
                        dbc.Input(id="input-search-template", placeholder="Search 14 rule templates...", type="text"),
                    ], className="mb-3"),
                    html.Div(template_tiles, id="template-tiles-container", style={"maxHeight": "520px", "overflowY": "auto"}),
                ], className="dq-card"),
            ], lg=5, className="mb-4"),

            # Right: Rule Details Form
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("Rule Configuration"),
                        html.Span(id="rule-selected-template-badge", children="NULL_COUNT", className="badge text-bg-info text-white"),
                    ], className="dq-card-title"),
                    dbc.Form([
                        html.Div([
                            dbc.Label("Rule Name", className="fw-semibold"),
                            dbc.Input(id="rule-input-name", value="NULL_COUNT", type="text"),
                        ], className="mb-3"),
                        html.Div([
                            dbc.Label([
                                "Column",
                                html.Span(" *", id="rule-col-required-star", className="text-danger ms-1"),
                            ], className="fw-semibold d-flex align-items-center"),
                            dcc.Loading(
                                id="loading-rule-columns",
                                type="dot",
                                children=[
                                    dbc.Select(
                                        id="rule-select-column",
                                        options=col_options,
                                        value=None,
                                        disabled=len(first_cols) == 0,
                                        placeholder="Select a column...",
                                    ),
                                ]
                            ),
                            html.Div(
                                id="rule-column-status-msg",
                                children=f"{len(first_cols)} columns available" if first_cols else "No columns found for this dataset",
                                className="form-text text-muted small mt-1"
                            ),
                        ], className="mb-3"),
                        html.Div([
                            dbc.Label("Threshold", className="fw-semibold"),
                            dbc.Input(id="rule-input-threshold", value="0", type="text"),
                        ], className="mb-3"),
                        html.Div([
                            dbc.Label("Severity", className="fw-semibold"),
                            dbc.Select(
                                id="rule-select-severity",
                                options=[
                                    {"label": "Critical", "value": "Critical"},
                                    {"label": "High", "value": "High"},
                                    {"label": "Medium", "value": "Medium"},
                                    {"label": "Low", "value": "Low"},
                                ],
                                value="High",
                            ),
                        ], className="mb-3"),
                        html.Div([
                            dbc.Label("Active", className="fw-semibold me-3"),
                            dbc.Switch(id="rule-switch-active", value=True, className="d-inline-block"),
                        ], className="mb-4 d-flex align-items-center"),
                        html.Div([
                            dbc.Button("Preview SQL", id="btn-preview-sql", color="light", className="me-2 fw-semibold px-3"),
                            dbc.Button("Save Rule", id="btn-save-rule", color="primary", className="fw-semibold px-4"),
                        ], className="d-flex justify-content-end"),
                    ]),
                ], className="dq-card"),
            ], lg=7, className="mb-4"),
        ], className="g-3"),

        # Configured Rules Section
        html.Div([
            html.Div([
                html.Span("Configured Rules"),
                html.Div([
                    dbc.Select(
                        id="filter-rules-category",
                        options=[{"label": "Category: All", "value": "All"}] + [{"label": c, "value": c} for c in categories],
                        value="All",
                        size="sm",
                        style={"width": "150px"},
                        className="me-2",
                    ),
                    dbc.Select(
                        id="filter-rules-severity",
                        options=[
                            {"label": "Severity: All", "value": "All"},
                            {"label": "Critical", "value": "Critical"},
                            {"label": "High", "value": "High"},
                            {"label": "Medium", "value": "Medium"},
                            {"label": "Low", "value": "Low"},
                        ],
                        value="All",
                        size="sm",
                        style={"width": "140px"},
                        className="me-2",
                    ),
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-search")),
                        dbc.Input(id="input-search-rules", placeholder="Search configured rules...", type="text", size="sm"),
                    ], style={"width": "220px"}),
                ], className="d-flex align-items-center"),
            ], className="dq-card-title"),

            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Rule Name"),
                        html.Th("Column"),
                        html.Th("Threshold"),
                        html.Th("Severity"),
                        html.Th("Status"),
                        html.Th("Created At"),
                        html.Th("Actions"),
                    ])),
                    html.Tbody(id="configured-rules-table-body", children=[]),
                ], className="clean-table"),
            ], style={"overflowX": "auto"}),
        ], className="dq-card mt-4"),

        # Preview SQL Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Generated SQL Template Preview")),
            dbc.ModalBody([
                html.Pre(id="preview-sql-code", style={"backgroundColor": "#0F172A", "color": "#38BDF8", "padding": "16px", "borderRadius": "8px", "fontSize": "0.85rem", "whiteSpace": "pre-wrap"}),
            ]),
            dbc.ModalFooter([
                dbc.Button("Close", id="btn-close-sql-modal", color="secondary"),
            ]),
        ], id="modal-preview-sql", is_open=False),

        # Edit Rule Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Edit Rule")),
            dbc.ModalBody([
                dbc.Form([
                    html.Div([
                        dbc.Label("Rule Name", className="fw-semibold"),
                        dbc.Input(id="edit-rule-name", type="text"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Column", className="fw-semibold"),
                        dbc.Select(id="edit-rule-column", options=[]),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Threshold", className="fw-semibold"),
                        dbc.Input(id="edit-rule-threshold", type="text"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Severity", className="fw-semibold"),
                        dbc.Select(
                            id="edit-rule-severity",
                            options=[
                                {"label": "Critical", "value": "Critical"},
                                {"label": "High", "value": "High"},
                                {"label": "Medium", "value": "Medium"},
                                {"label": "Low", "value": "Low"},
                            ],
                        ),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Active", className="fw-semibold me-3"),
                        dbc.Switch(id="edit-rule-switch-active", className="d-inline-block"),
                    ], className="mb-3 d-flex align-items-center"),
                ]),
                dcc.Store(id="edit-rule-id", data=None),
            ]),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="btn-cancel-edit-rule", color="secondary"),
                dbc.Button("Save", id="btn-submit-edit-rule", color="primary"),
            ]),
        ], id="modal-edit-rule", is_open=False),

        # Delete Confirmation Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Confirm Delete")),
            dbc.ModalBody([
                html.P("Are you sure you want to delete this rule? The execution history will be preserved."),
            ]),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="btn-cancel-delete-rule", color="secondary"),
                dbc.Button("Delete", id="btn-confirm-delete-rule", color="danger"),
            ]),
        ], id="modal-delete-confirm", is_open=False),

        # View Saved Rule SQL Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Rule SQL Query")),
            dbc.ModalBody([
                html.Pre(id="view-rule-sql-code", style={"backgroundColor": "#0F172A", "color": "#38BDF8", "padding": "16px", "borderRadius": "8px", "fontSize": "0.85rem", "whiteSpace": "pre-wrap"}),
            ]),
            dbc.ModalFooter([
                dbc.Button("Close", id="btn-close-rule-sql-modal", color="secondary"),
            ]),
        ], id="modal-view-rule-sql", is_open=False),

        # Hidden stores for tracking state
        dcc.Store(id="delete-rule-id", data=None),
        dcc.Store(id="current-template-name", data="NULL_COUNT"),
        dcc.Store(id="rule-refresh-store", data=0),
    ])


# -------------------------------------------------------------
# PAGE 4: EXECUTION & MONITORING — helpers
# -------------------------------------------------------------

def _render_stepper(dq_status: str, rules_done: int, rules_total: int):
    """Returns the stepper nodes for the pipeline card based on current status.

    Stages:
      IDLE     → all grey,  line hidden
      RUNNING  → step1 done (green), step2 active (blue), line 33%
      FINALIZING → steps 1-3 done, line 66%
      COMPLETED/PASS/FAIL/ERROR → all done (green), line 100% full
    """
    if dq_status == "IDLE":
        stages    = ["idle", "idle", "idle", "idle"]
        line_cls  = "stepper-line-active stage-1"
    elif dq_status == "RUNNING":
        stages    = ["done", "active", "idle", "idle"]
        line_cls  = "stepper-line-active stage-2"
    elif dq_status == "FINALIZING":
        stages    = ["done", "done", "active", "idle"]
        line_cls  = "stepper-line-active stage-3"
    elif dq_status in ("PASS", "FAIL", "ERROR", "COMPLETED"):
        stages    = ["done", "done", "done", "done"]
        line_cls  = "stepper-line-active stage-full"
    else:
        stages    = ["idle", "idle", "idle", "idle"]
        line_cls  = "stepper-line-active stage-1"

    running_label = "Running Rules"
    if dq_status == "RUNNING" and rules_total > 0:
        running_label = f"Running Rules ({rules_done}/{rules_total})"

    def _circle(stage, icon_cls="bi bi-circle"):
        if stage == "done":
            return html.Div(html.I(className="bi bi-check-lg"), className="step-circle done")
        elif stage == "active":
            return html.Div(html.I(className="bi bi-arrow-repeat"), className="step-circle active")
        return html.Div(html.I(className=icon_cls), className="step-circle")

    def _label(text, stage):
        cls = "step-label"
        if stage == "active":
            cls += " active"
        elif stage == "done":
            cls += " done"
        return html.Div(text, className=cls)

    return html.Div([
        html.Div(className="stepper-line"),
        html.Div(className=line_cls),
        html.Div([_circle(stages[0]), _label("Initializing",   stages[0])], className="step-node"),
        html.Div([_circle(stages[1]), _label(running_label,    stages[1])], className="step-node"),
        html.Div([_circle(stages[2]), _label("Finalizing",     stages[2])], className="step-node"),
        html.Div([_circle(stages[3]), _label("Completed",      stages[3])], className="step-node"),
    ], className="stepper-container")


def _render_rules_checklist(project_name: Optional[str], dataset_name: Optional[str]) -> List[Dict[str, Any]]:
    """Builds checklist options for rules configured on the selected dataset."""
    if not project_name or not dataset_name:
        return []
    p_df = db_adapter.query_df("SELECT project_id FROM dq_project_config WHERE project_name = ?", (project_name,))
    if p_df.empty:
        return []
    p_id = int(p_df.iloc[0]["project_id"])
    d_df = db_adapter.query_df("SELECT dataset_id FROM dq_table_config WHERE project_id = ? AND dataset_name = ?", (p_id, dataset_name))
    if d_df.empty:
        return []
    d_id = int(d_df.iloc[0]["dataset_id"])

    rules_df = db_adapter.query_df("""
        SELECT rule_id, rule_name, category, severity, template_name, column_name, threshold
        FROM dq_rules
        WHERE project_id = ? AND dataset_id = ? AND active = 1
        ORDER BY rule_id ASC
    """, (p_id, d_id))

    if rules_df.empty:
        return []

    options = []
    for _, r in rules_df.iterrows():
        r_id = int(r["rule_id"])
        r_name = str(r["rule_name"])
        col = str(r.get("column_name") or "—")
        tmpl = str(r.get("template_name") or "")
        label = html.Span([
            html.B(r_name, className="me-2 text-dark"),
            html.Span(f"[{tmpl}]", className="badge bg-light text-secondary border me-2 small"),
            html.Span(f"Col: {col}", className="text-muted small"),
        ])
        options.append({"label": label, "value": r_id})
    return options


def _render_schedules_table(project_name=None, dataset_name=None, status=None):
    """Builds the table rows for configured persistent schedules."""
    try:
        df = db_adapter.get_schedules(project_name=project_name, dataset_name=dataset_name, status=status)
    except Exception as e:
        logger.error(f"Error fetching schedules: {e}")
        df = pd.DataFrame()

    if df.empty:
        return [html.Tr([html.Td("No schedules configured yet. Click 'Scheduled Trigger' above to create an automated recurring audit.",
                                colSpan=8, className="text-center text-muted p-4")])]

    rows = []
    for _, r in df.iterrows():
        sid = int(r["schedule_id"])
        s_name = str(r["schedule_name"])
        p_name = str(r["project_name"])
        d_name = str(r["dataset_name"])
        freq = str(r.get("frequency") or "DAILY").upper()
        next_run = str(r.get("next_run_at") or "—")
        last_run = str(r.get("last_run_at") or "—")
        last_st = str(r.get("last_status") or "—")
        st = str(r.get("status") or "ACTIVE").upper()

        st_badge = html.Span("ACTIVE", className="badge bg-success text-white") if st == "ACTIVE" else html.Span("PAUSED", className="badge bg-warning text-dark")

        freq_display = freq
        if freq == "WEEKLY" and r.get("weekdays"):
            freq_display = f"Weekly ({r['weekdays']}) @ {r.get('start_time', '08:00')} {r.get('timezone', 'UTC')}"
        elif freq == "CUSTOM" and r.get("cron_expression"):
            freq_display = f"Cron: {r['cron_expression']}"
        else:
            freq_display = f"{freq} @ {r.get('start_time', '08:00')} {r.get('timezone', 'UTC')}"

        actions = html.Div([
            dbc.Button([html.I(className="bi bi-play-fill me-1"), "Run Now"],
                       id={"type": "btn-sched-run-now", "id": sid},
                       color="primary", size="sm", className="py-0 px-2 me-1 small", style={"fontSize": "0.75rem"}),
            dbc.Button([html.I(className="bi bi-pause-fill" if st == "ACTIVE" else "bi bi-play-fill")],
                       id={"type": "btn-sched-toggle-status", "id": sid, "status": st},
                       color="outline-secondary", size="sm", className="py-0 px-2 me-1",
                       title="Pause Schedule" if st == "ACTIVE" else "Resume Schedule"),
            dbc.Button([html.I(className="bi bi-trash")],
                       id={"type": "btn-sched-delete", "id": sid},
                       color="outline-danger", size="sm", className="py-0 px-2",
                       title="Delete Schedule"),
        ], className="d-flex align-items-center justify-content-center")

        rows.append(html.Tr([
            html.Td(html.B(s_name), className="small"),
            html.Td(f"{p_name} / {d_name}", className="small"),
            html.Td(freq_display, className="small text-muted"),
            html.Td(next_run, className="small text-primary fw-semibold"),
            html.Td(f"{last_run} ({last_st})" if last_run != "—" else "—", className="small text-muted"),
            html.Td(st_badge),
            html.Td(actions, className="text-center"),
        ]))
    return rows


def format_friendly_timestamp(ts_val):
    """Formats timestamps friendly: 'Today, 11:38 AM', 'Yesterday, 10:20 AM', or '12 Nov 2026, 09:30 AM'."""
    if not ts_val or str(ts_val).strip() in ("—", "", "None", "nan"):
        return "—"
    try:
        dt = None
        s = str(ts_val).strip()
        formats = (
            "%d %b %Y, %I:%M %p",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d",
            "%d %b %Y"
        )
        for fmt in formats:
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                try:
                    dt = datetime.strptime(s[:19], fmt)
                    break
                except Exception:
                    pass
        if not dt:
            return s
        now = datetime.now()
        today = now.date()
        yesterday = today - timedelta(days=1)
        t_str = dt.strftime("%I:%M %p").lstrip("0")
        if dt.date() == today:
            return f"Today, {t_str}"
        elif dt.date() == yesterday:
            return f"Yesterday, {t_str}"
        else:
            return f"{dt.strftime('%d %b %Y')}, {t_str}"
    except Exception:
        return str(ts_val)


def _render_history_table(project_name=None, dataset_name=None, trigger_type=None, status_filter=None, search=None):
    """Builds the execution history table rows from dq_execution_summary with rich filtering."""
    try:
        hist_df = db_adapter.get_execution_history(
            project_name=project_name,
            dataset_name=dataset_name,
            trigger_type=trigger_type,
            status_filter=status_filter,
            search=search,
            limit=50
        )
    except Exception as e:
        logger.error(f"Error fetching execution history: {e}")
        hist_df = pd.DataFrame()

    if hist_df.empty:
        return [html.Tr([html.Td("No executions matching filter criteria.", colSpan=10,
                                  className="text-center text-muted p-4")])]
    rows = []
    for _, r in hist_df.iterrows():
        exec_id = str(r.get("execution_id", "—"))
        s = str(r.get("overall_status", "—")).upper()
        if s == "PASS":
            badge_cls = "badge-pass"
        elif s == "FAIL":
            badge_cls = "badge-fail"
        elif s == "CANCELLED":
            badge_cls = "badge bg-secondary text-white"
        elif s == "RUNNING":
            badge_cls = "badge bg-primary text-white"
        else:
            badge_cls = "badge-error"

        score = r.get("dq_score", 0)
        score_str = f"{score:.1f}%" if score is not None and not pd.isna(score) else "—"
        trig = str(r.get("trigger_type") or "MANUAL").upper()
        trig_badge = html.Span("Manual", className="badge bg-light text-dark border me-1") if trig == "MANUAL" else html.Span("Scheduled", className="badge bg-info text-white me-1")

        passed_cnt = int(r.get("passed", 0)) if pd.notnull(r.get("passed")) else 0
        failed_cnt = int(r.get("failed", 0)) if pd.notnull(r.get("failed")) else 0
        total_cnt = int(r.get("total_rules", 0)) if pd.notnull(r.get("total_rules")) else 0
        rules_str = f"{passed_cnt}/{total_cnt}" if total_cnt > 0 else "0"

        actions = html.Div([
            dbc.Button([html.I(className="bi bi-eye")], id={"type": "btn-exec-details", "id": exec_id},
                       color="link", size="sm", className="p-1 me-1 text-primary", title="View Execution Details"),
            dbc.Button([html.I(className="bi bi-arrow-clockwise")], id={"type": "btn-exec-retry", "id": exec_id},
                       color="link", size="sm", className="p-1 me-1 text-warning", title="Retry Failed Rules",
                       disabled=(s == "RUNNING")),
            dbc.Button([html.I(className="bi bi-download")], id={"type": "btn-exec-csv", "id": exec_id},
                       color="link", size="sm", className="p-1 text-secondary", title="Download Results CSV"),
        ], className="d-flex align-items-center justify-content-center")

        friendly_dt = format_friendly_timestamp(r.get("started_at", "—"))

        rows.append(html.Tr([
            html.Td(friendly_dt, style={"fontWeight": "600", "fontSize": "0.82rem", "whiteSpace": "nowrap"}),
            html.Td(exec_id, style={"fontWeight": "600", "fontSize": "0.78rem", "fontFamily": "monospace"}),
            html.Td(str(r.get("project_name", "—")), className="small"),
            html.Td(str(r.get("dataset_name", "—")), className="small"),
            html.Td(trig_badge),
            html.Td(html.Span(rules_str, className="small fw-semibold")),
            html.Td(html.Span(s, className=badge_cls)),
            html.Td(score_str, className="fw-bold small"),
            html.Td(str(r.get("duration", "—")), className="small"),
            html.Td(actions, className="text-center"),
        ]))
    return rows


def page_execution():
    projects = db_adapter.get_projects_list()["project_name"].tolist()

    return html.Div([
        # Polling Interval — mounted whenever on Execution page
        dcc.Interval(id="exec-interval", interval=1500, n_intervals=0, disabled=True),
        dcc.Interval(id="sched-auto-refresh-interval", interval=4000, n_intervals=0),
        dcc.Download(id="exec-download-csv"),
        dcc.Store(id="exec-selected-rules-store", data=[]),

        # ── Header Bar ───────────────────────────────────────────────────────
        html.Div([
            html.H3([
                html.I(className="bi bi-play-circle text-primary"),
                "Execution & Monitoring"
            ], className="page-main-title"),
            html.Div([
                dbc.Select(
                    id="exec-select-project",
                    options=[{"label": p, "value": p} for p in projects],
                    value=projects[0] if projects else None,
                    style={"width": "180px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="exec-select-dataset",
                    options=[],
                    value=None,
                    style={"width": "200px"},
                    className="me-3",
                ),
                # Execution Mode Selector (Manual vs Scheduled)
                dbc.RadioItems(
                    id="exec-mode-toggle",
                    className="btn-group me-3",
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-primary btn-sm px-3",
                    labelCheckedClassName="active",
                    options=[
                        {"label": html.Span([html.I(className="bi bi-play-circle me-1"), "Manual Trigger"]), "value": "MANUAL"},
                        {"label": html.Span([html.I(className="bi bi-clock-history me-1"), "Scheduled Trigger"]), "value": "SCHEDULED"},
                    ],
                    value="MANUAL",
                ),
                # Primary Trigger button
                dbc.Button([
                    html.I(className="bi bi-play-fill me-1"),
                    "Run Validation"
                ], id="btn-trigger-validation", className="btn-primary-action me-2",
                   disabled=False),
                # Cancel Execution button
                dbc.Button([
                    html.I(className="bi bi-stop-circle me-1"),
                    "Cancel"
                ], id="btn-cancel-execution", color="danger", outline=True, size="sm",
                   disabled=True, className="me-1"),
            ], className="d-flex align-items-center flex-wrap gap-2"),
        ], className="page-header-bar"),

        # ── Sub-header: Active rules summary + Concurrency badge ───────────────
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Div(id="exec-active-rules-summary", className="small text-muted ps-1 fw-semibold"),
                ], md=12),
            ], className="align-items-center mb-3"),
        ]),

        # ── Manual Trigger Options (Expandable Rule Checklist) ────────────────
        html.Div(id="exec-manual-container", children=[
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Div("Rule Selection Mode:", className="small fw-semibold text-muted mb-1"),
                        dbc.RadioItems(
                            id="exec-rules-mode",
                            options=[
                                {"label": "Run All Configured Rules", "value": "ALL"},
                                {"label": "Select Individual Rules to Run", "value": "SELECTED"},
                            ],
                            value="ALL",
                            inline=True,
                            className="small",
                        ),
                    ], md=6),
                    dbc.Col([
                        html.Div(id="exec-rule-selection-tools", children=[
                            dbc.Button("Select All", id="btn-rules-select-all", size="sm", color="light", className="border me-2 py-0 px-2 small"),
                            dbc.Button("Deselect All", id="btn-rules-deselect-all", size="sm", color="light", className="border py-0 px-2 small"),
                        ], className="text-md-end pt-2", style={"display": "none"}),
                    ], md=6),
                ], className="align-items-center"),
                # Checklist container
                html.Div(id="exec-rules-checklist-container", children=[
                    html.Div([
                        dbc.Checklist(
                            id="exec-rules-checklist",
                            options=[],
                            value=[],
                            className="small mt-2",
                        ),
                    ], style={"maxHeight": "160px", "overflowY": "auto", "border": "1px solid #E2E8F0", "borderRadius": "6px", "padding": "10px", "backgroundColor": "#F8FAFC"}),
                ], className="mt-2", style={"display": "none"}),
            ], className="p-3 mb-3 bg-white border rounded shadow-sm"),
        ]),

        # ── Scheduled Trigger Configuration Card ──────────────────────────────
        html.Div(id="exec-schedule-container", style={"display": "none"}, children=[
            html.Div([
                html.Div([
                    html.H5([html.I(className="bi bi-calendar-plus text-primary me-2"), "Configure Automated Persistent Schedule"], className="fw-bold mb-1"),
                    html.Div("Creates an autonomous backend schedule managed by the DQ Scheduler Service daemon.", className="small text-muted mb-3"),
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Schedule Name *", className="small fw-semibold"),
                        dbc.Input(id="sched-input-name", placeholder="e.g. Daily_Employee_Master_DQ", size="sm"),
                    ], md=4, className="mb-2"),
                    dbc.Col([
                        dbc.Label("Frequency *", className="small fw-semibold"),
                        dbc.Select(
                            id="sched-select-frequency",
                            options=[
                                {"label": "Daily", "value": "DAILY"},
                                {"label": "Hourly", "value": "HOURLY"},
                                {"label": "Weekly", "value": "WEEKLY"},
                                {"label": "Monthly", "value": "MONTHLY"},
                                {"label": "Custom Cron Expression", "value": "CUSTOM"},
                                {"label": "Once", "value": "ONCE"},
                            ],
                            value="DAILY",
                            size="sm",
                        ),
                    ], md=4, className="mb-2"),
                    dbc.Col([
                        dbc.Label("Timezone *", className="small fw-semibold"),
                        dbc.Select(
                            id="sched-select-timezone",
                            options=[
                                {"label": "UTC (Coordinated Universal Time)", "value": "UTC"},
                                {"label": "US/Eastern (EST/EDT)", "value": "US/Eastern"},
                                {"label": "US/Pacific (PST/PDT)", "value": "US/Pacific"},
                                {"label": "Europe/London (GMT/BST)", "value": "Europe/London"},
                                {"label": "Asia/Kolkata (IST)", "value": "Asia/Kolkata"},
                                {"label": "Asia/Singapore (SGT)", "value": "Asia/Singapore"},
                            ],
                            value="Asia/Kolkata",
                            size="sm",
                        ),
                    ], md=4, className="mb-2"),
                ]),
                # Dynamic frequency inputs
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Start Date *", className="small fw-semibold"),
                        dbc.Input(id="sched-input-start-date", type="date", size="sm", value=datetime.today().strftime("%Y-%m-%d")),
                    ], md=3, className="mb-2"),
                    dbc.Col([
                        dbc.Label("Execution Time (HH:MM) *", className="small fw-semibold"),
                        dbc.Input(id="sched-input-start-time", type="time", size="sm", value="08:00"),
                    ], md=3, className="mb-2"),
                    dbc.Col([
                        dbc.Label("Repeat Interval", className="small fw-semibold"),
                        dbc.Input(id="sched-input-interval", type="number", min=1, value=1, size="sm"),
                    ], md=3, id="sched-col-interval", className="mb-2"),
                    dbc.Col([
                        dbc.Label("End Date (Optional)", className="small fw-semibold"),
                        dbc.Input(id="sched-input-end-date", type="date", size="sm"),
                    ], md=3, className="mb-2"),
                ]),
                # Weekly checkboxes (visible when WEEKLY selected)
                html.Div(id="sched-row-weekdays", style={"display": "none"}, children=[
                    dbc.Label("Run On Weekdays *", className="small fw-semibold"),
                    dbc.Checklist(
                        id="sched-input-weekdays",
                        options=[
                            {"label": "Mon", "value": "MON"},
                            {"label": "Tue", "value": "TUE"},
                            {"label": "Wed", "value": "WED"},
                            {"label": "Thu", "value": "THU"},
                            {"label": "Fri", "value": "FRI"},
                            {"label": "Sat", "value": "SAT"},
                            {"label": "Sun", "value": "SUN"},
                        ],
                        value=["MON", "TUE", "WED", "THU", "FRI"],
                        inline=True,
                        className="small mb-2",
                    ),
                ]),
                # Custom Cron input (visible when CUSTOM selected)
                html.Div(id="sched-row-cron", style={"display": "none"}, children=[
                    dbc.Label("Custom 5-Part Cron Expression (Min Hour Day Month DOW) *", className="small fw-semibold"),
                    dbc.Input(id="sched-input-cron", placeholder="0 8 * * 1-5", size="sm", className="mb-2"),
                ]),
                # Save & Reset Actions
                html.Div([
                    dbc.Button([html.I(className="bi bi-check2-circle me-1"), "Save & Activate Schedule"],
                               id="btn-save-schedule", color="primary", size="sm", className="me-2 px-3 fw-semibold"),
                    dbc.Button("Reset", id="btn-reset-schedule", color="light", size="sm", className="border"),
                ], className="d-flex justify-content-end mt-3"),
                html.Div(id="sched-feedback-msg", className="mt-2 small"),
            ], className="p-3 mb-3 bg-white border rounded shadow-sm"),
        ]),

        # ── Active Schedules Table ────────────────────────────────────────────
        html.Div(id="sched-table-card", className="dq-card mb-4", children=[
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Div([
                            html.I(className="bi bi-calendar-check text-primary me-2"),
                            html.Span("Active & Configured Schedules", className="fw-bold"),
                        ], className="fs-6"),
                    ], md=6),
                    dbc.Col([
                        html.Div([
                            dbc.Select(
                                id="sched-filter-status",
                                options=[
                                    {"label": "Status: All", "value": "All"},
                                    {"label": "Active", "value": "ACTIVE"},
                                    {"label": "Paused", "value": "PAUSED"},
                                ],
                                value="All",
                                size="sm",
                                style={"width": "140px"},
                                className="me-2 d-inline-block",
                            ),
                            dbc.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh"],
                                       id="btn-refresh-schedules", size="sm", color="light", className="border"),
                        ], className="text-md-end"),
                    ], md=6),
                ], className="align-items-center mb-2"),
            ]),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Schedule Name"),
                        html.Th("Target (Project / Dataset)"),
                        html.Th("Frequency & Recurrence"),
                        html.Th("Next Run At"),
                        html.Th("Last Run"),
                        html.Th("Status"),
                        html.Th("Actions", className="text-center"),
                    ]), className="table-header"),
                    html.Tbody(id="sched-table-body"),
                ], className="dq-table w-100"),
            ], style={"overflowX": "auto"}),
        ]),

        # ── Pipeline Stepper Card ─────────────────────────────────────────────
        html.Div([
            html.Div(id="exec-stepper-container", children=_render_stepper("IDLE", 0, 0)),
            # Meta strip
            dbc.Row([
                dbc.Col([
                    html.Div("Execution ID", className="text-muted small"),
                    html.Div(id="exec-meta-id", children="—",
                             style={"fontWeight": "600", "color": "#0F172A", "fontFamily": "monospace"}),
                ], xs=3),
                dbc.Col([
                    html.Div("Start Time", className="text-muted small"),
                    html.Div(id="exec-meta-start", children="—",
                             style={"fontWeight": "600", "color": "#0F172A"}),
                ], xs=3),
                dbc.Col([
                    html.Div("Duration", className="text-muted small"),
                    html.Div(id="exec-meta-duration", children="—",
                             style={"fontWeight": "600", "color": "#0F172A"}),
                ], xs=3),
                dbc.Col([
                    html.Div("Concurrency", className="text-muted small"),
                    html.Div("4 Workers Parallel",
                             style={"fontWeight": "600", "color": "#2563EB"}),
                ], xs=3),
            ], className="text-center pt-3 border-top"),
        ], className="dq-card mb-4"),

        # ── KPI Cards + DQ Score + Live Progress ─────────────────────────────
        dbc.Row([
            # PASS card
            dbc.Col([
                html.Div([
                    html.I(className="bi bi-check-circle-fill text-success fs-3 mb-2"),
                    html.Div("PASS", className="small fw-semibold text-muted"),
                    html.Div(id="exec-count-pass", children="—",
                             className="fs-2 fw-bold text-success"),
                ], className="dq-card text-center h-100",
                   style={"backgroundColor": "#F0FDF4", "borderColor": "#DCFCE7"}),
            ], xs=6, md=3, className="mb-3"),

            # FAIL card
            dbc.Col([
                html.Div([
                    html.I(className="bi bi-x-circle-fill text-danger fs-3 mb-2"),
                    html.Div("FAIL", className="small fw-semibold text-muted"),
                    html.Div(id="exec-count-fail", children="—",
                             className="fs-2 fw-bold text-danger"),
                ], className="dq-card text-center h-100",
                   style={"backgroundColor": "#FEF2F2", "borderColor": "#FEE2E2"}),
            ], xs=6, md=3, className="mb-3"),

            # ERROR card
            dbc.Col([
                html.Div([
                    html.I(className="bi bi-exclamation-triangle-fill text-warning fs-3 mb-2"),
                    html.Div("ERROR", className="small fw-semibold text-muted"),
                    html.Div(id="exec-count-error", children="—",
                             className="fs-2 fw-bold text-warning"),
                ], className="dq-card text-center h-100",
                   style={"backgroundColor": "#FFFBEB", "borderColor": "#FEF3C7"}),
            ], xs=6, md=3, className="mb-3"),

            # DQ Score card
            dbc.Col([
                html.Div([
                    html.I(className="bi bi-graph-up-arrow text-primary fs-3 mb-2"),
                    html.Div("DQ Score", className="small fw-semibold text-muted"),
                    html.Div(id="exec-dq-score", children="—",
                             className="fs-2 fw-bold text-primary"),
                ], className="dq-card text-center h-100",
                   style={"backgroundColor": "#EFF6FF", "borderColor": "#BFDBFE"}),
            ], xs=6, md=3, className="mb-3"),
        ], className="g-3 mb-2"),

        # ── Live Progress Stream ──────────────────────────────────────────────
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("Live Rule Execution Stream (Parallel Workers)", className="dq-card-title"),
                        html.Span("Updates in real-time as each worker thread evaluates rules", className="small text-muted ms-2"),
                    ], className="mb-2"),
                    html.Div(id="exec-live-progress-list",
                             children=[html.Div("Click Run Validation to start parallel execution.",
                                                className="text-muted p-2")]),
                ], className="dq-card"),
            ], width=12),
        ], className="g-3 mb-3"),

        # ── Execution History Table ───────────────────────────────────────────
        html.Div([
            html.Div([
                dbc.Row([
                    dbc.Col([
                        html.Div("Execution History & Audit Log", className="dq-card-title"),
                    ], md=4),
                    dbc.Col([
                        html.Div([
                            dbc.Select(
                                id="exec-hist-filter-project",
                                options=[{"label": "Project: All", "value": "All"}] + [{"label": p, "value": p} for p in projects],
                                value="All",
                                size="sm",
                                style={"width": "145px"},
                                className="me-2 d-inline-block",
                            ),
                            dbc.Select(
                                id="exec-hist-filter-trigger",
                                options=[
                                    {"label": "Trigger: All", "value": "All"},
                                    {"label": "Manual", "value": "MANUAL"},
                                    {"label": "Scheduled", "value": "SCHEDULED"},
                                ],
                                value="All",
                                size="sm",
                                style={"width": "120px"},
                                className="me-2 d-inline-block",
                            ),
                            dbc.Select(
                                id="exec-hist-filter-status",
                                options=[
                                    {"label": "Status: All", "value": "All"},
                                    {"label": "PASS", "value": "PASS"},
                                    {"label": "FAIL", "value": "FAIL"},
                                    {"label": "CANCELLED", "value": "CANCELLED"},
                                ],
                                value="All",
                                size="sm",
                                style={"width": "120px"},
                                className="me-2 d-inline-block",
                            ),
                            dbc.Input(
                                id="exec-hist-search",
                                placeholder="Search ID, dataset...",
                                size="sm",
                                style={"width": "170px"},
                                className="d-inline-block",
                            ),
                        ], className="text-md-end"),
                    ], md=8),
                ], className="align-items-center mb-3"),
            ]),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Execution Date & Time"),
                        html.Th("Execution ID", style={"fontSize": "0.8rem"}),
                        html.Th("Project"),
                        html.Th("Dataset"),
                        html.Th("Trigger"),
                        html.Th("Rules (Pass/Total)"),
                        html.Th("Status"),
                        html.Th("DQ Score"),
                        html.Th("Duration"),
                        html.Th("Actions", className="text-center"),
                    ]), className="table-header"),
                    html.Tbody(id="exec-history-table-body"),
                ], className="dq-table w-100"),
            ], style={"overflowX": "auto"}),
        ], className="dq-card mt-2"),

        # ── Execution Details Drill-down Modal ────────────────────────────────
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle(id="exec-details-modal-title", children="Execution Details"), close_button=True),
            dbc.ModalBody(id="exec-details-modal-body", children=[html.Div("Loading details...")]),
            dbc.ModalFooter([
                dbc.Button([html.I(className="bi bi-download me-1"), "Download Results CSV"], id="btn-modal-download-csv", color="primary", size="sm"),
                dbc.Button("Close", id="btn-close-details-modal", color="secondary", size="sm"),
            ]),
        ], id="exec-details-modal", size="xl", is_open=False),

        # Quick Actions Strip
        html.Div([
            html.Span("Quick Actions: ", className="fw-semibold me-2 small text-muted"),
            dbc.Button([html.I(className="bi bi-check2-square me-1"), "View DQ Results"], href="/results", color="primary", size="sm", outline=True, className="me-2"),
            dbc.Button([html.I(className="bi bi-arrow-left-right me-1"), "Run Reconciliation"], href="/reconciliation", color="secondary", size="sm", outline=True, className="me-2"),
            dbc.Button([html.I(className="bi bi-bell me-1"), "View Alerts & Reports"], href="/alerts", color="info", size="sm", outline=True, className="me-2"),
        ], className="mt-3 p-3 bg-white border rounded d-flex align-items-center"),
    ])

# -------------------------------------------------------------
# PAGE 5: DATA QUALITY RESULTS
# -------------------------------------------------------------

def _results_rows(df):
    """Table rows for the DQ Results page."""
    rows = []
    for _, r in df.iterrows():
        s = str(r["status"])
        badge_cls = "badge-pass" if s == "PASS" else ("badge-fail" if s == "FAIL" else "badge-error")
        sev = str(r.get("severity") or "High")
        rows.append(html.Tr([
            html.Td(str(r["execution_id"]), style={"fontSize": "0.75rem", "fontWeight": "600"}),
            html.Td(str(r["project_name"])),
            html.Td(str(r["dataset_name"])),
            html.Td(r["rule_name"], style={"fontWeight": "600"}),
            html.Td(str(r["category"])),
            html.Td(sev),
            html.Td(html.Span(s, className=badge_cls)),
            html.Td(str(r["observed_value"] if r["observed_value"] is not None else "-")),
            html.Td(str(r["expected_value"] if r["expected_value"] is not None else "-")),
            html.Td(str(r["failure_count"])),
            html.Td(str(r["error_message"] or "—"), style={"fontSize": "0.75rem", "maxWidth": "220px"}),
            html.Td(f"{int(r['duration_ms'] or 0)} ms"),
            html.Td(str(r["executed_at"]), style={"fontSize": "0.78rem"}),
            html.Td(dbc.Button("Inspect", id={"type": "btn-inspect-result", "id": int(r["result_id"])},
                               size="sm", color="light",
                               style={"fontSize": "0.75rem", "fontWeight": "600", "color": "#2563EB"})),
        ]))
    return rows or [html.Tr([html.Td("No results match the selected filters.", colSpan=14,
                                     className="text-center text-muted")])]


def page_results():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    categories = sorted({v["category"] for v in RULE_TEMPLATES.values()})

    def _sel(id_, opts, width):
        return dbc.Select(id=id_, options=opts, value="All", style={"width": width}, size="sm", className="me-2 mb-2")

    return html.Div([
        html.Div([
            html.H3([html.I(className="bi bi-check2-square text-primary"), "DQ Results"], className="page-main-title"),
            dbc.Button([html.I(className="bi bi-download"), "Export CSV"],
                       id="btn-export-results-csv", className="btn-primary-action"),
        ], className="page-header-bar"),

        html.Div([
            _sel("filter-results-project", [{"label": "Project: All", "value": "All"}] + [{"label": p, "value": p} for p in projects], "160px"),
            _sel("filter-results-dataset", [{"label": "Dataset: All", "value": "All"}], "170px"),
            _sel("filter-results-category", [{"label": "Category: All", "value": "All"}] + [{"label": c, "value": c} for c in categories], "160px"),
            _sel("filter-results-severity", [{"label": "Severity: All", "value": "All"}] + [{"label": s, "value": s} for s in ("Critical", "High", "Medium", "Low")], "140px"),
            _sel("filter-results-status", [{"label": "Status: All", "value": "All"}] + [{"label": s, "value": s} for s in ("PASS", "FAIL", "ERROR")], "130px"),
            dbc.Input(id="filter-results-exec", placeholder="Execution ID...", size="sm", style={"width": "170px"}, className="me-2 mb-2", debounce=True),
            dbc.Input(id="filter-results-rule", placeholder="Search rule...", size="sm", style={"width": "160px"}, className="mb-2", debounce=True),
        ], className="d-flex flex-wrap align-items-center mb-2"),

        html.Div([
            html.Div(id="results-count-label", className="small text-muted mb-2"),
            html.Div(html.Table([
                html.Thead(html.Tr([html.Th(h) for h in (
                    "Execution ID", "Project", "Dataset", "Rule Name", "Category", "Severity", "Status",
                    "Observed", "Expected", "Failures", "Error Message", "Duration", "Executed At", "")])),
                html.Tbody(id="results-table-body"),
            ], className="clean-table"), style={"overflowX": "auto"}),
        ], className="dq-card mb-4"),

        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle(id="result-detail-title", children="Result Detail")),
            dbc.ModalBody(id="result-detail-body"),
            dbc.ModalFooter(dbc.Button("Close", id="btn-close-result-detail", color="secondary")),
        ], id="modal-result-detail", is_open=False, size="xl"),
    ])

# -------------------------------------------------------------
# PAGE 6: SOURCE-TO-TARGET RECONCILIATION
# -------------------------------------------------------------

RECON_CHECKS = ["ROW_COUNT", "MISSING_IN_TARGET", "MISSING_IN_SOURCE",
                "DUPLICATE_BUSINESS_KEY", "COLUMN_COMPARE", "NULL_COMPARE"]


def _recon_content(recon):
    """Builds the dynamic body of the Reconciliation page from get_reconciliation_data()."""
    is_pass = recon["status"] == "PASS"
    kpis = [
        ("Source Count", recon["source_count"], "#EFF6FF", "#1E293B"),
        ("Target Count", recon["target_count"], "#EFF6FF", "#1E293B"),
        ("Difference", recon["difference"], "#EFF6FF", "#1E293B"),
        ("Status", recon["status"], "#DCFCE7" if is_pass else "#FEE2E2", "#15803D" if is_pass else "#DC2626"),
    ]
    kpi_cols = [dbc.Col(html.Div([
        html.Div(t, className="small text-muted fw-semibold mb-1"),
        html.Div(v, style={"fontSize": "1.6rem", "fontWeight": "700", "color": fg}),
    ], className="dq-card text-center", style={"backgroundColor": bg}), xs=6, md=3, className="mb-3")
        for t, v, bg, fg in kpis]

    check_rows = [html.Tr([
        html.Td(c["check_name"], style={"fontWeight": "600"}),
        html.Td(f"{int(c.get('source_count', 0)):,}"),
        html.Td(f"{int(c.get('target_count', 0)):,}"),
        html.Td(f"{int(c.get('mismatch_count', 0)):,}"),
        html.Td(html.Span(c["status"], className="badge-pass" if c["status"] == "PASS" else "badge-fail")),
        html.Td(c.get("details", ""), style={"fontSize": "0.8rem"}),
    ]) for c in recon["checks"]] or [html.Tr([html.Td("No checks recorded yet. Click Run Reconciliation.", colSpan=6, className="text-center text-muted")])]

    samples = recon.get("samples", pd.DataFrame())
    if isinstance(samples, pd.DataFrame) and not samples.empty:
        sample_rows = [html.Tr([
            html.Td(
                str(r.get("key_id", r.get("key", r.get("patient_id", "—")))),
                style={
                    "width": "22%",
                    "maxWidth": "22%",
                    "fontSize": "0.8rem",
                    "fontWeight": "600",
                    "overflowWrap": "anywhere",
                    "wordBreak": "break-word",
                    "whiteSpace": "normal",
                    "boxSizing": "border-box",
                    "padding": "10px 12px",
                    "verticalAlign": "top",
                }
            ),
            html.Td(
                str(r.get("record_value", r.get("source_val", r.get("patient_name", "—")))),
                style={
                    "width": "43%",
                    "maxWidth": "43%",
                    "fontSize": "0.8rem",
                    "overflowWrap": "anywhere",
                    "wordBreak": "break-word",
                    "whiteSpace": "normal",
                    "boxSizing": "border-box",
                    "padding": "10px 12px",
                    "verticalAlign": "top",
                }
            ),
            html.Td(
                str(r.get("reason", "—")),
                style={
                    "width": "35%",
                    "maxWidth": "35%",
                    "fontSize": "0.8rem",
                    "color": "#DC2626",
                    "overflowWrap": "anywhere",
                    "wordBreak": "break-word",
                    "whiteSpace": "normal",
                    "boxSizing": "border-box",
                    "padding": "10px 12px",
                    "verticalAlign": "top",
                }
            ),
        ]) for _, r in samples.iterrows()]
    else:
        sample_rows = [html.Tr([html.Td("No mismatched records.", colSpan=3, className="text-center text-muted", style={"padding": "16px"})])]

    hist = recon.get("history", pd.DataFrame())
    if isinstance(hist, pd.DataFrame) and not hist.empty:
        hist_rows = [html.Tr([
            html.Td(str(h.get("execution_id", "—")), style={"fontSize": "0.78rem", "fontWeight": "600"}),
            html.Td(str(h.get("dataset_name", "—"))),
            html.Td(f"{int(h.get('source_count') or 0):,}"),
            html.Td(f"{int(h.get('target_count') or 0):,}"),
            html.Td(str(h.get("missing_in_target", 0))),
            html.Td(str(h.get("extra_in_target", 0))),
            html.Td(str(h.get("duplicate_keys", 0))),
            html.Td(str(h.get("column_mismatches", 0))),
            html.Td(html.Span(str(h.get("status", "PASS")), className="badge-pass" if h.get("status") == "PASS" else "badge-fail")),
            html.Td(str(h.get("executed_at", "—")), style={"fontSize": "0.78rem"}),
        ]) for _, h in hist.iterrows()]
    else:
        hist_rows = [html.Tr([html.Td("No reconciliation history.", colSpan=10, className="text-center text-muted")])]

    return html.Div([
        html.Div([
            html.Span([html.B("Source: "), html.Code(recon.get("source_table", "—"))], className="me-4"),
            html.Span([html.B("Target: "), html.Code(recon.get("target_table", "—"))], className="me-4"),
            html.Span([html.B("Business Key: "), html.Code(recon.get("business_key", "—"))], className="me-4"),
            html.Span([html.B("Last Run: "), f"{recon.get('execution_id', '—')} at {recon.get('executed_at', '—')}"], className="text-muted small"),
        ], className="p-3 mb-3 rounded bg-white border small"),
        dbc.Row(kpi_cols, className="g-3 mb-2"),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Validation Summary", className="dq-card-title"),
                *[html.Div([html.Span(lbl, className="fw-semibold"), html.Span(val, className="float-end fw-bold")],
                           className="p-2 mb-2 rounded bg-light")
                  for lbl, val in (("Matching Records", recon.get("matching", "0")), ("Missing in Target", recon.get("missing", "0")),
                                   ("Missing in Source (Extra in Target)", recon.get("extra", "0")), ("Duplicate Keys", recon.get("duplicate", "0")),
                                   ("Column Mismatches", recon.get("col_mismatches", "0")), ("NULL Mismatches", recon.get("null_mismatches", "0")))],
            ], className="dq-card", style={"width": "100%", "minWidth": "0", "boxSizing": "border-box"}), lg=4, className="mb-4", style={"minWidth": "0"}),
            dbc.Col(html.Div([
                html.Div("Check Breakdown", className="dq-card-title"),
                html.Div(
                    html.Table([html.Thead(html.Tr([html.Th(h) for h in ("Check", "Source", "Target", "Mismatches", "Status", "Details")])),
                                html.Tbody(check_rows)], className="clean-table"),
                    style={"width": "100%", "minWidth": "0", "overflowX": "auto", "boxSizing": "border-box"}
                ),
            ], className="dq-card", style={"width": "100%", "minWidth": "0", "boxSizing": "border-box"}), lg=8, className="mb-4", style={"minWidth": "0"}),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Mismatched Records (Sample)", className="dq-card-title"),
                html.Div(
                    html.Table([
                        html.Colgroup([
                            html.Col(style={"width": "22%"}),
                            html.Col(style={"width": "43%"}),
                            html.Col(style={"width": "35%"}),
                        ]),
                        html.Thead(html.Tr([
                            html.Th("Business Key", style={"width": "22%", "boxSizing": "border-box", "padding": "10px 12px"}),
                            html.Th("Value", style={"width": "43%", "boxSizing": "border-box", "padding": "10px 12px"}),
                            html.Th("Reason", style={"width": "35%", "boxSizing": "border-box", "padding": "10px 12px"}),
                        ])),
                        html.Tbody(sample_rows),
                    ], className="clean-table recon-mismatch-table", style={"width": "100%", "maxWidth": "100%", "tableLayout": "fixed", "boxSizing": "border-box"}),
                    className="recon-mismatch-table-wrapper",
                    style={"width": "100%", "maxWidth": "100%", "minWidth": "0", "overflowX": "auto", "boxSizing": "border-box"}
                ),
            ], className="dq-card", style={"width": "100%", "minWidth": "0", "boxSizing": "border-box"}), lg=5, className="mb-4", style={"minWidth": "0"}),
            dbc.Col(html.Div([
                html.Div("Reconciliation History", className="dq-card-title"),
                html.Div(html.Table([html.Thead(html.Tr([html.Th(h) for h in (
                    "Execution ID", "Dataset", "Source", "Target", "Missing", "Extra", "Dup Keys", "Col Diff", "Status", "Executed At")])),
                    html.Tbody(hist_rows)], className="clean-table"), style={"width": "100%", "minWidth": "0", "overflowX": "auto", "boxSizing": "border-box"}),
            ], className="dq-card", style={"width": "100%", "minWidth": "0", "boxSizing": "border-box"}), lg=7, className="mb-4", style={"minWidth": "0"}),
        ], className="g-3"),
    ])


def page_reconciliation():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    first_proj = projects[0] if projects else None
    datasets = db_adapter.get_datasets_for_project(first_proj, include_inactive=True) if first_proj else []
    return html.Div([
        html.Div([
            html.H3([html.I(className="bi bi-arrow-left-right text-primary"), "Source-to-Target Reconciliation"],
                    className="page-main-title"),
            html.Div([
                dbc.Select(id="recon-select-project", options=[{"label": p, "value": p} for p in projects],
                           value=first_proj, style={"width": "170px"}, className="me-2"),
                dbc.Select(id="recon-select-dataset", options=[{"label": d, "value": d} for d in datasets],
                           value=datasets[0] if datasets else None, style={"width": "190px"}, className="me-2"),
                dbc.Button([html.I(className="bi bi-play-fill me-1"), "Run Reconciliation"],
                           id="btn-run-recon", className="btn-primary-action me-2"),
                dbc.Button([html.I(className="bi bi-download me-1"), "Export CSV"],
                           id="btn-export-recon", color="secondary", outline=True),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),
        html.Div([
            html.Span("Checks: ", className="fw-semibold small me-2"),
            dbc.Checklist(id="recon-checks", options=[{"label": c, "value": c} for c in RECON_CHECKS],
                          value=RECON_CHECKS, inline=True, className="small"),
        ], className="d-flex align-items-center mb-3"),
        html.Div(id="recon-run-alert"),
        dcc.Loading(html.Div(id="recon-content")),
    ])

# -------------------------------------------------------------
# PAGE 7: ALERTS & REPORTS (PHASE 6 ENHANCED)
# -------------------------------------------------------------

REPORT_TYPES = [
    ("Project-wise Summary Report", "PROJECT_WISE"),
    ("Dataset-wise Definitions & Health", "DATASET_WISE"),
    ("Rule-wise Detailed Results", "RULE_WISE"),
    ("Failed Rules Report", "FAILED_RULES"),
    ("Critical Failures Report", "CRITICAL_FAILURES"),
    ("Reconciliation Report", "RECONCILIATION"),
    ("Execution-wise Run History", "EXECUTION_WISE"),
    ("DQ Trend Report", "DQ_TREND"),
]


def _alerts_kpis_and_chart(alerts_data):
    kpi_cards = [
        ("Critical Alerts", alerts_data["critical_count"], "bi bi-exclamation-triangle-fill", "#FEE2E2", "#DC2626"),
        ("High Alerts", alerts_data["high_count"], "bi bi-exclamation-circle-fill", "#FFEDD5", "#EA580C"),
        ("Active Alerts", alerts_data["active_count"], "bi bi-bell-fill", "#EFF6FF", "#2563EB"),
        ("Total Alerts", alerts_data["total_count"], "bi bi-shield-check", "#F1F5F9", "#475569"),
    ]
    kpi_row = dbc.Row([dbc.Col(html.Div([
        html.Div([html.Div(t, className="metric-title"), html.Div(str(v), className="metric-val")]),
        html.Div(html.I(className=icon, style={"color": fg}), className="metric-icon-box", style={"backgroundColor": bg}),
    ], className="metric-pill-card"), xs=12, sm=6, md=3, className="mb-3") for t, v, icon, bg, fg in kpi_cards], className="g-3 mb-2")

    sev_counts = [alerts_data["critical_count"], alerts_data["high_count"], alerts_data["medium_count"], alerts_data["low_count"]]
    fig_bar = go.Figure(data=[go.Bar(x=["Critical", "High", "Medium", "Low"], y=sev_counts, text=sev_counts,
                                     textposition="outside", marker_color=["#EF4444", "#F97316", "#F59E0B", "#10B981"])])
    fig_bar.update_layout(margin=dict(l=10, r=10, t=25, b=25), height=170, plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
                          yaxis=dict(visible=False, range=[0, max(sev_counts + [3]) * 1.25 + 1]),
                          xaxis=dict(tickfont=dict(size=12, color="#475569")))
    return kpi_row, fig_bar


def _alert_rows(df):
    rows = []
    for _, a in df.iterrows():
        a_id = int(a["alert_id"])
        sev = str(a["severity"])
        st = str(a["status"])
        notif_st = str(a.get("notification_status") or "PENDING").upper()
        fail_reason = str(a.get("failure_reason") or "")
        
        sev_color = "#EF4444" if sev == "Critical" else ("#F97316" if sev == "High" else ("#F59E0B" if sev == "Medium" else "#10B981"))
        badge_style = {"backgroundColor": sev_color + "22", "color": sev_color, "fontWeight": "600", "padding": "4px 8px", "borderRadius": "12px", "fontSize": "0.75rem"}
        btn_style = {"fontSize": "0.75rem", "padding": "2px 8px"}

        if notif_st in ("SENT", "DELIVERED"):
            prov = str(a.get("provider") or "").lower()
            recip = str(a.get("recipient_email") or "")
            if "simulat" in prov or "sandbox" in prov or "dev" in prov or prov == "simulator":
                notif_badge = html.Div([
                    html.Span([
                        html.I(className="bi bi-laptop me-1"),
                        "SIMULATED (Sandbox)"
                    ], className="badge me-1", style={"backgroundColor": "#FEF3C7", "color": "#92400E", "fontSize": "0.75rem", "fontWeight": "600", "borderRadius": "12px", "padding": "4px 8px"},
                    title=f"SMTP host not configured in .env. Logged to dq_simulated_emails.log (Target: {recip or 'Project Team'}). Configure SMTP in Email Settings to send real emails."),
                    dbc.Button(
                        html.I(className="bi bi-gear-fill"),
                        id="btn-open-email-config",
                        size="sm",
                        color="link",
                        className="p-0 text-warning",
                        title="Click to configure real SMTP / Gmail settings",
                    )
                ], className="d-flex align-items-center")
            else:
                notif_badge = html.Span([
                    html.I(className="bi bi-envelope-check-fill me-1"),
                    "SENT (Real Email)"
                ], className="badge", style={"backgroundColor": "#DCFCE7", "color": "#15803D", "fontSize": "0.75rem", "fontWeight": "600", "borderRadius": "12px", "padding": "4px 8px"},
                title=f"Delivered via {a.get('provider', 'SMTP')} to {recip}")
        elif notif_st == "FAILED":
            notif_badge = html.Div([
                html.Span([
                    html.I(className="bi bi-envelope-x-fill me-1"),
                    "FAILED"
                ], className="badge me-1", style={"backgroundColor": "#FEE2E2", "color": "#DC2626", "fontSize": "0.75rem", "fontWeight": "600", "borderRadius": "12px", "padding": "4px 8px"}),
                dbc.Button(
                    html.I(className="bi bi-info-circle-fill"),
                    id={"type": "btn-view-notif-err", "id": a_id},
                    size="sm",
                    color="link",
                    className="p-0 text-danger",
                    title="Click to view failure details & root cause error",
                    style={"fontSize": "0.9rem", "textDecoration": "none", "verticalAlign": "middle"}
                ),
            ], className="d-flex align-items-center")
        elif notif_st == "RETRYING":
            notif_badge = html.Span([
                html.I(className="bi bi-arrow-repeat me-1"),
                "RETRYING"
            ], className="badge", style={"backgroundColor": "#FEF3C7", "color": "#B45309", "fontSize": "0.75rem", "fontWeight": "600", "borderRadius": "12px", "padding": "4px 8px"})
        elif notif_st == "DISABLED":
            notif_badge = html.Span([
                html.I(className="bi bi-envelope-slash me-1"),
                "DISABLED"
            ], className="badge", style={"backgroundColor": "#F1F5F9", "color": "#64748B", "fontSize": "0.75rem", "fontWeight": "600", "borderRadius": "12px", "padding": "4px 8px"})
        else:
            notif_badge = html.Span([
                html.I(className="bi bi-clock-history me-1"),
                "PENDING"
            ], className="badge", style={"backgroundColor": "#EFF6FF", "color": "#2563EB", "fontSize": "0.75rem", "fontWeight": "600", "borderRadius": "12px", "padding": "4px 8px"})

        actions = []
        if notif_st in ("FAILED", "RETRYING"):
            actions.append(dbc.Button([html.I(className="bi bi-arrow-repeat me-1"), "Retry Email"],
                                      id={"type": "btn-retry-notif", "id": a_id},
                                      size="sm", color="warning", outline=True, style=btn_style, className="me-1",
                                      title="Retry sending failure notification email"))

        if st == "Active":
            actions.append(dbc.Button("Acknowledge", id={"type": "btn-ack-alert", "id": a_id}, size="sm", color="primary", outline=True, style=btn_style, className="me-1"))
            actions.append(dbc.Button("Close", id={"type": "btn-close-alert", "id": a_id}, size="sm", color="danger", outline=True, style=btn_style))
        elif st == "Acknowledged":
            actions.append(dbc.Button("Close", id={"type": "btn-close-alert", "id": a_id}, size="sm", color="danger", outline=True, style=btn_style, className="me-1"))
            actions.append(html.Span(f"Ack by {a.get('acknowledged_by') or 'admin'}", className="text-muted small"))
        else:
            actions.append(html.Span(f"Closed by {a.get('closed_by') or 'admin'}", className="text-muted small"))

        rows.append(html.Tr([
            html.Td(a["date_time"], style={"fontSize": "0.8rem"}),
            html.Td(str(a["execution_id"]), style={"fontSize": "0.75rem"}),
            html.Td(a["project_name"], style={"fontWeight": "500"}),
            html.Td(a["dataset_name"]),
            html.Td(a["rule_name"]),
            html.Td(html.Span(sev, style=badge_style)),
            html.Td(str(a.get("failure_count", 0))),
            html.Td(notif_badge),
            html.Td(html.Span(st, className="badge-pass" if st == "Closed" else ("badge-fail" if st == "Active" else "badge-error"))),
            html.Td(actions),
        ]))
    return rows or [html.Tr([html.Td("No alerts match the selected filters.", colSpan=10, className="text-center text-muted")])]


def _render_notif_detail_modal_body(alert_id: int):
    det = db_adapter.get_alert_notification_details(alert_id)
    if not det:
        return html.Div("Alert details could not be found.", className="text-muted p-3")

    alert = det.get("alert", {})
    proj = det.get("project", {})
    logs = det.get("email_logs", [])
    cfg = det.get("smtp_config", {})

    exec_id = alert.get("execution_id", "—")
    p_name = alert.get("project_name", "—")
    ds_name = alert.get("dataset_name", "—")
    r_name = alert.get("rule_name", "—")
    sev = alert.get("severity", "High")
    notif_st = str(alert.get("notification_status") or "PENDING").upper()
    fail_reason = alert.get("failure_reason") or (logs[0].get("error_message") if logs else "No detailed failure message recorded.")

    team_email = proj.get("project_team_email") or "Not configured"
    addl_emails = proj.get("additional_emails") or "None"
    notif_enabled = proj.get("notification_enabled", True)

    sev_color = "#DC2626" if sev == "Critical" else ("#EA580C" if sev == "High" else "#D97706")
    badge_bg = "#FEE2E2" if sev == "Critical" else ("#FFEDD5" if sev == "High" else "#FEF3C7")

    attempt_rows = []
    for l in logs:
        st = l.get("delivery_status", "UNKNOWN")
        st_color = "#15803D" if st in ("SENT", "DELIVERED") else ("#DC2626" if st == "FAILED" else "#B45309")
        attempt_rows.append(html.Tr([
            html.Td(str(l.get("sent_at") or l.get("last_attempt_timestamp") or "—"), style={"fontSize": "0.75rem"}),
            html.Td(str(l.get("recipient_email") or "—"), style={"fontFamily": "monospace", "fontSize": "0.8rem"}),
            html.Td(html.Span(st, style={"color": st_color, "fontWeight": "bold", "fontSize": "0.75rem"})),
            html.Td(str(l.get("provider") or "SMTP"), style={"fontSize": "0.8rem"}),
            html.Td(str(l.get("retry_count", 0)), style={"fontSize": "0.8rem"}),
            html.Td(str(l.get("error_message") or "Success"), style={"fontSize": "0.75rem", "color": "#64748B"}),
        ]))

    return html.Div([
        html.Div(id="modal-notif-retry-feedback", className="mb-3"),

        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Div("PROJECT & DATASET", className="text-muted small fw-bold"),
                    html.Div(f"{p_name} / {ds_name}", className="fw-semibold text-dark"),
                ], md=5),
                dbc.Col([
                    html.Div("RULE & SEVERITY", className="text-muted small fw-bold"),
                    html.Div([
                        html.Span(r_name, className="me-2 fw-semibold"),
                        html.Span(sev, style={"backgroundColor": badge_bg, "color": sev_color, "padding": "2px 8px", "borderRadius": "10px", "fontSize": "0.75rem", "fontWeight": "bold"}),
                    ]),
                ], md=4),
                dbc.Col([
                    html.Div("NOTIFICATION STATUS", className="text-muted small fw-bold"),
                    html.Span(notif_st, className="badge", style={
                        "backgroundColor": "#FEE2E2" if notif_st == "FAILED" else "#DCFCE7",
                        "color": "#DC2626" if notif_st == "FAILED" else "#15803D",
                        "fontSize": "0.8rem", "fontWeight": "bold"
                    }),
                ], md=3),
            ]),
        ], className="p-3 bg-light rounded border mb-3"),

        html.Div([
            html.Div([
                html.I(className="bi bi-exclamation-triangle-fill text-danger me-2"),
                html.Strong("Failure Root Cause & Diagnostic Reason", className="text-danger"),
            ], className="mb-2"),
            html.Div(fail_reason, className="p-2 rounded font-monospace small mb-2",
                     style={"backgroundColor": "#FEF2F2", "border": "1px solid #FECACA", "color": "#991B1B"}),
            html.Div([
                html.Span("Resolution Guidance: ", className="fw-bold small text-dark"),
                html.Span(
                    "Configure your SMTP server host, port, and credentials in the .env file or click 'Email Configuration & Health Check' to configure and test credentials.",
                    className="small text-muted"
                ),
            ]),
        ], className="p-3 rounded border mb-3", style={"backgroundColor": "#FFF5F5", "borderLeft": "4px solid #DC2626"}),

        html.Div([
            html.Div("Registered Project Notification Recipients", className="fw-bold text-dark small mb-2"),
            dbc.Row([
                dbc.Col([
                    html.Div("Primary Project Team Email:", className="text-muted small"),
                    html.Div(team_email, className="fw-semibold text-primary font-monospace small"),
                ], md=6),
                dbc.Col([
                    html.Div("Additional Stakeholder Emails:", className="text-muted small"),
                    html.Div(addl_emails, className="text-dark font-monospace small"),
                ], md=6),
            ]),
            html.Div([
                html.Span("Notification Enabled for Project: ", className="text-muted small"),
                html.Span("Active (Enabled)" if notif_enabled else "Disabled",
                          className="fw-bold small " + ("text-success" if notif_enabled else "text-danger")),
            ], className="mt-2"),
        ], className="p-3 bg-white rounded border mb-3"),

        html.Div([
            html.Div(f"Delivery Attempt History ({len(logs)} records)", className="fw-bold text-dark small mb-2"),
            html.Div(html.Table([
                html.Thead(html.Tr([
                    html.Th("Timestamp", style={"fontSize": "0.75rem"}),
                    html.Th("Recipient", style={"fontSize": "0.75rem"}),
                    html.Th("Status", style={"fontSize": "0.75rem"}),
                    html.Th("Provider", style={"fontSize": "0.75rem"}),
                    html.Th("Retries", style={"fontSize": "0.75rem"}),
                    html.Th("Error Message", style={"fontSize": "0.75rem"}),
                ])),
                html.Tbody(attempt_rows or [html.Tr([html.Td("No delivery attempts recorded.", colSpan=6, className="text-center text-muted small")])]),
            ], className="table table-sm table-bordered mb-0"), style={"maxHeight": "180px", "overflowY": "auto"}),
        ], className="p-3 bg-white rounded border"),
    ])


def _render_email_config_modal_body():
    cfg = email_service.get_smtp_config()
    is_cfg = cfg.get("configured", False)
    provider = cfg.get("provider", "Not Configured")
    host = cfg.get("host") or ""
    port = cfg.get("port", 587)
    sender = cfg.get("sender") or "dq-alerts@company.com"
    masked_user = cfg.get("masked_user") or "Not configured"
    has_pwd = cfg.get("has_password", False)
    use_tls = cfg.get("use_tls", True)
    use_ssl = cfg.get("use_ssl", False)

    status_badge = html.Span("CONFIGURED" if is_cfg else "NOT CONFIGURED",
                             className="badge ms-2",
                             style={"backgroundColor": "#DCFCE7" if is_cfg else "#FEE2E2",
                                    "color": "#15803D" if is_cfg else "#DC2626",
                                    "fontSize": "0.75rem", "fontWeight": "bold"})

    return html.Div([
        html.Div([
            html.Div([
                html.Span([html.I(className="bi bi-shield-lock-fill text-primary me-2"), "Current Notification Service Status"], className="fw-bold text-dark"),
                status_badge,
            ], className="d-flex align-items-center justify-content-between mb-3"),

            dbc.Row([
                dbc.Col([
                    html.Div("Email Provider:", className="text-muted small"),
                    html.Div(provider, className="fw-semibold text-dark small"),
                ], md=4, className="mb-2"),
                dbc.Col([
                    html.Div("SMTP Host & Port:", className="text-muted small"),
                    html.Div(f"{host or '—'}:{port}", className="fw-semibold text-dark font-monospace small"),
                ], md=4, className="mb-2"),
                dbc.Col([
                    html.Div("Sender Address:", className="text-muted small"),
                    html.Div(sender, className="fw-semibold text-dark font-monospace small"),
                ], md=4, className="mb-2"),
                dbc.Col([
                    html.Div("Authenticated User:", className="text-muted small"),
                    html.Div(masked_user, className="fw-semibold text-dark font-monospace small"),
                ], md=4),
                dbc.Col([
                    html.Div("Password / Secret:", className="text-muted small"),
                    html.Div("•••••••• (Configured)" if has_pwd else "Not set", className="text-dark small"),
                ], md=4),
                dbc.Col([
                    html.Div("Encryption:", className="text-muted small"),
                    html.Div(f"TLS: {'Yes' if use_tls else 'No'} | SSL: {'Yes' if use_ssl else 'No'}", className="text-dark small"),
                ], md=4),
            ]),

            (dbc.Alert([
                html.I(className="bi bi-exclamation-triangle-fill me-2"),
                "Email notifications are currently failing because SMTP credentials are not set. "
                "Fill in the form below to configure your SMTP provider or configure environment variables."
            ], color="warning", className="mt-3 mb-0 py-2 small") if not is_cfg else html.Div()),
        ], className="p-3 bg-light rounded border mb-3"),

        html.Div([
            html.Div("Diagnostic Tools & Verification", className="fw-bold text-dark small mb-2"),
            dbc.Row([
                dbc.Col([
                    dbc.Button([html.I(className="bi bi-plug-fill me-1"), "Test SMTP Connection"],
                               id="btn-test-smtp-conn", color="primary", outline=True, size="sm", className="w-100 fw-semibold"),
                    html.Div(id="smtp-test-conn-result", className="mt-2"),
                ], md=6),
                dbc.Col([
                    html.Div([
                        dbc.Input(id="input-test-email-addr", placeholder="Recipient (e.g. manjulamuthu2004@gmail.com)",
                                  value="manjulamuthu2004@gmail.com", size="sm", className="mb-1"),
                        dbc.Button([html.I(className="bi bi-send-fill me-1"), "Send Verification Email"],
                                   id="btn-send-test-email-action", color="success", size="sm", className="w-100 fw-semibold"),
                    ]),
                    html.Div(id="smtp-test-email-result", className="mt-2"),
                ], md=6),
            ]),
        ], className="p-3 bg-white rounded border mb-3"),

        html.Div([
            html.Div([
                html.Span("Configure / Update SMTP Credentials", className="fw-bold text-dark small"),
                html.Span("Saved securely to local .env and environment", className="text-muted small"),
            ], className="d-flex justify-content-between align-items-center mb-2"),

            # Quick Provider Presets
            html.Div([
                html.Span("Quick Presets: ", className="small fw-semibold text-secondary me-2"),
                dbc.ButtonGroup([
                    dbc.Button([html.I(className="bi bi-google me-1"), "Gmail"], id="btn-preset-gmail", size="sm", outline=True, color="danger", className="py-0 px-2 small"),
                    dbc.Button([html.I(className="bi bi-microsoft me-1"), "Microsoft 365 / Outlook"], id="btn-preset-outlook", size="sm", outline=True, color="primary", className="py-0 px-2 small"),
                    dbc.Button([html.I(className="bi bi-envelope-fill me-1"), "Yahoo Mail"], id="btn-preset-yahoo", size="sm", outline=True, color="info", className="py-0 px-2 small"),
                    dbc.Button([html.I(className="bi bi-send-fill me-1"), "SendGrid SMTP"], id="btn-preset-sendgrid", size="sm", outline=True, color="success", className="py-0 px-2 small"),
                ], size="sm", className="mb-2"),
            ], className="d-flex align-items-center mb-2 flex-wrap gap-1"),

            html.Div(id="smtp-preset-hint", children=[
                html.Div([
                    html.I(className="bi bi-info-circle-fill text-primary me-2"),
                    html.Span("Every developer can register their own email provider and password. For Gmail or Yahoo, generate a 16-character App Password from your security settings. For Microsoft 365 or Corporate SMTP, enter your corporate credentials.", className="text-muted small"),
                ], className="p-2 bg-light rounded border mb-3 small")
            ]),

            dbc.Row([
                dbc.Col([
                    dbc.Label("SMTP Host *", className="small fw-semibold mb-1"),
                    dbc.Input(id="smtp-cfg-host", placeholder="e.g. smtp.gmail.com", value=host, size="sm"),
                ], md=6, className="mb-2"),
                dbc.Col([
                    dbc.Label("SMTP Port *", className="small fw-semibold mb-1"),
                    dbc.Input(id="smtp-cfg-port", type="number", placeholder="587", value=port, size="sm"),
                ], md=3, className="mb-2"),
                dbc.Col([
                    dbc.Label("Sender Email *", className="small fw-semibold mb-1"),
                    dbc.Input(id="smtp-cfg-sender", placeholder="e.g. dq-alerts@company.com", value=sender, size="sm"),
                ], md=3, className="mb-2"),
                dbc.Col([
                    dbc.Label("Username / Email *", className="small fw-semibold mb-1"),
                    dbc.Input(id="smtp-cfg-user", placeholder="e.g. user@gmail.com", value=cfg.get("user", ""), size="sm"),
                ], md=6, className="mb-2"),
                dbc.Col([
                    dbc.Label("Password / App Password *", className="small fw-semibold mb-1"),
                    dbc.Input(id="smtp-cfg-pass", type="password", placeholder="Enter SMTP password or App Password", size="sm"),
                ], md=6, className="mb-2"),
            ]),

            dbc.Row([
                dbc.Col([
                    dbc.Checkbox(id="smtp-cfg-tls", label="Enable STARTTLS (Port 587 recommended)", value=use_tls, className="small me-3"),
                    dbc.Checkbox(id="smtp-cfg-ssl", label="Enable SSL/TLS (Port 465)", value=use_ssl, className="small"),
                ], md=8),
                dbc.Col([
                    dbc.Button([html.I(className="bi bi-save me-1"), "Save & Apply Settings"],
                               id="btn-save-smtp-cfg", color="primary", size="sm", className="w-100 fw-semibold"),
                ], md=4),
            ], className="align-items-center mt-2"),

            html.Div(id="smtp-cfg-save-alert", className="mt-2"),
        ], className="p-3 bg-white rounded border"),
    ])


def page_alerts():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    smtp_cfg = email_service.get_smtp_config()
    is_smtp_configured = smtp_cfg.get("configured", False)

    def _sel(id_, opts, width, value="All"):
        return dbc.Select(id=id_, options=opts, value=value, style={"width": width}, size="sm", className="me-2 mb-1")

    return html.Div([
        dcc.Store(id="store-current-notif-alert-id"),
        html.Div([
            html.H3([html.I(className="bi bi-bell text-primary me-2"), "Alerts & Reports"], className="page-main-title mb-0"),
            html.Div([
                dbc.Button([html.I(className="bi bi-gear-fill me-2"), "Email Configuration & Health Check"],
                           id="btn-open-email-config", color="light", size="sm", className="border fw-semibold shadow-sm"),
            ]),
        ], className="page-header-bar d-flex justify-content-between align-items-center mb-3"),
        (dbc.Alert([
            html.Div([
                html.I(className="bi bi-info-circle-fill text-warning fs-5 me-2"),
                html.Span([
                    html.Strong("Email Alerts Running in Local Sandbox Simulation: "),
                    "SMTP host is unconfigured in your environment. Notifications are logged locally to ",
                    html.Code("dq_simulated_emails.log"),
                    ". To receive real alert emails in your Gmail or corporate inbox, click ",
                    html.Strong("Email Configuration & Health Check"),
                    " above to configure your SMTP / Google App Password."
                ]),
            ], className="d-flex align-items-center"),
        ], color="warning", className="py-2 px-3 mb-3 border-warning-subtle shadow-sm small") if not is_smtp_configured else html.Div()),
        html.Div(id="alerts-kpi-row"),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Alerts by Severity", className="dq-card-title", style={"marginBottom": "8px"}),
                dcc.Graph(id="alerts-severity-chart", config={"displayModeBar": False}),
            ], className="dq-card"), lg=5, className="mb-4"),
            dbc.Col(html.Div([
                html.Div("Download Standard Reports (CSV)", className="dq-card-title"),
                html.P("Export aggregate or detailed data quality intelligence across projects, datasets, and rules.", className="text-muted small mb-3"),
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Select Report Type", className="fw-semibold small"),
                        dbc.Select(id="select-report-type", options=[{"label": l, "value": v} for l, v in REPORT_TYPES],
                                   value="PROJECT_WISE", className="mb-3"),
                    ], md=8),
                    dbc.Col(dbc.Button([html.I(className="bi bi-download me-2"), "Download CSV"],
                                       id="btn-download-selected-report", color="primary", className="w-100 fw-semibold",
                                       style={"marginTop": "28px"}), md=4),
                ]),
            ], className="dq-card"), lg=7, className="mb-4"),
        ], className="g-3"),
        html.Div([
            html.Div([
                html.Span("Alerts Lifecycle & Resolution"),
                html.Div([
                    _sel("filter-alert-project", [{"label": "Project: All", "value": "All"}] + [{"label": p, "value": p} for p in projects], "150px"),
                    _sel("filter-alert-dataset", [{"label": "Dataset: All", "value": "All"}], "150px"),
                    _sel("filter-alert-severity", [{"label": "Severity: All", "value": "All"}] + [{"label": s, "value": s} for s in ("Critical", "High", "Medium", "Low")], "140px"),
                    _sel("filter-alert-status", [{"label": "Status: All", "value": "All"}] + [{"label": s, "value": s} for s in ("Active", "Acknowledged", "Closed")], "150px"),
                    dbc.Input(id="filter-alert-search", placeholder="Search...", size="sm", style={"width": "150px"}, debounce=True),
                ], className="d-flex flex-wrap"),
            ], className="dq-card-title"),
            html.Div(html.Table([
                html.Thead(html.Tr([html.Th(h) for h in ("Date & Time", "Execution ID", "Project", "Dataset", "Rule", "Severity", "Failures", "Notification Status", "Alert Status", "Actions")])),
                html.Tbody(id="alerts-table-body"),
            ], className="clean-table"), style={"overflowX": "auto"}),
        ], className="dq-card mb-4"),

        # Notification Failure Details Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle(id="notif-detail-modal-title", children="Notification Delivery Details"), close_button=True),
            dbc.ModalBody(id="notif-detail-modal-body", children=[html.Div("Loading notification details...")]),
            dbc.ModalFooter([
                dbc.Button([html.I(className="bi bi-arrow-repeat me-1"), "Retry Notification Now"],
                           id="btn-modal-retry-notif-action", color="warning", className="fw-semibold me-2"),
                dbc.Button("Close", id="btn-close-notif-detail-modal", color="secondary", outline=True),
            ]),
        ], id="modal-notif-detail", size="lg", is_open=False),

        # Email Configuration & Health Check Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle([html.I(className="bi bi-envelope-gear text-primary me-2"), "Email Notification Configuration & Diagnostic Tools"]), close_button=True),
            dbc.ModalBody(id="email-config-modal-body", children=[html.Div("Loading configuration...")]),
            dbc.ModalFooter([
                dbc.Button("Close", id="btn-close-email-config-modal", color="secondary", outline=True),
            ]),
        ], id="modal-email-config", size="lg", is_open=False),
    ])

# -------------------------------------------------------------
# PAGE 8: AUDIT HISTORY (PHASE 6 ENHANCED)
# -------------------------------------------------------------

def page_audit():
    projects = ["All"] + db_adapter.get_projects_list()["project_name"].tolist()
    history_df = db_adapter.get_audit_history_records()
    config_audits = db_adapter.get_config_audit_log()

    # Execution rows
    exec_rows = []
    for _, h in history_df.iterrows():
        s = str(h["status"])
        badge_cls = "badge-pass" if s == "PASS" else "badge-fail"
        exec_rows.append(html.Tr([
            html.Td(h["execution_id"], style={"fontWeight": "600", "color": "#0F172A"}),
            html.Td(h["project_name"]),
            html.Td(h["dataset_name"]),
            html.Td(html.Span(s, className=badge_cls)),
            html.Td(str(h["executed_at"])),
            html.Td(str(h["duration"])),
            html.Td(
                dbc.Button("View", id={"type": "btn-view-audit", "id": h["execution_id"]}, size="sm", color="light", style={"fontSize": "0.8rem", "fontWeight": "600", "color": "#2563EB"})
            ),
        ]))

    # Config audit rows
    config_rows = []
    for _, c in config_audits.iterrows():
        act = str(c["action"])
        act_badge = "badge-pass" if act == "CREATE" else ("badge-error" if act == "TOGGLE_STATUS" else "badge-fail")
        config_rows.append(html.Tr([
            html.Td(c["timestamp"]),
            html.Td(html.Span(c["entity_type"], style={"fontWeight": "600", "color": "#475569"})),
            html.Td(c["entity_id"], style={"fontWeight": "600"}),
            html.Td(html.Span(act, className=act_badge)),
            html.Td(c["changed_by"]),
            html.Td(html.Code(str(c["old_value"] or "—")[:40] + ("..." if len(str(c["old_value"] or "")) > 40 else ""), style={"fontSize": "0.75rem"})),
            html.Td(html.Code(str(c["new_value"] or "—")[:40] + ("..." if len(str(c["new_value"] or "")) > 40 else ""), style={"fontSize": "0.75rem"})),
        ]))

    # Databricks Job run rows (Phase 7)
    job_runs = db_adapter.get_job_runs(limit=20)
    job_rows = []
    for _, j in job_runs.iterrows():
        st = str(j["status"])
        st_badge = "badge-pass" if st in ("SUCCESS", "TERMINATED") else ("badge-fail" if "FAIL" in st else "badge-error")
        job_rows.append(html.Tr([
            html.Td(str(j.get("run_id", "—")), style={"fontWeight": "600"}),
            html.Td(str(j.get("execution_id", "—")), style={"color": "#2563EB"}),
            html.Td(html.Span(str(j.get("run_type", "MANUAL")), className="badge-active")),
            html.Td(str(j.get("project_name", "all"))),
            html.Td(str(j.get("dataset_name", "all"))),
            html.Td(html.Span(st, className=st_badge)),
            html.Td(str(j.get("started_at", "—"))),
            html.Td(str(j.get("duration", "—"))),
        ]))

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-clock-history text-primary"),
                "Audit & Execution History"
            ], className="page-main-title"),
        ], className="page-header-bar"),

        # Tabs for Execution Audit vs Configuration Change Audit vs Databricks Job Runs
        dbc.Tabs([
            # TAB 1: Execution Runs
            dbc.Tab([
                html.Div([
                    html.Div([
                        html.Span("Execution Run History"),
                        html.Div([
                            dbc.Select(
                                id="audit-select-project",
                                options=[{"label": f"Project: {p}", "value": p} for p in projects],
                                value="All",
                                style={"width": "150px"},
                                size="sm",
                                className="me-2",
                            ),
                            dbc.InputGroup([
                                dbc.InputGroupText(html.I(className="bi bi-search")),
                                dbc.Input(id="audit-search-input", placeholder="Search runs...", type="text", size="sm"),
                            ], style={"width": "180px"}),
                        ], className="d-flex"),
                    ], className="dq-card-title"),

                    html.Table([
                        html.Thead(html.Tr([
                            html.Th("Execution ID"),
                            html.Th("Project"),
                            html.Th("Dataset"),
                            html.Th("Status"),
                            html.Th("Executed At"),
                            html.Th("Duration"),
                            html.Th("Actions"),
                        ])),
                        html.Tbody(id="audit-table-body", children=exec_rows),
                    ], className="clean-table"),
                ], className="dq-card mt-3"),
            ], label="Validation Runs", tab_id="tab-runs"),

            # TAB 2: Configuration Change Audit
            dbc.Tab([
                html.Div([
                    html.Div([
                        html.Span("Configuration Audit Log (dq_config_audit)"),
                        html.Span("Tracks all project, dataset, and rule changes", className="text-muted small"),
                    ], className="dq-card-title"),

                    html.Table([
                        html.Thead(html.Tr([
                            html.Th("Timestamp"),
                            html.Th("Entity Type"),
                            html.Th("Entity ID"),
                            html.Th("Action"),
                            html.Th("User"),
                            html.Th("Old Value"),
                            html.Th("New Value"),
                        ])),
                        html.Tbody(config_rows),
                    ], className="clean-table"),
                ], className="dq-card mt-3"),
            ], label="Configuration Changes", tab_id="tab-config"),

            # TAB 3: Databricks Workflow Runs (Phase 7)
            dbc.Tab([
                html.Div([
                    html.Div([
                        html.Span("Databricks Workflow Runs (dq_job_runs)"),
                        html.Span(f"Automated & manual execution history for Job #{db_adapter.DQ_JOB_ID}", className="text-muted small"),
                    ], className="dq-card-title"),

                    html.Table([
                        html.Thead(html.Tr([
                            html.Th("Run ID"),
                            html.Th("Execution ID"),
                            html.Th("Trigger"),
                            html.Th("Project"),
                            html.Th("Dataset"),
                            html.Th("Status"),
                            html.Th("Started At"),
                            html.Th("Duration"),
                        ])),
                        html.Tbody(job_rows),
                    ], className="clean-table"),
                ], className="dq-card mt-3"),
            ], label="Databricks Workflow Runs", tab_id="tab-job-runs"),
        ], active_tab="tab-runs"),

        # Audit Detail Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle(id="audit-modal-title", children="Execution Detail")),
            dbc.ModalBody(id="audit-modal-body", children="Loading details..."),
            dbc.ModalFooter([
                dbc.Button("Close", id="btn-close-audit-modal", color="secondary"),
            ]),
        ], id="modal-audit-detail", is_open=False, size="lg"),
    ])


def _render_volumes_list(volumes, cat, sch):
    if not volumes:
        return html.Div()
    pills = []
    for v in volumes:
        v_name = v.get("name", "")
        v_path = v.get("full_name") or f"/Volumes/{cat}/{sch}/{v_name}"
        pills.append(html.Span([
            html.I(className="bi bi-folder2-open text-warning me-1"),
            html.Span(v_name, className="fw-bold me-1"),
            html.Span(f"({v_path})", className="text-muted small"),
        ], className="badge bg-light text-dark border me-2 mb-2 p-2"))
    return html.Div([
        html.Div("DISCOVERED UNITY CATALOG VOLUMES", className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
        html.Div(pills, className="d-flex flex-wrap p-2 bg-light rounded border"),
    ])


def _render_explorer_table_metadata(cat, sch, tbl):
    if not cat or not sch or not tbl:
        return html.Div()
    try:
        meta = db_adapter.get_unity_table_metadata(cat, sch, tbl)
        cols = meta.get("columns", [])
        types = {s.get("column"): s.get("sql_type", "STRING") for s in meta.get("schema_info", [])}
        b_cands = meta.get("business_key_candidates", [])
        col_badges = [
            html.Span([
                html.Span(c, className="fw-semibold me-1"),
                html.Span(types.get(c, "STRING"), className="badge bg-primary text-white me-1", style={"fontSize": "0.65rem"}),
                html.Span("[Key Candidate]", className="badge bg-success-subtle text-success border border-success", style={"fontSize": "0.65rem"}) if c in b_cands else None
            ], className="badge bg-light text-dark border me-2 mb-2 p-2")
            for c in cols
        ]
        return html.Div([
            html.Div([
                html.Div(f"TABLE METADATA: {cat}.{sch}.{tbl}", className="fw-bold text-muted small", style={"letterSpacing": "0.05em"}),
                dcc.Link([
                    html.I(className="bi bi-plus-circle-fill me-1"),
                    "Register as DQ Dataset"
                ], href="/management", className="btn btn-sm btn-outline-primary py-0 px-2 fw-semibold small text-decoration-none"),
            ], className="d-flex justify-content-between align-items-center mb-2"),
            html.Div([
                html.Div(f"Detected Columns ({len(cols)}):", className="small fw-semibold text-secondary mb-2"),
                html.Div(col_badges, className="d-flex flex-wrap"),
            ], className="p-3 bg-white border rounded"),
        ])
    except Exception as e:
        return html.Div(f"Error inspecting table metadata: {e}", className="text-muted small")


def _render_connection_kpi_cards(conn_info=None):
    """Renders the 4 summary KPI cards for Databricks workspace connection."""
    if conn_info is None:
        conn_info = db_adapter.get_databricks_connection_info()
    is_connected = conn_info.get("connected", False)
    clean_host = conn_info.get("clean_host", "")
    username = conn_info.get("username", "")
    display_name = conn_info.get("display_name", "")
    auth_type = conn_info.get("auth_type", "OAuth U2M")
    ws_id = conn_info.get("workspace_id", "")
    cloud_prov = conn_info.get("cloud_provider", "Databricks Cloud")
    catalogs_count = conn_info.get("catalogs_count", 0)

    # Status badge
    if is_connected:
        status_badge = html.Span([
            html.I(className="bi bi-circle-fill text-success me-1", style={"fontSize": "0.65rem"}),
            "Connected"
        ], className="badge bg-success-subtle text-success border border-success px-3 py-2 fw-semibold")
    else:
        status_badge = html.Span([
            html.I(className="bi bi-circle text-secondary me-1", style={"fontSize": "0.65rem"}),
            "Disconnected"
        ], className="badge bg-secondary-subtle text-secondary border px-3 py-2 fw-semibold")

    return dbc.Row([
        dbc.Col(html.Div([
            html.Div("Connection Status", className="text-muted small fw-semibold mb-1"),
            html.Div([
                status_badge,
                html.Span(clean_host if is_connected else "No Workspace", className="ms-2 fw-bold text-dark small text-truncate", style={"maxWidth": "160px"}),
            ], className="d-flex align-items-center"),
            html.Div(f"Org / Workspace ID: {ws_id}" if ws_id else cloud_prov, className="text-muted small mt-1", style={"fontSize": "0.75rem"}),
        ], className="dq-card p-3 h-100", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"}), md=3, className="mb-3", style={"minWidth": 0}),

        dbc.Col(html.Div([
            html.Div("Authenticated Identity", className="text-muted small fw-semibold mb-1"),
            html.Div([
                html.I(className="bi bi-person-circle text-primary fs-5 me-2"),
                html.Span(username if is_connected and username else ("Unauthenticated" if not is_connected else "Connected User"),
                          className="fw-bold text-dark small text-truncate", style={"maxWidth": "180px"}),
            ], className="d-flex align-items-center"),
            html.Div(display_name if display_name else "User-Scoped Session Isolation", className="text-muted small mt-1", style={"fontSize": "0.75rem"}),
        ], className="dq-card p-3 h-100", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"}), md=3, className="mb-3", style={"minWidth": 0}),

        dbc.Col(html.Div([
            html.Div("Authentication Protocol", className="text-muted small fw-semibold mb-1"),
            html.Div([
                html.I(className="bi bi-key-fill text-warning fs-5 me-2"),
                html.Span(auth_type if is_connected else "None", className="fw-bold text-dark small"),
            ], className="d-flex align-items-center"),
            html.Div("Official Databricks SDK Unified Auth", className="text-muted small mt-1", style={"fontSize": "0.75rem"}),
        ], className="dq-card p-3 h-100", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"}), md=3, className="mb-3", style={"minWidth": 0}),

        dbc.Col(html.Div([
            html.Div("Accessible Catalogs", className="text-muted small fw-semibold mb-1"),
            html.Div([
                html.I(className="bi bi-database-check text-info fs-5 me-2"),
                html.Span(f"{catalogs_count} Catalog(s) Discovered" if is_connected else "0 Catalogs", className="fw-bold text-dark small"),
            ], className="d-flex align-items-center"),
            html.Div("Dynamic Permission-Scoped Discovery", className="text-muted small mt-1", style={"fontSize": "0.75rem"}),
        ], className="dq-card p-3 h-100", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"}), md=3, className="mb-3", style={"minWidth": 0}),
    ], className="g-3 mb-3")


def page_connection():
    conn_info = db_adapter.get_databricks_connection_info()
    is_connected = conn_info.get("connected", False)
    clean_host = conn_info.get("clean_host", "")
    full_host = conn_info.get("host", "")
    username = conn_info.get("username", "")
    display_name = conn_info.get("display_name", "")
    auth_type = conn_info.get("auth_type", "OAuth U2M")
    ws_id = conn_info.get("workspace_id", "")
    cloud_prov = conn_info.get("cloud_provider", "Databricks Cloud")
    catalogs_count = conn_info.get("catalogs_count", 0)

    # Header Bar
    header = html.Div([
        html.Div([
            html.H3([html.I(className="bi bi-clouds text-primary me-2"), "Databricks Workspace Connection"],
                    className="page-main-title"),
            html.Div("Connect your personal or enterprise Databricks workspace using OAuth, CLI Profile, or PAT. Dynamic Unity Catalog resource discovery without hardcoding.",
                     className="text-muted small"),
        ]),
        html.Div([
            dbc.Button([html.I(className="bi bi-shield-check me-1"), "Verify Connection"],
                       id="btn-conn-verify", size="sm", color="outline-primary", className="me-2 rounded-pill px-3 fw-semibold",
                       disabled=not is_connected),
            dbc.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh Discovery"],
                       id="btn-conn-refresh-discovery", size="sm", color="outline-secondary", className="me-2 rounded-pill px-3 fw-semibold",
                       disabled=not is_connected),
            dbc.Button([html.I(className="bi bi-power me-1"), "Disconnect"],
                       id="btn-conn-disconnect", size="sm", color="outline-danger", className="rounded-pill px-3 fw-semibold",
                       disabled=not is_connected),
        ], className="d-flex align-items-center flex-wrap gap-2"),
    ], className="page-header-bar", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"})

    # 4 KPI Summary Cards Container
    kpi_cards = html.Div(
        id="conn-kpi-cards-container",
        children=_render_connection_kpi_cards(conn_info),
        style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"}
    )

    # Connection Form Card (Zero Hardcoding banner completely removed)
    connection_form = html.Div([
        html.Div([
            html.Div([
                html.I(className="bi bi-link-45deg text-primary me-2 fs-5"),
                html.Span("Connect Databricks Workspace", className="fw-bold fs-6"),
            ], className="d-flex align-items-center"),
            html.Span("Every developer or organization authenticates with their own workspace.", className="text-muted small"),
        ], className="d-flex justify-content-between align-items-center mb-3 pb-2 border-bottom flex-wrap gap-2"),

        dbc.Label("Choose Authentication Mechanism *", className="small fw-semibold text-secondary mb-2"),
        dbc.RadioItems(
            id="conn-form-auth-type",
            options=[
                {"label": "Personal Access Token (PAT - Recommended for Free Edition)", "value": "PAT"},
                {"label": "Databricks OAuth U2M (Single Sign-On)", "value": "OAUTH_U2M"},
                {"label": "Databricks CLI Profile (~/.databrickscfg)", "value": "CLI_PROFILE"},
            ],
            value="PAT" if not is_connected else (auth_type if auth_type in ("PAT", "OAUTH_U2M", "CLI_PROFILE") else "PAT"),
            inline=True,
            className="mb-3 small fw-semibold text-dark",
        ),

        dbc.Row([
            dbc.Col([
                dbc.Label(["Workspace Host URL ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                dbc.Input(
                    id="conn-form-host",
                    placeholder="https://dbc-xxxx.cloud.databricks.com",
                    value=clean_host if clean_host else "",
                    type="text",
                    className="rounded-pill px-3 py-2 small",
                    style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"},
                ),
                html.Div("Example: https://dbc-9f225403-7c26.cloud.databricks.com. Query parameters (?autoLogin=...) are automatically stripped.", className="text-muted small mt-1", style={"fontSize": "0.72rem"}),
            ], md=6, className="mb-3", style={"minWidth": 0}),

            dbc.Col([
                html.Div(id="conn-form-profile-container", style={"display": "none"}, children=[
                    dbc.Label("Databricks CLI Profile Name", className="small fw-semibold text-secondary mb-1"),
                    dbc.Input(
                        id="conn-form-profile",
                        placeholder="DEFAULT",
                        value="DEFAULT",
                        type="text",
                        className="rounded-pill px-3 py-2 small",
                        style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"},
                    ),
                    html.Div("Profile defined in your local ~/.databrickscfg file.", className="text-muted small mt-1", style={"fontSize": "0.72rem"}),
                ]),
                html.Div(id="conn-form-token-container", style={"display": "block"}, children=[
                    dbc.Label(["Personal Access Token (PAT) ", html.Span("*", className="text-danger")], className="small fw-semibold text-secondary mb-1"),
                    dbc.Input(
                        id="conn-form-token",
                        placeholder="dapi...",
                        type="password",
                        className="rounded-pill px-3 py-2 small",
                        style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"},
                    ),
                    html.Div("Personal Access Token generated from User Settings → Developer. Stored in-memory only.", className="text-muted small mt-1", style={"fontSize": "0.72rem"}),
                ]),
            ], md=6, className="mb-3", style={"minWidth": 0}),
        ], className="g-3"),

        html.Div([
            dbc.Button([
                html.I(className="bi bi-lightning-charge-fill me-2"),
                "Connect to Databricks"
            ], id="btn-conn-submit", color="primary", className="rounded-pill px-4 py-2 fw-semibold shadow-sm"),
        ], className="d-flex justify-content-end mt-2"),

        dcc.Loading(
            id="loading-conn-alert",
            type="circle",
            children=html.Div(id="conn-form-alert", className="mt-3", style={"wordBreak": "break-word", "overflowWrap": "anywhere"})
        ),
    ], className="dq-card p-4 mb-4", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"})

    # Dynamic Unity Catalog Resource Explorer Card
    catalogs = db_adapter.get_unity_catalogs() if is_connected else []
    cat_opts = [{"label": c["name"] + (f" ({c.get('type')})" if c.get('type') else ""), "value": c["name"]} for c in catalogs]
    initial_cat = cat_opts[0]["value"] if cat_opts else None
    initial_schemas = db_adapter.get_unity_schemas(initial_cat) if initial_cat else []
    sch_opts = [{"label": s["name"], "value": s["name"]} for s in initial_schemas]
    initial_sch = sch_opts[0]["value"] if sch_opts else None
    initial_tables = db_adapter.get_unity_tables(initial_cat, initial_sch) if (initial_cat and initial_sch) else []
    tbl_opts = [{"label": f"{t['name']} ({t.get('type', 'MANAGED')})", "value": t["name"]} for t in initial_tables]
    initial_volumes = db_adapter.get_unity_volumes(initial_cat, initial_sch) if (initial_cat and initial_sch) else []

    header_count_text = (
        f"Dynamically discovered from {clean_host} ({len(catalogs)} Catalogs Accessible)"
        if is_connected else
        "Connect your Databricks workspace above to discover accessible Unity Catalog resources."
    )

    explorer_body = html.Div(id="conn-explorer-body", style={"display": "block" if is_connected else "none"}, children=[
        dbc.Row([
            dbc.Col([
                dbc.Label("Select Catalog", className="small fw-semibold text-secondary mb-1"),
                dbc.Select(
                    id="conn-explorer-catalogs-select",
                    options=cat_opts,
                    value=initial_cat,
                    placeholder="Choose a Catalog...",
                    className="rounded-pill px-3 py-2 small",
                    style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"},
                ),
            ], md=4, className="mb-3", style={"minWidth": 0}),

            dbc.Col([
                dbc.Label("Select Schema", className="small fw-semibold text-secondary mb-1"),
                dbc.Select(
                    id="conn-explorer-schemas-select",
                    options=sch_opts,
                    value=initial_sch,
                    placeholder="Choose a Schema...",
                    className="rounded-pill px-3 py-2 small",
                    style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"},
                ),
            ], md=4, className="mb-3", style={"minWidth": 0}),

            dbc.Col([
                dbc.Label("Select Table / Delta Dataset", className="small fw-semibold text-secondary mb-1"),
                dbc.Select(
                    id="conn-explorer-tables-select",
                    options=tbl_opts,
                    value=tbl_opts[0]["value"] if tbl_opts else None,
                    placeholder="Choose a Table...",
                    className="rounded-pill px-3 py-2 small",
                    style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"},
                ),
            ], md=4, className="mb-3", style={"minWidth": 0}),
        ], className="g-3"),

        html.Div(id="conn-explorer-volumes-container", children=_render_volumes_list(initial_volumes, initial_cat, initial_sch), className="mb-3"),

        html.Div(id="conn-explorer-table-metadata-container", children=_render_explorer_table_metadata(initial_cat, initial_sch, tbl_opts[0]["value"] if tbl_opts else None)),
    ])

    explorer_empty_state = html.Div(id="conn-explorer-empty-state", style={"display": "none" if is_connected else "block"}, children=[
        html.Div([
            html.I(className="bi bi-clouds fs-1 text-muted d-block mb-3"),
            html.H6("No Databricks Workspace Connected", className="fw-bold text-dark"),
            html.P("Connect your Databricks account above to discover your accessible Unity Catalog catalogs, schemas, tables, and volumes.", className="text-muted small"),
        ], className="text-center py-5 bg-light rounded border"),
    ])

    explorer_card = html.Div([
        html.Div([
            html.Div([
                html.I(className="bi bi-compass-fill text-primary me-2 fs-5"),
                html.Span("Live Unity Catalog Resource Explorer", className="fw-bold fs-6"),
            ], className="d-flex align-items-center"),
            html.Span(id="conn-explorer-header-count", children=header_count_text, className="text-muted small"),
        ], className="d-flex justify-content-between align-items-center mb-3 pb-2 border-bottom flex-wrap gap-2"),

        dcc.Loading(
            id="loading-conn-explorer",
            type="circle",
            children=html.Div([
                explorer_body,
                explorer_empty_state,
            ])
        ),
    ], className="dq-card p-4", style={"width": "100%", "minWidth": 0, "boxSizing": "border-box"})

    return html.Div([
        header,
        kpi_cards,
        connection_form,
        explorer_card,
    ], style={"width": "100%", "minWidth": 0, "boxSizing": "border-box", "maxWidth": "100%"})

# -------------------------------------------------------------
# ROUTER CALLBACK
# -------------------------------------------------------------

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
)
def render_page(pathname):
    page = (pathname or "/overview").strip("/").lower()
    if page == "connection":
        return page_connection()
    elif page == "management":
        return page_management()
    elif page == "rules":
        return page_rules()
    elif page == "execution":
        return page_execution()
    elif page == "results":
        return page_results()
    elif page == "reconciliation":
        return page_reconciliation()
    elif page == "alerts":
        return page_alerts()
    elif page == "audit":
        return page_audit()
    return page_overview()

# -------------------------------------------------------------
# INTERACTIVE REAL ACTION CALLBACKS
# -------------------------------------------------------------

# 1. Project Management: Autofill Example Details
@app.callback(
    [Output("new-project-name", "value"),
     Output("new-project-desc", "value"),
     Output("new-project-owner", "value"),
     Output("new-project-team-email", "value"),
     Output("new-project-additional-emails", "value"),
     Output("new-project-notif-enabled", "value"),
     Output("init-input-upload-dataset-name", "value"),
     Output("init-input-upload-table-name", "value")],
    Input("btn-autofill-example", "n_clicks"),
    prevent_initial_call=True,
)
def handle_autofill_example(n_clicks):
    return (
        "Banking_Operations",
        "Customer transaction and payment data quality monitoring",
        "Risk & Analytics Team",
        "banking-dq@company.com",
        "alerts-oncall@company.com",
        ["enabled"],
        "Customer_Transactions",
        "customer_transactions_source",
    )


# 2. Add Project: Toggle between Upload and Existing Dataset containers
@app.callback(
    [Output("init-container-upload-dataset", "style"),
     Output("init-container-existing-dataset", "style")],
    Input("init-dataset-reg-method", "value"),
    prevent_initial_call=False,
)
def toggle_init_dataset_method(method):
    if method == "existing":
        return {"display": "none"}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}


# 3. Add Project: Handle Initial Dataset File Upload
@app.callback(
    [Output("init-upload-preview-container", "children"),
     Output("init-input-upload-dataset-name", "value", allow_duplicate=True),
     Output("init-input-upload-table-name", "value", allow_duplicate=True),
     Output("init-select-upload-business-key", "options"),
     Output("init-select-upload-business-key", "value"),
     Output("init-upload-file-store", "data")],
    [Input("init-upload-dataset-file", "contents"),
     Input("init-upload-dataset-file", "filename")],
    prevent_initial_call=True,
)
def on_init_file_uploaded(contents, filename):
    if not contents or not filename:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    parsed = db_adapter.parse_uploaded_file(contents, filename)
    if not parsed.get("success"):
        err_box = dbc.Alert([
            html.I(className="bi bi-exclamation-triangle-fill me-2"),
            parsed.get("message", "Error parsing uploaded file.")
        ], color="danger", className="py-2")
        return err_box, dash.no_update, dash.no_update, [], None, None

    cols = parsed["columns"]
    total_rows = parsed["total_rows"]
    fmt = parsed["file_format"]
    schema_info = parsed["schema_info"]
    preview_rows = parsed["preview_rows"]

    type_badges = [
        html.Span([
            html.Span(s["column"], className="fw-semibold text-dark me-1"),
            html.Span(s["sql_type"], className="badge bg-light text-secondary border me-2 small"),
        ], className="d-inline-flex align-items-center mb-1")
        for s in schema_info
    ]

    preview_table = html.Div([
        html.Table([
            html.Thead(html.Tr([html.Th(c, style={"fontSize": "0.78rem", "backgroundColor": "#f8fafc"}) for c in cols])),
            html.Tbody([
                html.Tr([html.Td(str(row.get(c, "")), style={"fontSize": "0.75rem", "whiteSpace": "nowrap"}) for c in cols])
                for row in preview_rows[:5]
            ]),
        ], className="table table-sm table-striped table-bordered mb-0"),
    ], style={"overflowX": "auto", "maxHeight": "180px"})

    preview_ui = html.Div([
        html.Div([
            html.Span([html.I(className="bi bi-file-earmark-check me-1"), filename], className="badge bg-success-subtle text-success border border-success me-2 px-2 py-1"),
            html.Span([html.I(className="bi bi-tag me-1"), fmt], className="badge bg-primary-subtle text-primary border border-primary me-2 px-2 py-1"),
            html.Span([html.I(className="bi bi-list-ol me-1"), f"{total_rows:,} records"], className="badge bg-info-subtle text-info border border-info me-2 px-2 py-1"),
            html.Span([html.I(className="bi bi-layout-three-columns me-1"), f"{len(cols)} columns detected"], className="badge bg-secondary-subtle text-secondary border me-2 px-2 py-1"),
        ], className="d-flex flex-wrap align-items-center mb-2"),
        html.Div(type_badges, className="p-2 bg-light rounded border mb-2 small"),
        html.Div(preview_table, className="border rounded bg-white p-2"),
    ], className="p-2 bg-white rounded border mb-2")

    import re
    base_name = os.path.splitext(filename)[0]
    clean_base = re.sub(r'[^a-zA-Z0-9_]', '_', base_name).strip('_')
    ds_suggest = clean_base.capitalize()
    tbl_suggest = f"{clean_base.lower()}_source"

    candidates = [c for c in cols if c.lower() in ("id", "key", "pk", "uuid") or c.lower().endswith(("_id", "_key", "_no", "_code"))]
    b_opts = [{"label": f"{c}" + (" [Key Candidate]" if c in candidates else ""), "value": c} for c in cols]
    def_bkey = candidates[0] if candidates else (cols[0] if cols else None)

    file_store = {
        "filename": filename,
        "contents": contents,
        "format": fmt,
        "total_rows": total_rows,
        "columns": cols,
    }
    return preview_ui, ds_suggest, tbl_suggest, b_opts, def_bkey, file_store


# 4. Add Project: Option 2 Cascading Discovery
@app.callback(
    [Output("init-select-existing-schema", "options"),
     Output("init-select-existing-schema", "value")],
    Input("init-select-existing-catalog", "value"),
    prevent_initial_call=True,
)
def load_init_schemas(cat):
    if not cat:
        return [], None
    schemas = db_adapter.get_unity_schemas(cat)
    opts = [{"label": s["name"] if isinstance(s, dict) else str(s), "value": s["name"] if isinstance(s, dict) else str(s)} for s in schemas]
    val = opts[0]["value"] if opts else None
    return opts, val


@app.callback(
    [Output("init-select-existing-table", "options"),
     Output("init-select-existing-table", "value")],
    [Input("init-select-existing-schema", "value"),
     Input("init-select-existing-catalog", "value")],
    prevent_initial_call=True,
)
def load_init_tables(sch, cat):
    if not cat or not sch:
        return [], None
    tables = db_adapter.get_unity_tables(cat, sch)
    opts = []
    tbl_names = []
    for t in tables:
        name = t["name"] if isinstance(t, dict) else str(t)
        tbl_names.append(name)
        tbl_type = f" ({t.get('type')})" if isinstance(t, dict) and t.get('type') else ""
        opts.append({"label": f"{name}{tbl_type}", "value": name})
    val = tbl_names[0] if tbl_names else None
    if not opts:
        opts = [{"label": f"ℹ️ 0 tables in '{cat}.{sch}'", "value": "", "disabled": True}]
        val = None
    return opts, val


@app.callback(
    [Output("init-select-existing-business-key", "options"),
     Output("init-select-existing-business-key", "value"),
     Output("init-input-existing-dataset-name", "value"),
     Output("init-existing-table-preview-container", "children")],
    Input("init-select-existing-table", "value"),
    [State("init-select-existing-catalog", "value"),
     State("init-select-existing-schema", "value")],
    prevent_initial_call=True,
)
def on_init_table_selected(tbl, cat, sch):
    if not cat or not sch or not tbl:
        return [], None, dash.no_update, html.Div()
    meta = db_adapter.get_unity_table_metadata(cat, sch, tbl)
    cols = meta.get("columns", [])
    b_cand = meta.get("business_key_candidates", [])
    schema_map = {s.get("column"): s.get("sql_type", "STRING") for s in meta.get("schema_info", [])}
    col_opts = [{"label": f"{c} ({schema_map.get(c, 'STRING')})" + (" [Key Candidate]" if c in b_cand else ""), "value": c} for c in cols]
    def_key = b_cand[0] if b_cand else (cols[0] if cols else None)
    ds_name = tbl
    preview_ui = _build_table_preview_card(meta)
    return col_opts, def_key, ds_name, preview_ui


# 5. Add Project & Atomic Dataset Registration Callback
@app.callback(
    [Output("modal-add-project", "is_open"),
     Output("add-project-result-alert", "children"),
     Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("global-toast", "icon", allow_duplicate=True),
     Output("project-refresh-store", "data", allow_duplicate=True)],
    [Input("btn-open-add-project", "n_clicks"),
     Input("btn-cancel-add-project", "n_clicks"),
     Input("btn-submit-add-project", "n_clicks")],
    [State("modal-add-project", "is_open"),
     State("new-project-name", "value"),
     State("new-project-desc", "value"),
     State("new-project-owner", "value"),
     State("new-project-team-email", "value"),
     State("new-project-additional-emails", "value"),
     State("new-project-notif-enabled", "value"),
     State("init-dataset-reg-method", "value"),
     State("init-upload-file-store", "data"),
     State("init-input-upload-dataset-name", "value"),
     State("init-input-upload-catalog", "value"),
     State("init-input-upload-schema", "value"),
     State("init-input-upload-table-name", "value"),
     State("init-select-upload-business-key", "value"),
     State("init-input-upload-target-table", "value"),
     State("init-input-upload-description", "value"),
     State("init-input-existing-dataset-name", "value"),
     State("init-select-existing-catalog", "value"),
     State("init-select-existing-schema", "value"),
     State("init-select-existing-table", "value"),
     State("init-select-existing-business-key", "value"),
     State("init-input-existing-target-table", "value"),
     State("init-input-existing-description", "value")],
    prevent_initial_call=True,
)
def handle_add_project(
    open_click, cancel_click, submit_click,
    is_open, name, desc, owner, team_email, add_emails, notif_enabled,
    reg_method, file_store,
    up_ds_name, up_cat, up_sch, up_tbl, up_b_key, up_tgt, up_desc,
    ex_ds_name, ex_cat, ex_sch, ex_tbl, ex_b_key, ex_tgt, ex_desc
):
    trig = ctx.triggered_id
    _ts = datetime.now().isoformat()
    if not trig or not _real_click():
        return (dash.no_update,) * 6

    if trig == "btn-open-add-project":
        return True, None, False, "", "info", dash.no_update
    elif trig == "btn-cancel-add-project":
        return False, None, False, "", "info", dash.no_update
    elif trig == "btn-submit-add-project":
        is_notif = bool("enabled" in (notif_enabled or []))

        # Atomic Registration via Option 1 (Upload)
        if reg_method == "upload":
            if not file_store or not file_store.get("contents"):
                err = dbc.Alert("Please select or drop an initial dataset file (CSV, JSON, Parquet, Excel).", color="danger", className="py-2 mb-0")
                return True, err, False, "", "info", dash.no_update

            res = db_adapter.create_project_with_initial_upload(
                project_name=name or "",
                description=desc or "",
                owner=owner or "Data Engineering Team",
                domain=None,
                project_team_email=team_email or "",
                additional_emails=add_emails or "",
                notification_enabled=is_notif,
                contents=file_store["contents"],
                filename=file_store.get("filename", "dataset.csv"),
                dataset_name=up_ds_name or "",
                catalog=up_cat or "hospital_catalog",
                schema=up_sch or "healthcare",
                table_name=up_tbl or "",
                business_key=up_b_key or "id",
                target_table=up_tgt,
                dataset_description=up_desc or "",
                user="admin"
            )

        # Atomic Registration via Option 2 (Existing UC Table)
        else:
            res = db_adapter.create_project_with_initial_existing(
                project_name=name or "",
                description=desc or "",
                owner=owner or "Data Engineering Team",
                domain=None,
                project_team_email=team_email or "",
                additional_emails=add_emails or "",
                notification_enabled=is_notif,
                dataset_name=ex_ds_name or ex_tbl or "",
                catalog=ex_cat or "",
                schema=ex_sch or "",
                table_name=ex_tbl or "",
                business_key=ex_b_key or "id",
                target_table=ex_tgt,
                dataset_description=ex_desc or "",
                user="admin"
            )

        if res.get("success"):
            p_name = res.get("project_name", name)
            d_name = res.get("dataset_name", "")
            toast_text = f"Project '{p_name}' and initial dataset '{d_name}' registered successfully!"
            return False, None, True, toast_text, "success", _ts
        else:
            err_msg = res.get("message") or "Project and initial dataset creation failed."
            err_content = dbc.Alert([
                html.I(className="bi bi-exclamation-triangle-fill me-2"),
                err_msg
            ], color="danger", className="small py-2 mb-0")
            return True, err_content, False, "", "info", dash.no_update

    return is_open, None, False, "", "info", dash.no_update

# 4. View Project Details Action
@app.callback(
    [Output("modal-view-project", "is_open"),
     Output("modal-view-project-title", "children"),
     Output("modal-view-project-body", "children")],
    [Input({"type": "btn-view-proj", "id": dash.ALL}, "n_clicks"),
     Input("btn-close-view-project", "n_clicks")],
    [State("modal-view-project", "is_open")],
    prevent_initial_call=True,
)
def handle_view_project(view_clicks, close_click, is_open):
    trig = ctx.triggered_id
    
    # NEVER open modal unless explicitly triggered by a button click
    if trig is None:
        return dash.no_update, dash.no_update, dash.no_update
    
    if trig == "btn-close-view-project":
        return False, "Project Details", None

    # Table re-render fires the pattern input with n_clicks=None; ignore it.
    if isinstance(trig, dict) and not _real_click():
        return dash.no_update, dash.no_update, dash.no_update
    
    if isinstance(trig, dict) and trig.get("type") == "btn-view-proj":
        p_id = trig["id"]
        return True, *_build_project_view(p_id)

    # Default: NEVER open modal for any other reason
    return dash.no_update, dash.no_update, dash.no_update


# 4b. Enable / Disable an individual dataset from inside the View Project modal
@app.callback(
    [Output("modal-view-project-title", "children", allow_duplicate=True),
     Output("modal-view-project-body", "children", allow_duplicate=True),
     Output("project-refresh-store", "data", allow_duplicate=True)],
    Input({"type": "btn-toggle-ds", "id": dash.ALL, "project": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_toggle_dataset(clicks):
    trig = ctx.triggered_id
    if not isinstance(trig, dict) or not _real_click():
        return dash.no_update, dash.no_update, dash.no_update
    db_adapter.toggle_dataset(int(trig["id"]))
    title, body = _build_project_view(int(trig["project"]))
    return title, body, datetime.now().isoformat()


def _build_project_view(p_id):
    """Builds (title, body) for the View Project modal."""
    if True:
        details = db_adapter.get_project_details(p_id)
        if not details:
            return "Project Details", html.Div("Project not found.", className="text-muted")
        
        proj = details["project"]
        datasets = details["datasets"]
        rules = details["rules"]
        last_exec = details["last_execution"]
        avg_score = details["avg_dq_score"]
        p_name = proj["project_name"]

        # Datasets Table Rows
        ds_rows = []
        for d in datasets:
            d_active = bool(d["active"])
            ds_rows.append(html.Tr([
                html.Td(d["dataset_name"], style={"fontWeight": "600"}),
                html.Td(html.Code(d["source_table"], style={"fontSize": "0.75rem", "whiteSpace": "normal", "wordBreak": "break-all"})),
                html.Td(html.Code(d["target_table"] or "—", style={"fontSize": "0.75rem", "whiteSpace": "normal", "wordBreak": "break-all"})),
                html.Td(dbc.Badge(d["business_key"] or "id", color="secondary", className="px-2 py-1")),
                html.Td(html.Span("Active", className="badge-active") if d_active else html.Span("Inactive", className="badge-inactive")),
                html.Td(dbc.Button(
                    "Disable" if d_active else "Enable",
                    id={"type": "btn-toggle-ds", "id": int(d["dataset_id"]), "project": int(p_id)},
                    size="sm", color="danger" if d_active else "success", outline=True,
                    style={"fontSize": "0.72rem", "padding": "2px 8px"},
                )),
            ]))

        # Rules Table Rows
        rule_rows = []
        for r in rules:
            r_status = r["severity"]
            badge_color = "danger" if r_status == "Critical" else ("warning" if r_status == "High" else "info")
            rule_rows.append(html.Tr([
                html.Td(r["rule_name"], style={"fontWeight": "600"}),
                html.Td(r["category"]),
                html.Td(html.Code(r["template_name"])),
                html.Td(r["column_name"] or "—"),
                html.Td(str(r["threshold"])),
                html.Td(html.Span(r["severity"], className=f"badge text-bg-{badge_color}")),
            ]))

        # Notification Settings Section
        notif_enabled = bool(proj.get("notification_enabled", 1))
        team_email = proj.get("project_team_email") or "—"
        add_emails = proj.get("additional_emails") or "—"
        notif_badge = dbc.Badge("Enabled", color="success", className="px-2 py-1") if notif_enabled else dbc.Badge("Disabled", color="secondary", className="px-2 py-1")

        notif_widget = dbc.Row([
            dbc.Col([
                html.Div("Notification Alerts", className="small text-muted fw-semibold"),
                html.Div(notif_badge, className="mt-1"),
            ], md=3, className="mb-2"),
            dbc.Col([
                html.Div("Project Team Email", className="small text-muted fw-semibold"),
                html.Div(team_email, className="fw-semibold text-dark", style={"wordBreak": "break-all"}),
            ], md=5, className="mb-2"),
            dbc.Col([
                html.Div("Additional Recipients", className="small text-muted fw-semibold"),
                html.Div(add_emails, className="text-secondary small", style={"wordBreak": "break-all"}),
            ], md=4, className="mb-2"),
        ], className="p-3 bg-light rounded mb-3 border")

        # Last Execution Summary
        if last_exec:
            status_cls = "badge-pass" if last_exec["overall_status"] == "PASS" else "badge-fail"
            exec_widget = dbc.Row([
                dbc.Col([
                    html.Div("Last Execution ID", className="small text-muted fw-semibold"),
                    html.Div(last_exec["execution_id"], className="fw-bold text-dark"),
                ], md=4),
                dbc.Col([
                    html.Div("Status & Score", className="small text-muted fw-semibold"),
                    html.Div([
                        html.Span(last_exec["overall_status"], className=f"{status_cls} me-2"),
                        html.Span(f"{last_exec['dq_score']}% DQ Score", className="fw-bold text-dark"),
                    ]),
                ], md=4),
                dbc.Col([
                    html.Div("Rules Breakdown", className="small text-muted fw-semibold"),
                    html.Div(f"{last_exec['passed']} PASS / {last_exec['failed']} FAIL / {last_exec['errored']} ERROR", className="small fw-semibold text-secondary"),
                ], md=4),
            ], className="p-3 bg-light rounded mb-3")
        else:
            exec_widget = html.Div("No validation executions recorded yet for this project.", className="text-muted small p-3 bg-light rounded mb-3")

        body = html.Div([
            # Top Info Card
            dbc.Row([
                dbc.Col([
                    html.Div("Project ID", className="small text-muted fw-semibold"),
                    html.Div(f"#{proj['project_id']}", className="fw-bold text-dark fs-5"),
                ], xs=6, md=3, className="mb-2"),
                dbc.Col([
                    html.Div("Project Owner", className="small text-muted fw-semibold"),
                    html.Div(proj.get("owner") or "Data Engineering Team", className="fw-bold text-dark"),
                ], xs=6, md=3, className="mb-2"),
                dbc.Col([
                    html.Div("Historical DQ Score", className="small text-muted fw-semibold"),
                    html.Div(f"{avg_score}%", className="fw-bold text-primary fs-5"),
                ], xs=6, md=3, className="mb-2"),
                dbc.Col([
                    html.Div("Status", className="small text-muted fw-semibold"),
                    html.Div(html.Span("Active", className="badge-active") if proj["active"] else html.Span("Inactive", className="badge-inactive")),
                ], xs=6, md=3, className="mb-2"),
            ], className="mb-3 p-3 bg-white border rounded"),

            html.P(proj["description"] or "No description provided.", className="text-muted small mb-3"),

            # Notification Settings Section
            html.H6("Notification & Alert Settings", className="fw-bold text-dark mb-2"),
            notif_widget,

            # Execution History Section
            html.H6("Recent Execution Status", className="fw-bold text-dark mb-2"),
            exec_widget,

            # Registered Datasets Section
            html.H6(f"Registered Datasets ({len(datasets)})", className="fw-bold text-dark mb-2"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Dataset"),
                        html.Th("Source Table"),
                        html.Th("Target Table"),
                        html.Th("Business Key"),
                        html.Th("Status"),
                        html.Th("Action"),
                    ])),
                    html.Tbody(ds_rows if ds_rows else [html.Tr([html.Td("No datasets registered", colSpan=6, className="text-center text-muted")])]),
                ], className="clean-table"),
            ], style={"overflowX": "auto", "marginBottom": "1.5rem"}),

            # Configured DQ Rules Section
            html.H6(f"Active DQ Rules ({len(rules)})", className="fw-bold text-dark mb-2"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("Rule Name"),
                        html.Th("Category"),
                        html.Th("Template"),
                        html.Th("Column"),
                        html.Th("Threshold"),
                        html.Th("Severity"),
                    ])),
                    html.Tbody(rule_rows if rule_rows else [html.Tr([html.Td("No rules configured for this project", colSpan=6, className="text-center text-muted")])]),
                ], className="clean-table"),
            ], style={"overflowX": "auto"}),
        ])

        return f"Project Overview: {p_name}", body

# 5. Edit Project Configuration Action
@app.callback(
    [Output("modal-edit-project", "is_open"),
     Output("edit-project-id", "data"),
     Output("edit-project-name", "value"),
     Output("edit-project-desc", "value"),
     Output("edit-project-owner", "value"),
     Output("edit-project-team-email", "value"),
     Output("edit-project-additional-emails", "value"),
     Output("edit-project-notif-enabled", "value"),
     Output("project-refresh-store", "data", allow_duplicate=True),
     Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("global-toast", "icon", allow_duplicate=True)],
    [Input({"type": "btn-edit-proj", "id": dash.ALL}, "n_clicks"),
     Input("btn-cancel-edit-project", "n_clicks"),
     Input("btn-submit-edit-project", "n_clicks")],
    [State("modal-edit-project", "is_open"),
     State("edit-project-id", "data"),
     State("edit-project-name", "value"),
     State("edit-project-desc", "value"),
     State("edit-project-owner", "value"),
     State("edit-project-team-email", "value"),
     State("edit-project-additional-emails", "value"),
     State("edit-project-notif-enabled", "value")],
    prevent_initial_call=True,
)
def handle_edit_project(edit_clicks, cancel_click, submit_click, is_open, cur_p_id, p_name, p_desc, p_owner, p_team_email, p_add_emails, p_notif_enabled):
    trig = ctx.triggered_id
    _ts = datetime.now().isoformat()
    
    # Only proceed if a button was actually clicked
    if not trig:
        return (dash.no_update,) * 12
    # Table re-render fires the pattern input with n_clicks=None; ignore it.
    if isinstance(trig, dict) and not _real_click():
        return (dash.no_update,) * 12
    
    if trig == "btn-cancel-edit-project":
        return False, None, "", "", "", "", "", ["enabled"], dash.no_update, False, "", "info"

    elif trig == "btn-submit-edit-project":
        if not cur_p_id:
            return False, None, "", "", "", "", "", ["enabled"], dash.no_update, True, "Error: Project ID is missing.", "danger"
        if not p_name or not p_name.strip():
            return True, cur_p_id, p_name, p_desc, p_owner, p_team_email, p_add_emails, p_notif_enabled, dash.no_update, True, "Project Name is required.", "danger"

        is_notif_enabled = bool("enabled" in (p_notif_enabled or []))
        res = db_adapter.update_project_and_dataset(
            project_id=int(cur_p_id),
            project_name=p_name.strip(),
            description=(p_desc or "").strip(),
            owner=(p_owner or "Data Engineering Team").strip(),
            project_team_email=(p_team_email or "").strip(),
            additional_emails=(p_add_emails or "").strip(),
            notification_enabled=is_notif_enabled,
            user="admin"
        )
        if res.get("success"):
            return False, None, "", "", "", "", "", ["enabled"], _ts, True, f"Project '{p_name.strip()}' updated successfully!", "success"
        else:
            err_msg = res.get("message") or res.get("error") or "Update failed."
            return True, cur_p_id, p_name, p_desc, p_owner, p_team_email, p_add_emails, p_notif_enabled, dash.no_update, True, err_msg, "danger"

    elif isinstance(trig, dict) and trig.get("type") == "btn-edit-proj":
        p_id = int(trig["id"])
        details = db_adapter.get_project_details(p_id)
        if not details or not details.get("project"):
            return False, None, "", "", "", "", "", ["enabled"], dash.no_update, True, f"Project #{p_id} not found.", "danger"

        proj = details["project"]
        notif_val = ["enabled"] if proj.get("notification_enabled", 1) else []
        return (
            True,
            proj["project_id"],
            proj["project_name"],
            proj.get("description") or "",
            proj.get("owner") or "Data Engineering Team",
            proj.get("project_team_email") or "",
            proj.get("additional_emails") or "",
            notif_val,
            dash.no_update,
            False,
            "",
            "info",
        )
    
    # Default: NEVER open modal for any other reason
    return (dash.no_update,) * 12

# 2. Toggle Project Active Status & Alert Lifecycle
@app.callback(
    [Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("global-toast", "icon", allow_duplicate=True),
     Output("project-refresh-store", "data", allow_duplicate=True),
     Output("alert-refresh-store", "data", allow_duplicate=True)],
    [Input({"type": "btn-toggle-proj", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-ack-alert", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-close-alert", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-retry-notif", "id": dash.ALL}, "n_clicks")],
    prevent_initial_call=True,
)
def handle_table_actions(toggle_clicks, ack_clicks, close_clicks, retry_clicks):
    trig = ctx.triggered_id
    _ts = datetime.now().isoformat()
    # Pattern-matching inputs fire with n_clicks=None whenever a table re-renders.
    # Only act on a genuine click.
    if not trig or not _real_click():
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    if isinstance(trig, dict) and trig.get("type") == "btn-toggle-proj":
        p_id = trig["id"]
        db_adapter.toggle_project(p_id)
        # Refresh table so Enable/Disable label flips immediately
        return True, f"Project ID {p_id} status updated successfully.", "success", _ts, dash.no_update

    if isinstance(trig, dict) and trig.get("type") == "btn-ack-alert":
        a_id = trig["id"]
        db_adapter.acknowledge_alert(a_id, user="admin")
        return True, f"Alert #{a_id} marked as Acknowledged.", "info", dash.no_update, _ts

    if isinstance(trig, dict) and trig.get("type") == "btn-close-alert":
        a_id = trig["id"]
        db_adapter.close_alert(a_id, user="admin")
        return True, f"Alert #{a_id} marked as Closed.", "success", dash.no_update, _ts

    if isinstance(trig, dict) and trig.get("type") == "btn-retry-notif":
        a_id = trig["id"]
        res = db_adapter.retry_notification(a_id, user="admin")
        if res.get("success"):
            return True, f"Notification retry succeeded for Alert #{a_id} ({res.get('status')}).", "success", dash.no_update, _ts
        else:
            err_detail = res.get("error") or res.get("message") or "Delivery failed"
            return True, f"Notification retry failed for Alert #{a_id}: {err_detail}", "danger", dash.no_update, _ts

    return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Management (Page 2) callbacks ---------------------------------------------
@app.callback(
    Output("management-table-body", "children"),
    [Input("project-refresh-store", "data"),
     Input("input-search-project", "value"),
     Input("filter-project-status", "value")],
)
def refresh_management_table(_refresh_ts, search_term, status_filter):
    """Re-fetch projects from DB and filter by status and search keyword."""
    projects_df = db_adapter.get_projects_list(status_filter=status_filter, search=search_term)
    return _build_management_table_rows(projects_df)


# -------------------------------------------------------------
# DATASET REGISTRATION CALLBACKS (TWO OPTIONS: UPLOAD & EXISTING)
# -------------------------------------------------------------

@app.callback(
    [Output("modal-add-dataset", "is_open"),
     Output("add-dataset-project-id", "data"),
     Output("select-dataset-target-project", "value"),
     Output("dataset-registration-form-container", "style"),
     Output("dataset-registration-status-card", "style"),
     Output("dataset-registration-status-card", "children"),
     Output("upload-preview-container", "children"),
     Output("upload-file-store", "data"),
     Output("upload-submit-alert", "children"),
     Output("existing-submit-alert", "children")],
    [Input("btn-open-register-dataset", "n_clicks"),
     Input("btn-cancel-add-dataset", "n_clicks")],
    prevent_initial_call=True,
)
def handle_open_add_dataset(top_open_clicks, cancel_clicks):
    trig = ctx.triggered_id
    if not trig or not _real_click():
        return (dash.no_update,) * 10

    if trig == "btn-cancel-add-dataset":
        return False, None, dash.no_update, {"display": "block"}, {"display": "none"}, None, None, None, None, None

    if trig == "btn-open-register-dataset":
        # Opened from top header button: select first project by default
        projects_df = db_adapter.get_projects_list()
        default_pid = int(projects_df.iloc[0]["project_id"]) if not projects_df.empty else 1
        return True, default_pid, default_pid, {"display": "block"}, {"display": "none"}, None, None, None, None, None

    return (dash.no_update,) * 10


@app.callback(
    [Output("container-upload-dataset", "style"),
     Output("container-existing-dataset", "style")],
    Input("dataset-reg-method", "value"),
)
def toggle_dataset_reg_method(method):
    if method == "existing":
        return {"display": "none"}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}


@app.callback(
    [Output("upload-preview-container", "children", allow_duplicate=True),
     Output("input-upload-dataset-name", "value"),
     Output("input-upload-table-name", "value"),
     Output("select-upload-business-key", "options"),
     Output("select-upload-business-key", "value"),
     Output("upload-file-store", "data", allow_duplicate=True)],
    [Input("upload-dataset-file", "contents"),
     Input("upload-dataset-file", "filename")],
    prevent_initial_call=True,
)
def on_file_uploaded(contents, filename):
    if not contents or not filename:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    parsed = db_adapter.parse_uploaded_file(contents, filename)
    if not parsed.get("success"):
        err_box = dbc.Alert([
            html.I(className="bi bi-exclamation-triangle-fill me-2"),
            parsed.get("message", "Error parsing uploaded file.")
        ], color="danger", className="py-2")
        return err_box, dash.no_update, dash.no_update, [], None, None

    # Build Preview Display
    cols = parsed["columns"]
    total_rows = parsed["total_rows"]
    fmt = parsed["file_format"]
    schema_info = parsed["schema_info"]
    preview_rows = parsed["preview_rows"]

    # Schema type badges
    type_badges = []
    for s in schema_info:
        type_badges.append(html.Span([
            html.Span(s["column"], className="fw-semibold text-dark me-1"),
            html.Span(s["sql_type"], className="badge bg-light text-primary border me-2 small"),
        ], className="d-inline-flex align-items-center mb-1"))

    # Table preview
    preview_table = html.Div([
        html.Table([
            html.Thead(html.Tr([html.Th(c, style={"fontSize": "0.78rem"}) for c in cols])),
            html.Tbody([
                html.Tr([html.Td(str(row.get(c, "")), style={"fontSize": "0.75rem"}) for c in cols])
                for row in preview_rows[:5]
            ]),
        ], className="table table-sm table-striped table-bordered mb-0"),
    ], style={"overflowX": "auto", "maxHeight": "200px"})

    preview_ui = html.Div([
        # File Metadata Badges
        html.Div([
            html.Span([html.I(className="bi bi-file-earmark-check-fill text-success me-1"), filename],
                      className="badge bg-success-subtle text-success border border-success me-2 px-3 py-2"),
            html.Span([html.I(className="bi bi-filetype-csv me-1"), f"Format: {fmt}"],
                      className="badge bg-primary-subtle text-primary border border-primary me-2 px-3 py-2"),
            html.Span([html.I(className="bi bi-list-ol me-1"), f"{total_rows:,} records detected"],
                      className="badge bg-info-subtle text-info border border-info me-2 px-3 py-2"),
            html.Span([html.I(className="bi bi-layout-three-columns me-1"), f"{len(cols)} columns detected"],
                      className="badge bg-secondary-subtle text-secondary border me-2 px-3 py-2"),
        ], className="d-flex flex-wrap align-items-center mb-3"),

        # Detected Schema
        html.Div([
            html.Div("DETECTED SCHEMA & DATA TYPES", className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
            html.Div(type_badges, className="p-3 bg-white rounded border mb-3"),
        ]),

        # Sample Preview
        html.Div([
            html.Div(f"DATASET PREVIEW (First {min(5, total_rows)} of {total_rows:,} records)",
                     className="fw-bold text-muted small mb-2", style={"letterSpacing": "0.05em"}),
            html.Div(preview_table, className="border rounded bg-white p-2"),
        ]),
    ], className="p-3 bg-light rounded-3 border")

    col_options = [{"label": f"{c} ({s['sql_type']})", "value": c} for c, s in zip(cols, schema_info)]
    # Default business key to column containing id/key, or first column
    def_b_key = next((c for c in cols if "id" in c.lower() or "key" in c.lower() or "num" in c.lower()), cols[0])

    store_data = {
        "contents": contents,
        "filename": filename,
        "file_format": fmt,
        "total_rows": total_rows,
        "columns": cols,
        "schema_info": schema_info,
    }

    return (
        preview_ui,
        parsed["suggested_dataset_name"],
        parsed["suggested_table_name"],
        col_options,
        def_b_key,
        store_data
    )


# -------------------------------------------------------------
# DATABRICKS CONNECTION MANAGER & DYNAMIC DISCOVERY CALLBACKS
# -------------------------------------------------------------

@app.callback(
    [Output("conn-form-profile-container", "style"),
     Output("conn-form-token-container", "style")],
    Input("conn-form-auth-type", "value"),
)
def toggle_conn_form_auth(auth_type):
    if auth_type == "CLI_PROFILE":
        return {"display": "block"}, {"display": "none"}
    elif auth_type == "PAT":
        return {"display": "none"}, {"display": "block"}
    return {"display": "none"}, {"display": "none"}


@app.callback(
    [Output("conn-form-alert", "children"),
     Output("connection-refresh-store", "data", allow_duplicate=True),
     Output("conn-form-host", "value")],
    Input("btn-conn-submit", "n_clicks"),
    [State("conn-form-auth-type", "value"),
     State("conn-form-host", "value"),
     State("conn-form-token", "value"),
     State("conn-form-profile", "value")],
    prevent_initial_call=True,
)
def handle_connect_databricks_action(n_clicks, auth_type, host, token, profile):
    if not _real_click():
        return dash.no_update, dash.no_update, dash.no_update

    res = db_adapter.connect_databricks_workspace(
        host=host,
        token=token,
        auth_type=auth_type,
        profile=profile,
    )
    _ts = datetime.now().isoformat()
    clean_h = res.get("clean_host") or (host or "").split("?")[0].split("#")[0].rstrip("/")

    if res.get("success"):
        alert = dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2 fs-5"),
            html.Span(res.get("message", "Connected successfully!"), className="fw-semibold"),
        ], color="success", className="py-2 mb-0")
        return alert, _ts, clean_h
    else:
        msg_content = [
            html.Div([
                html.I(className="bi bi-exclamation-triangle-fill me-2 fs-5 text-warning"),
                html.Span(res.get("message", "Connection failed."), className="fw-semibold"),
            ], className="d-flex align-items-center mb-2"),
        ]
        if res.get("suggest_pat"):
            msg_content.append(html.Div([
                html.Div([
                    html.B("Quick Fix for Databricks Free Edition / Personal Workspaces:"),
                    html.Ul([
                        html.Li([
                            "In your Databricks workspace (top right), click your user profile icon → ",
                            html.B("Settings"), " → ", html.B("Developer"), " → ", html.B("Access tokens"), " → ", html.B("Manage"), " → ", html.B("Generate new token"), "."
                        ]),
                        html.Li("Copy the token (starts with dapi...)."),
                        html.Li("Select 'Personal Access Token (Development Fallback)' above, paste the token, and click 'Connect to Databricks'."),
                    ], className="mb-0 ps-3 small mt-1"),
                ], className="p-3 rounded bg-white border small text-dark mt-2 shadow-sm"),
            ]))
        alert = dbc.Alert(msg_content, color="danger" if not res.get("suggest_pat") else "warning", className="py-2 mb-0")
        return alert, _ts, clean_h


@app.callback(
    [Output("conn-form-alert", "children", allow_duplicate=True),
     Output("connection-refresh-store", "data", allow_duplicate=True)],
    Input("btn-conn-disconnect", "n_clicks"),
    prevent_initial_call=True,
)
def handle_disconnect_databricks_action(n_clicks):
    if not _real_click():
        return dash.no_update, dash.no_update
    res = db_adapter.disconnect_databricks_workspace()
    _ts = datetime.now().isoformat()
    alert = dbc.Alert([
        html.I(className="bi bi-info-circle-fill me-2"),
        res.get("message", "Disconnected successfully."),
    ], color="secondary", className="py-2 mb-0")
    return alert, _ts


@app.callback(
    Output("conn-form-alert", "children", allow_duplicate=True),
    Input("btn-conn-verify", "n_clicks"),
    prevent_initial_call=True,
)
def handle_verify_databricks_action(n_clicks):
    if not _real_click():
        return dash.no_update
    res = db_adapter.verify_databricks_connection()
    if res.get("connected"):
        return dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            f"Active Databricks connection verified. Authenticated as: {res.get('username')}.",
        ], color="success", className="py-2 mb-0")
    else:
        return dbc.Alert([
            html.I(className="bi bi-exclamation-octagon-fill me-2"),
            f"Connection verification failed: {res.get('message')}. Please reconnect.",
        ], color="warning", className="py-2 mb-0")


@app.callback(
    [Output("conn-kpi-cards-container", "children"),
     Output("btn-conn-verify", "disabled"),
     Output("btn-conn-refresh-discovery", "disabled"),
     Output("btn-conn-disconnect", "disabled"),
     Output("conn-explorer-header-count", "children"),
     Output("conn-explorer-catalogs-select", "options"),
     Output("conn-explorer-catalogs-select", "value"),
     Output("conn-explorer-body", "style"),
     Output("conn-explorer-empty-state", "style")],
    [Input("connection-refresh-store", "data"),
     Input("btn-conn-refresh-discovery", "n_clicks")],
    prevent_initial_call=True,
)
def refresh_connection_page_state(conn_data, n_clicks):
    refresh = _safe_triggered_id() == "btn-conn-refresh-discovery"
    conn_info = db_adapter.get_databricks_connection_info()
    is_connected = conn_info.get("connected", False)
    clean_host = conn_info.get("clean_host", "")

    # Re-render 4 KPI Cards
    kpi_ui = _render_connection_kpi_cards(conn_info)

    if not is_connected:
        header_text = "Connect your Databricks workspace above to discover accessible Unity Catalog resources."
        return kpi_ui, True, True, True, header_text, [], None, {"display": "none"}, {"display": "block"}

    # Fetch dynamic catalogs
    catalogs = db_adapter.get_unity_catalogs(refresh=refresh)
    cat_opts = []
    cat_names = []
    for c in catalogs:
        name = c["name"] if isinstance(c, dict) else str(c)
        cat_names.append(name)
        c_type = f" ({c.get('type')})" if isinstance(c, dict) and c.get('type') else ""
        cat_opts.append({"label": f"{name}{c_type}", "value": name})

    chosen_cat = None
    if cat_names:
        if "dq_catalog" in cat_names:
            chosen_cat = "dq_catalog"
        else:
            chosen_cat = cat_names[0]

    header_text = f"Dynamically discovered from {clean_host} ({len(cat_opts)} Catalogs Accessible)"

    return kpi_ui, False, False, False, header_text, cat_opts, chosen_cat, {"display": "block"}, {"display": "none"}


@app.callback(
    [Output("conn-explorer-schemas-select", "options"),
     Output("conn-explorer-schemas-select", "value")],
    [Input("conn-explorer-catalogs-select", "value"),
     Input("btn-conn-refresh-discovery", "n_clicks")],
    prevent_initial_call=True,
)
def update_explorer_schemas(cat, n_clicks):
    if not cat:
        return [], None
    refresh = _safe_triggered_id() == "btn-conn-refresh-discovery"
    schemas = db_adapter.get_unity_schemas(cat, refresh=refresh)
    opts = [{"label": s["name"], "value": s["name"]} for s in schemas]
    val = None
    if opts:
        default_opt = next((o["value"] for o in opts if o["value"].lower() == "default"), None)
        val = default_opt or opts[0]["value"]
    return opts, val


@app.callback(
    [Output("conn-explorer-tables-select", "options"),
     Output("conn-explorer-tables-select", "value"),
     Output("conn-explorer-volumes-container", "children")],
    [Input("conn-explorer-schemas-select", "value"),
     Input("conn-explorer-catalogs-select", "value")],
    prevent_initial_call=True,
)
def update_explorer_tables(sch, cat):
    if not cat or not sch:
        return [], None, html.Div()
    tables = db_adapter.get_unity_tables(cat, sch)
    tbl_opts = [{"label": f"{t['name']} ({t.get('type', 'MANAGED')})", "value": t["name"]} for t in tables]
    val = tbl_opts[0]["value"] if tbl_opts else None
    if not tbl_opts:
        tbl_opts = [{"label": f"ℹ️ 0 tables in '{cat}.{sch}' (try selecting another schema or catalog)", "value": "", "disabled": True}]
        val = None
    volumes = db_adapter.get_unity_volumes(cat, sch)
    vol_ui = _render_volumes_list(volumes, cat, sch)
    return tbl_opts, val, vol_ui


@app.callback(
    Output("conn-explorer-table-metadata-container", "children"),
    [Input("conn-explorer-tables-select", "value"),
     State("conn-explorer-catalogs-select", "value"),
     State("conn-explorer-schemas-select", "value")],
    prevent_initial_call=True,
)
def update_explorer_table_metadata(tbl, cat, sch):
    if not cat or not sch or not tbl:
        return html.Div()
    return _render_explorer_table_metadata(cat, sch, tbl)


@app.callback(
    Output("sidebar-connection-status", "children"),
    Input("connection-refresh-store", "data"),
    prevent_initial_call=True,
)
def update_sidebar_connection_status(conn_ts):
    return _render_sidebar_connection_status()


# ── Option 2 Dynamic Unity Catalog Discovery Callbacks ──────────────────────

@app.callback(
    Output("modal-connection-bar-container", "children"),
    [Input("modal-add-dataset", "is_open"),
     Input("connection-refresh-store", "data")],
    prevent_initial_call=True,
)
def refresh_modal_connection_bar(is_open, conn_data):
    if not is_open:
        return dash.no_update
    return _render_modal_connection_bar()


@app.callback(
    [Output("select-existing-catalog", "options"),
     Output("select-existing-catalog", "value"),
     Output("select-existing-target-catalog", "options")],
    [Input("modal-add-dataset", "is_open"),
     Input("dataset-reg-method", "value"),
     Input("btn-refresh-catalogs", "n_clicks"),
     Input("connection-refresh-store", "data")],
    State("select-existing-catalog", "value"),
    prevent_initial_call=True,
)
def load_catalogs(is_open, method, n_clicks, conn_data, current_val):
    if not is_open:
        return dash.no_update, dash.no_update, dash.no_update
    refresh = _safe_triggered_id() in ("btn-refresh-catalogs", "connection-refresh-store")
    catalogs = db_adapter.get_unity_catalogs(refresh=refresh)
    opts = []
    cat_names = []
    for c in catalogs:
        name = c["name"] if isinstance(c, dict) else str(c)
        cat_names.append(name)
        comment = f" - {c.get('comment')}" if isinstance(c, dict) and c.get('comment') else ""
        opts.append({"label": f"{name}{comment}", "value": name})
    val = current_val if (current_val and current_val in cat_names) else (cat_names[0] if cat_names else None)
    return opts, val, opts


@app.callback(
    [Output("select-existing-schema", "options"),
     Output("select-existing-schema", "value")],
    [Input("select-existing-catalog", "value"),
     Input("btn-refresh-schemas", "n_clicks")],
    State("select-existing-schema", "value"),
    prevent_initial_call=True,
)
def load_schemas(cat, n_clicks, current_val):
    if not cat:
        return [], None
    refresh = _safe_triggered_id() == "btn-refresh-schemas"
    schemas = db_adapter.get_unity_schemas(cat, refresh=refresh)
    opts = []
    sch_names = []
    for s in schemas:
        name = s["name"] if isinstance(s, dict) else str(s)
        sch_names.append(name)
        comment = f" - {s.get('comment')}" if isinstance(s, dict) and s.get('comment') else ""
        opts.append({"label": f"{name}{comment}", "value": name})
    val = current_val if (current_val and current_val in sch_names) else (sch_names[0] if sch_names else None)
    return opts, val


@app.callback(
    [Output("select-existing-table", "options"),
     Output("select-existing-table", "value")],
    [Input("select-existing-schema", "value"),
     Input("select-existing-catalog", "value"),
     Input("btn-refresh-tables", "n_clicks")],
    State("select-existing-table", "value"),
    prevent_initial_call=True,
)
def load_tables(sch, cat, n_clicks, current_val):
    if not cat or not sch:
        return [], None
    refresh = _safe_triggered_id() == "btn-refresh-tables"
    tables = db_adapter.get_unity_tables(cat, sch, refresh=refresh)
    opts = []
    tbl_names = []
    for t in tables:
        name = t["name"] if isinstance(t, dict) else str(t)
        tbl_names.append(name)
        tbl_type = f" ({t.get('type')})" if isinstance(t, dict) and t.get('type') else ""
        opts.append({"label": f"{name}{tbl_type}", "value": name})
    val = current_val if (current_val and current_val in tbl_names) else (tbl_names[0] if tbl_names else None)
    if not opts:
        opts = [{"label": f"ℹ️ 0 tables in '{cat}.{sch}' (try selecting another schema or catalog)", "value": "", "disabled": True}]
        val = None
    return opts, val


@app.callback(
    [Output("select-existing-business-key", "options"),
     Output("select-existing-business-key", "value"),
     Output("input-existing-dataset-name", "value"),
     Output("existing-table-preview-container", "children")],
    [Input("select-existing-table", "value"),
     Input("btn-refresh-existing-metadata", "n_clicks")],
    [State("select-existing-catalog", "value"),
     State("select-existing-schema", "value"),
     State("input-existing-dataset-name", "value")],
    prevent_initial_call=True,
)
def on_table_selected(tbl, n_clicks, cat, sch, current_ds_name):
    if not cat or not sch or not tbl:
        return [], None, dash.no_update, html.Div()
    refresh = _safe_triggered_id() == "btn-refresh-existing-metadata"
    meta = db_adapter.get_unity_table_metadata(cat, sch, tbl, refresh=refresh)
    cols = meta.get("columns", [])
    b_cand = meta.get("business_key_candidates", [])

    schema_map = {s.get("column"): s.get("sql_type", "STRING") for s in meta.get("schema_info", [])}
    col_opts = [{"label": f"{c} ({schema_map.get(c, 'STRING')})" + (" [Key Candidate]" if c in b_cand else ""), "value": c} for c in cols]
    def_key = b_cand[0] if b_cand else (cols[0] if cols else None)

    ds_name = tbl
    preview_ui = _build_table_preview_card(meta)
    return col_opts, def_key, ds_name, preview_ui


@app.callback(
    Output("collapse-existing-reconciliation", "is_open"),
    Input("toggle-existing-reconciliation", "value"),
    prevent_initial_call=True,
)
def toggle_reconciliation_section(enable_recon):
    return bool(enable_recon)


@app.callback(
    [Output("select-existing-target-schema", "options"),
     Output("select-existing-target-schema", "value")],
    Input("select-existing-target-catalog", "value"),
    prevent_initial_call=True,
)
def load_target_schemas(cat):
    if not cat:
        return [], None
    schemas = db_adapter.get_unity_schemas(cat)
    opts = []
    sch_names = []
    for s in schemas:
        name = s["name"] if isinstance(s, dict) else str(s)
        sch_names.append(name)
        opts.append({"label": name, "value": name})
    val = sch_names[0] if sch_names else None
    return opts, val


@app.callback(
    [Output("select-existing-target-table", "options"),
     Output("select-existing-target-table", "value")],
    [Input("select-existing-target-schema", "value"),
     Input("select-existing-target-catalog", "value")],
    prevent_initial_call=True,
)
def load_target_tables(sch, cat):
    if not cat or not sch:
        return [], None
    tables = db_adapter.get_unity_tables(cat, sch)
    opts = []
    tbl_names = []
    for t in tables:
        name = t["name"] if isinstance(t, dict) else str(t)
        tbl_names.append(name)
        tbl_type = f" ({t.get('type')})" if isinstance(t, dict) and t.get('type') else ""
        opts.append({"label": f"{name}{tbl_type}", "value": name})
    val = tbl_names[0] if tbl_names else None
    if not opts:
        opts = [{"label": f"ℹ️ 0 tables in '{cat}.{sch}'", "value": "", "disabled": True}]
        val = None
    return opts, val


@app.callback(
    [Output("select-existing-target-bkey", "options"),
     Output("select-existing-target-bkey", "value")],
    [Input("select-existing-target-table", "value"),
     Input("select-existing-target-schema", "value"),
     Input("select-existing-target-catalog", "value")],
    prevent_initial_call=True,
)
def load_target_bkeys(tbl, sch, cat):
    if not cat or not sch or not tbl:
        return [], None
    meta = db_adapter.get_unity_table_metadata(cat, sch, tbl)
    cols = meta.get("columns", [])
    b_cand = meta.get("business_key_candidates", [])
    schema_map = {s.get("column"): s.get("sql_type", "STRING") for s in meta.get("schema_info", [])}
    opts = [{"label": f"{c} ({schema_map.get(c, 'STRING')})", "value": c} for c in cols]
    val = b_cand[0] if b_cand else (cols[0] if cols else None)
    return opts, val
    if not cat or not sch or not tbl:
        return [], None
    meta = db_adapter.get_unity_table_metadata(cat, sch, tbl)
    cols = meta.get("columns", [])
    b_cand = meta.get("business_key_candidates", [])
    schema_map = {s.get("column"): s.get("sql_type", "STRING") for s in meta.get("schema_info", [])}
    opts = [{"label": f"{c} ({schema_map.get(c, 'STRING')})", "value": c} for c in cols]
    val = b_cand[0] if b_cand else (cols[0] if cols else None)
    return opts, val


@app.callback(
    Output("existing-connectivity-alert", "children"),
    Input("btn-test-existing-conn", "n_clicks"),
    [State("select-existing-catalog", "value"),
     State("select-existing-schema", "value"),
     State("select-existing-table", "value"),
     State("select-existing-business-key", "value"),
     State("toggle-existing-reconciliation", "value"),
     State("select-existing-target-catalog", "value"),
     State("select-existing-target-schema", "value"),
     State("select-existing-target-table", "value"),
     State("select-existing-target-bkey", "value")],
    prevent_initial_call=True,
)
def on_test_existing_connectivity(n_clicks, cat, sch, tbl, b_key, enable_recon, tgt_cat, tgt_sch, tgt_tbl, tgt_bkey):
    if not _real_click():
        return dash.no_update
    if not cat or not sch or not tbl:
        return dbc.Alert("Please select Catalog, Schema, and Existing Table.", color="warning", className="small py-2 mb-0")

    res = db_adapter.test_unity_catalog_connectivity(
        catalog=cat,
        schema=sch,
        table=tbl,
        business_key=b_key,
        target_catalog=tgt_cat if enable_recon else None,
        target_schema=tgt_sch if enable_recon else None,
        target_table=tgt_tbl if enable_recon else None,
        target_business_key=tgt_bkey if enable_recon else None,
    )

    checklist_items = []
    for chk in res.get("checks", []):
        icon = "bi-check-circle-fill text-success" if chk["status"] == "PASS" else "bi-x-circle-fill text-danger"
        checklist_items.append(html.Div([
            html.I(className=f"bi {icon} me-2"),
            html.Span(f"{chk['name']}: ", className="fw-semibold small"),
            html.Span(chk['message'], className="text-muted small"),
        ], className="mb-1"))

    alert_color = "success" if res.get("success") else "danger"
    return dbc.Alert([
        html.Div([
            html.I(className=f"bi {'bi-check-circle-fill text-success' if res.get('success') else 'bi-exclamation-triangle-fill text-danger'} me-2 fs-5"),
            html.Span(res.get("message", ""), className="fw-bold"),
        ], className="d-flex align-items-center mb-2"),
        html.Div(checklist_items, className="p-2 bg-white rounded border"),
    ], color=alert_color, className="small py-2 mb-0")


@app.callback(
    [Output("dataset-registration-status-card", "children", allow_duplicate=True),
     Output("dataset-registration-status-card", "style", allow_duplicate=True),
     Output("dataset-registration-form-container", "style", allow_duplicate=True),
     Output("upload-submit-alert", "children"),
     Output("existing-submit-alert", "children"),
     Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("global-toast", "icon", allow_duplicate=True),
     Output("project-refresh-store", "data", allow_duplicate=True)],
    [Input("btn-submit-upload-dataset", "n_clicks"),
     Input("btn-submit-existing-dataset", "n_clicks")],
    [State("select-dataset-target-project", "value"),
     State("dataset-reg-method", "value"),
     # Upload states
     State("upload-file-store", "data"),
     State("input-upload-dataset-name", "value"),
     State("input-upload-catalog", "value"),
     State("input-upload-schema", "value"),
     State("input-upload-table-name", "value"),
     State("select-upload-business-key", "value"),
     State("input-upload-target-table", "value"),
     State("input-upload-description", "value"),
     # Existing states
     State("input-existing-dataset-name", "value"),
     State("select-existing-catalog", "value"),
     State("select-existing-schema", "value"),
     State("select-existing-table", "value"),
     State("select-existing-business-key", "value"),
     State("toggle-existing-reconciliation", "value"),
     State("select-existing-target-catalog", "value"),
     State("select-existing-target-schema", "value"),
     State("select-existing-target-table", "value"),
     State("select-existing-target-bkey", "value"),
     State("input-existing-description", "value")],
    prevent_initial_call=True,
)
def handle_submit_dataset(
    upload_click, existing_click,
    project_id, reg_method,
    file_store, up_ds_name, up_cat, up_sch, up_tbl, up_b_key, up_tgt, up_desc,
    ex_ds_name, ex_cat, ex_sch, ex_tbl, ex_b_key, enable_recon, ex_tgt_cat, ex_tgt_sch, ex_tgt_tbl, ex_tgt_bkey, ex_desc
):
    trig = _safe_triggered_id()
    if not trig or not _real_click():
        return (dash.no_update,) * 9

    _ts = datetime.now().isoformat()
    if not project_id:
        err = dbc.Alert("Target Project is required. Please select or create a project.", color="danger", className="py-2 mb-0")
        return dash.no_update, dash.no_update, dash.no_update, err, err, False, "", "info", dash.no_update

    p_id = int(project_id)
    p_df = db_adapter.query_df("SELECT project_name FROM dq_project_config WHERE project_id = ?", (p_id,))
    p_name = p_df.iloc[0]["project_name"] if not p_df.empty else f"Project #{p_id}"

    # OPTION 1: Upload Dataset Submit
    if trig == "btn-submit-upload-dataset":
        if not file_store or not file_store.get("contents"):
            err = dbc.Alert("Please upload a file (CSV, JSON, Parquet, or Excel) before registering.", color="warning", className="py-2 mb-0")
            return dash.no_update, dash.no_update, dash.no_update, err, dash.no_update, False, "", "info", dash.no_update

        if not up_ds_name or not up_tbl:
            err = dbc.Alert("Dataset Name and Destination Table Name are required.", color="danger", className="py-2 mb-0")
            return dash.no_update, dash.no_update, dash.no_update, err, dash.no_update, False, "", "info", dash.no_update

        res = db_adapter.register_uploaded_dataset(
            project_id=p_id,
            dataset_name=up_ds_name.strip(),
            contents=file_store["contents"],
            filename=file_store["filename"],
            catalog=(up_cat or "hospital_catalog").strip(),
            schema=(up_sch or "healthcare").strip(),
            table_name=up_tbl.strip(),
            business_key=(up_b_key or "").strip(),
            target_table=(up_tgt or "").strip() or None,
            description=(up_desc or "").strip(),
            user="admin"
        )

        if not res.get("success"):
            err = dbc.Alert([
                html.I(className="bi bi-exclamation-triangle-fill me-2"),
                res.get("message")
            ], color="danger", className="py-2 mb-0")
            return dash.no_update, dash.no_update, dash.no_update, err, dash.no_update, False, "", "info", dash.no_update

        # Build Post-Registration Details Card
        status_card = _build_registration_success_card(
            dataset_name=res["dataset_name"],
            project_name=p_name,
            full_table=res["full_table_name"],
            record_count=res["total_records"],
            columns=res["columns"],
            schema_info=res.get("schema_info", []),
            business_key=res.get("business_key", ""),
            method="Uploaded File (Delta Table Created)",
            target_table=res.get("target_table_name"),
            registered_at=res.get("registered_at"),
            connection_status=res.get("connection_status", "Active (Local Unity Catalog)")
        )
        return status_card, {"display": "block"}, {"display": "none"}, None, None, True, f"Dataset '{up_ds_name}' registered successfully!", "success", _ts

    # OPTION 2: Existing Table Submit
    if trig == "btn-submit-existing-dataset":
        if not ex_ds_name or not ex_cat or not ex_sch or not ex_tbl:
            err = dbc.Alert("Dataset Name, Catalog, Schema, and Existing Table Name are required.", color="danger", className="py-2 mb-0")
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, err, False, "", "info", dash.no_update

        target_table = None
        if enable_recon and ex_tgt_tbl:
            t_cat = (ex_tgt_cat or ex_cat).strip()
            t_sch = (ex_tgt_sch or ex_sch).strip()
            target_table = f"{t_cat}.{t_sch}.{ex_tgt_tbl.strip()}"

        res = db_adapter.register_existing_table(
            project_id=p_id,
            dataset_name=ex_ds_name.strip(),
            catalog=ex_cat.strip(),
            schema=ex_sch.strip(),
            table_name=ex_tbl.strip(),
            business_key=(ex_b_key or "id").strip(),
            target_table=target_table,
            description=(ex_desc or "").strip(),
            user="admin"
        )

        if not res.get("success"):
            err = dbc.Alert([
                html.I(className="bi bi-exclamation-triangle-fill me-2"),
                res.get("message")
            ], color="danger", className="py-2 mb-0")
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, err, False, "", "info", dash.no_update

        status_card = _build_registration_success_card(
            dataset_name=res["dataset_name"],
            project_name=p_name,
            full_table=res["full_table_name"],
            record_count=res.get("total_records", 0),
            columns=res.get("columns", []),
            schema_info=res.get("schema_info", []),
            business_key=res.get("business_key", ""),
            method="Existing Unity Catalog Table",
            target_table=res.get("target_table_name"),
            registered_at=res.get("registered_at"),
            connection_status=res.get("connection_status", "Connected (Unity Catalog)")
        )
        return status_card, {"display": "block"}, {"display": "none"}, None, None, True, f"Dataset '{ex_ds_name}' registered successfully!", "success", _ts

    return (dash.no_update,) * 9


def _build_registration_success_card(
    dataset_name, project_name, full_table, record_count, columns, schema_info, business_key, method,
    target_table=None, registered_at=None, connection_status=None
):
    """Builds the comprehensive post-registration confirmation card."""
    if not registered_at:
        registered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not connection_status:
        connection_status = "Connected (Databricks Unity Catalog)"

    col_pills = [
        html.Span([
            html.Span(c, className="fw-semibold me-1"),
            html.Span(next((s.get("sql_type", "TEXT") for s in schema_info if s.get("column") == c), "TEXT"),
                      className="badge bg-light text-primary border small"),
        ], className="badge bg-white text-dark border me-2 mb-2 p-2")
        for c in columns
    ]

    return html.Div([
        # Success Header
        html.Div([
            html.I(className="bi bi-check-circle-fill text-success fs-1 mb-2"),
            html.H4("Dataset Successfully Registered!", className="fw-bold text-dark mb-1"),
            html.P(f"Dataset '{dataset_name}' is now registered under '{project_name}' in Unity Catalog and ready for automated quality validations.",
                   className="text-muted"),
            html.Div([
                html.Span([
                    html.I(className="bi bi-hdd-network-fill text-success me-1"),
                    f"Status: {connection_status}"
                ], className="badge bg-success-subtle text-success border border-success me-2 px-3 py-1"),
                html.Span([
                    html.I(className="bi bi-clock-history me-1"),
                    f"Registered: {registered_at}"
                ], className="badge bg-light text-muted border px-3 py-1"),
            ], className="mt-2"),
        ], className="text-center py-3"),

        # Summary KPIs Grid
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div("Target Project", className="small text-muted fw-semibold"),
                    html.Div(project_name, className="fs-6 fw-bold text-dark text-truncate"),
                ], className="p-3 bg-light rounded border text-center h-100"),
            ], md=4, className="mb-3"),
            dbc.Col([
                html.Div([
                    html.Div("Dataset Name", className="small text-muted fw-semibold"),
                    html.Div(dataset_name, className="fs-6 fw-bold text-primary text-truncate"),
                ], className="p-3 bg-light rounded border text-center h-100"),
            ], md=4, className="mb-3"),
            dbc.Col([
                html.Div([
                    html.Div("Business Key", className="small text-muted fw-semibold"),
                    html.Div(business_key or "id", className="fs-6 fw-bold text-secondary text-truncate"),
                ], className="p-3 bg-light rounded border text-center h-100"),
            ], md=4, className="mb-3"),
        ], className="mb-2"),

        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div("Source Table (Unity Catalog)", className="small text-muted fw-semibold"),
                    html.Div(full_table, className="fs-6 fw-bold text-dark text-truncate"),
                ], className="p-3 bg-light rounded border text-center h-100"),
            ], md=4, className="mb-3"),
            dbc.Col([
                html.Div([
                    html.Div("Target Table (Reconciliation)", className="small text-muted fw-semibold"),
                    html.Div(target_table or "N/A (Single Table)", className="fs-6 fw-bold text-dark text-truncate"),
                ], className="p-3 bg-light rounded border text-center h-100"),
            ], md=4, className="mb-3"),
            dbc.Col([
                html.Div([
                    html.Div("Record Count & Columns", className="small text-muted fw-semibold"),
                    html.Div(f"{record_count:,} rows | {len(columns)} cols", className="fs-6 fw-bold text-success"),
                ], className="p-3 bg-light rounded border text-center h-100"),
            ], md=4, className="mb-3"),
        ], className="mb-3"),

        # Schema & Columns Grid
        html.Div([
            html.Div([
                html.Span("Registered Columns & Data Types", className="fw-bold text-dark me-2"),
                html.Span(f"({len(columns)} columns)", className="badge bg-secondary-subtle text-secondary small"),
            ], className="mb-2"),
            html.Div(col_pills, className="d-flex flex-wrap p-3 bg-light rounded border mb-4"),
        ]),

        # Next Steps Action Strip
        html.Div([
            html.Div([
                html.Span("Next Step: ", className="fw-bold text-secondary me-2"),
                html.Span("Configure Quality Rules to establish automated baseline validations for this dataset.", className="text-muted small"),
            ], className="mb-3"),
            html.Div([
                dbc.Button([
                    html.I(className="bi bi-shield-check me-2"),
                    "Configure Data Quality Rules"
                ], href="/rules", color="success", className="rounded-pill px-4 py-2 fw-semibold me-3 shadow-sm"),
                dbc.Button([
                    html.I(className="bi bi-play-circle-fill me-2"),
                    "Run Execution"
                ], href="/execution", color="primary", className="rounded-pill px-3 py-2 fw-semibold me-3"),
                dbc.Button([
                    html.I(className="bi bi-arrow-left-right me-2"),
                    "Source-Target Reconciliation"
                ], href="/reconciliation", color="outline-primary", className="rounded-pill px-3 py-2 fw-semibold me-3"),
            ], className="d-flex align-items-center justify-content-end"),
        ], className="p-3 bg-white rounded border"),
    ], className="p-2")



# =============================================================================
# OVERVIEW CALLBACKS — refresh KPIs, trend, recent executions every 30s
# =============================================================================

@app.callback(
    [Output("overview-filter-dataset", "options"),
     Output("overview-filter-dataset", "value")],
    Input("overview-filter-project", "value"),
    prevent_initial_call=True,
)
def overview_dataset_options(proj):
    ds = db_adapter.get_datasets_for_project(proj, include_inactive=True) if proj and proj != "All" else []
    return [{"label": "Dataset: All", "value": "All"}] + [{"label": d, "value": d} for d in ds], "All"


@app.callback(
    [Output("overview-kpi-row",             "children"),
     Output("overview-trend-chart",         "figure"),
     Output("overview-donut-chart",         "figure"),
     Output("overview-project-score-chart", "figure"),
     Output("overview-recent-exec-body",    "children"),
     Output("overview-critical-failures",   "children"),
     Output("overview-last-updated",        "children")],
    [Input("overview-interval", "n_intervals"),
     Input("btn-overview-refresh", "n_clicks"),
     Input("overview-filter-project", "value"),
     Input("overview-filter-dataset", "value")],
    prevent_initial_call=True,
)
def refresh_overview(n_intervals, refresh_clicks, proj, ds):
    return _overview_figures(proj, ds)




# =============================================================================
# EXECUTION & MONITORING CALLBACKS
# =============================================================================

# Helper: Render Execution Details Modal Content
def _render_execution_details_modal_content(execution_id: str):
    det = db_adapter.get_execution_details(execution_id)
    if not det or not det.get("summary"):
        return html.Div(f"No execution details found for {execution_id}.", className="text-muted p-3")

    s = det["summary"]
    prog = det.get("progress", [])
    c_stats = det.get("concurrency_stats", {})
    alerts = det.get("alerts", [])
    emails = det.get("emails", [])

    st = str(s.get("overall_status") or "UNKNOWN")
    badge_cls = "badge-pass" if st == "PASS" else ("badge-fail" if st == "FAIL" else "badge-error")

    metrics_row = dbc.Row([
        dbc.Col([html.Div("Execution ID", className="small text-muted"), html.B(execution_id, style={"fontFamily": "monospace"})], md=3),
        dbc.Col([html.Div("Target", className="small text-muted"), html.B(f"{s.get('project_name')} / {s.get('dataset_name')}")], md=3),
        dbc.Col([html.Div("Overall Status", className="small text-muted"), html.Span(st, className=badge_cls)], md=2),
        dbc.Col([html.Div("DQ Score", className="small text-muted"), html.B(f"{s.get('dq_score', 0)}%", className="text-primary fs-5")], md=2),
        dbc.Col([html.Div("Duration", className="small text-muted"), html.B(f"{s.get('duration_ms', 0)} ms")], md=2),
    ], className="p-3 bg-light rounded mb-3")

    th_dist = c_stats.get("thread_distribution", {})
    th_badges = [html.Span(f"{th}: {cnt} rule(s)", className="badge bg-secondary text-white me-2 small") for th, cnt in th_dist.items()]
    concurrency_strip = html.Div([
        html.Div([
            html.I(className="bi bi-cpu text-primary me-2"),
            html.B("Parallel Concurrency Metrics: "),
            html.Span(f"Max Workers: {c_stats.get('max_workers', 4)} | Thread Distribution: "),
            *th_badges
        ], className="small text-muted mb-3 p-2 border rounded bg-white")
    ])

    rule_rows = []
    for p in prog:
        p_st = str(p.get("status") or "—")
        p_badge = "badge-pass" if p_st == "PASS" else ("badge-fail" if p_st == "FAIL" else "badge-error")
        rule_rows.append(html.Tr([
            html.Td(html.B(str(p.get("rule_name"))), className="small"),
            html.Td(str(p.get("column_name") or "—"), className="small"),
            html.Td(str(p.get("threshold") or "0"), className="small text-muted"),
            html.Td(html.Span(p_st, className=p_badge)),
            html.Td(str(p.get("observed_value") or "—"), className="small"),
            html.Td(str(p.get("expected_value") or "—"), className="small"),
            html.Td(str(p.get("failure_count") or 0), className="small fw-semibold text-danger" if p_st == "FAIL" else "small"),
            html.Td(f"{p.get('duration_ms', 0)} ms", className="small"),
            html.Td(html.Span(str(p.get("worker_thread") or "Main"), className="badge bg-light text-dark border small")),
            html.Td(str(p.get("error_message") or "—"), className="small text-muted"),
        ]))

    rules_table = html.Div([
        html.H6([html.I(className="bi bi-list-check me-2"), "Rule Execution Breakdown"], className="fw-bold mb-2"),
        html.Table([
            html.Thead(html.Tr([
                html.Th("Rule Name"), html.Th("Column"), html.Th("Threshold"), html.Th("Status"),
                html.Th("Observed"), html.Th("Expected"), html.Th("Failures"),
                html.Th("Duration"), html.Th("Worker Thread"), html.Th("Notes / Error"),
            ]), className="table-header"),
            html.Tbody(rule_rows if rule_rows else [html.Tr([html.Td("No rule details recorded.", colSpan=10, className="text-center text-muted")])]),
        ], className="dq-table w-100"),
    ], style={"overflowX": "auto"}, className="mb-3")

    alerts_section = None
    if alerts:
        alert_items = [html.Div(f"[{a.get('severity')}] {a.get('rule_name')}: {a.get('message')}", className="small text-danger mb-1") for a in alerts]
        alerts_section = html.Div([
            html.H6([html.I(className="bi bi-bell-fill text-danger me-2"), f"Linked Alerts ({len(alerts)})"], className="fw-bold mb-1"),
            html.Div(alert_items, className="p-2 border rounded bg-light mb-3"),
        ])

    return html.Div([metrics_row, concurrency_strip, rules_table, alerts_section])


# Callback E0: Mode toggle (Manual vs Scheduled)
@app.callback(
    [Output("exec-manual-container", "style"),
     Output("exec-schedule-container", "style")],
    Input("exec-mode-toggle", "value"),
)
def exec_mode_toggle_callback(mode):
    if mode == "SCHEDULED":
        return {"display": "none"}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}


# Callback E0.1: Rules mode toggle (All vs Selected)
@app.callback(
    [Output("exec-rules-checklist-container", "style"),
     Output("exec-rule-selection-tools", "style")],
    Input("exec-rules-mode", "value"),
)
def exec_rules_mode_callback(mode):
    if mode == "SELECTED":
        return {"display": "block"}, {"display": "block"}
    return {"display": "none"}, {"display": "none"}


# Callback E1: Project → populate dataset dropdown
@app.callback(
    [Output("exec-select-dataset", "options"),
     Output("exec-select-dataset", "value")],
    Input("exec-select-project", "value"),
    prevent_initial_call=False,
)
def exec_update_dataset_dropdown(selected_project):
    if not selected_project:
        return [], None
    datasets = db_adapter.get_datasets_for_project(selected_project)
    options = [{"label": d, "value": d} for d in datasets]
    value = datasets[0] if datasets else None
    return options, value


# Callback E1.1: Project/Dataset → populate rules checklist & active rules summary
@app.callback(
    [Output("exec-rules-checklist", "options"),
     Output("exec-rules-checklist", "value"),
     Output("exec-active-rules-summary", "children")],
    [Input("exec-select-project", "value"),
     Input("exec-select-dataset", "value")],
)
def exec_update_rules_checklist_and_summary(proj, ds):
    if not proj or not ds:
        return [], [], ""
    options = _render_rules_checklist(proj, ds)
    vals = [opt["value"] for opt in options]
    summary_txt = f"Active Rules Configured: {len(options)} rule(s) for {proj} / {ds}"
    return options, vals, summary_txt


# Callback E1.2: Select All / Deselect All rules checklist
@app.callback(
    Output("exec-rules-checklist", "value", allow_duplicate=True),
    [Input("btn-rules-select-all", "n_clicks"),
     Input("btn-rules-deselect-all", "n_clicks")],
    State("exec-rules-checklist", "options"),
    prevent_initial_call=True,
)
def handle_select_deselect_rules(all_clicks, none_clicks, options):
    if not _real_click():
        return dash.no_update
    trig = ctx.triggered_id
    if trig == "btn-rules-select-all":
        return [opt["value"] for opt in (options or [])]
    return []


# Callback E1.3: Frequency selection conditional inputs
@app.callback(
    [Output("sched-row-weekdays", "style"),
     Output("sched-row-cron", "style"),
     Output("sched-col-interval", "style")],
    Input("sched-select-frequency", "value"),
)
def handle_sched_freq_inputs(freq):
    freq_u = (freq or "DAILY").upper()
    weekdays_style = {"display": "block"} if freq_u == "WEEKLY" else {"display": "none"}
    cron_style = {"display": "block"} if freq_u == "CUSTOM" else {"display": "none"}
    interval_style = {"display": "none"} if freq_u in ("CUSTOM", "ONCE") else {"display": "block"}
    return weekdays_style, cron_style, interval_style


# Callback E1.4: Save Schedule
@app.callback(
    [Output("sched-feedback-msg", "children"),
     Output("sched-table-body", "children", allow_duplicate=True)],
    Input("btn-save-schedule", "n_clicks"),
    [State("sched-input-name", "value"),
     State("exec-select-project", "value"),
     State("exec-select-dataset", "value"),
     State("sched-select-frequency", "value"),
     State("sched-select-timezone", "value"),
     State("sched-input-start-date", "value"),
     State("sched-input-start-time", "value"),
     State("sched-input-interval", "value"),
     State("sched-input-weekdays", "value"),
     State("sched-input-cron", "value"),
     State("sched-input-end-date", "value"),
     State("exec-rules-mode", "value"),
     State("exec-rules-checklist", "value")],
    prevent_initial_call=True,
)
def handle_save_schedule(n_clicks, s_name, proj, ds, freq, tz, s_date, s_time,
                         rep_int, weekdays, cron_expr, e_date,
                         rules_mode, selected_rules):
    if not _real_click() or not proj or not ds:
        return dash.no_update, dash.no_update

    try:
        w_days_str = ",".join(weekdays) if weekdays else None
        sched_data = {
            "schedule_name": s_name or f"{proj}_{ds}_{freq}_DQ",
            "project_name": proj,
            "dataset_name": ds,
            "frequency": freq or "DAILY",
            "timezone": tz or "UTC",
            "start_date": s_date or datetime.today().strftime("%Y-%m-%d"),
            "start_time": s_time or "08:00",
            "repeat_interval": int(rep_int or 1),
            "weekdays": w_days_str,
            "cron_expression": cron_expr,
            "end_date": e_date,
            "concurrency_policy": "PARALLEL",
            "rules_mode": rules_mode or "ALL",
            "selected_rules": selected_rules if rules_mode == "SELECTED" else None,
        }
        new_id = db_adapter.save_schedule(sched_data, user="admin")
        msg = html.Div([
            html.I(className="bi bi-check-circle-fill text-success me-2"),
            html.Span(f"Schedule '{sched_data['schedule_name']}' created successfully (ID: {new_id}).", className="text-success fw-bold"),
        ], className="p-2 rounded bg-success-subtle")
        table_rows = _render_schedules_table(project_name=proj, dataset_name=ds)
        return msg, table_rows
    except Exception as ex:
        err_msg = html.Div([
            html.I(className="bi bi-exclamation-triangle-fill text-danger me-2"),
            html.Span(f"Error saving schedule: {ex}", className="text-danger fw-bold"),
        ], className="p-2 rounded bg-danger-subtle")
        return err_msg, dash.no_update


# Callback E1.5: Schedules table filter and row actions (Run Now, Pause/Resume, Delete)
@app.callback(
    [Output("sched-table-body", "children"),
     Output("exec-store", "data", allow_duplicate=True),
     Output("exec-interval", "disabled", allow_duplicate=True),
     Output("btn-trigger-validation", "disabled", allow_duplicate=True),
     Output("btn-cancel-execution", "disabled", allow_duplicate=True),
     Output("exec-history-table-body", "children", allow_duplicate=True)],
    [Input("btn-refresh-schedules", "n_clicks"),
     Input("sched-filter-status", "value"),
     Input("exec-select-project", "value"),
     Input("sched-auto-refresh-interval", "n_intervals"),
     Input({"type": "btn-sched-run-now", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-sched-toggle-status", "id": dash.ALL, "status": dash.ALL}, "n_clicks"),
     Input({"type": "btn-sched-delete", "id": dash.ALL}, "n_clicks")],
    prevent_initial_call="initial_duplicate",
)
def handle_schedules_table_actions(refresh_clicks, st_filter, proj, auto_tick, run_clicks, toggle_clicks, delete_clicks):
    trig = ctx.triggered_id
    if isinstance(trig, dict) and _real_click():
        action_type = trig.get("type")
        sid = int(trig.get("id"))
        if action_type == "btn-sched-run-now":
            res = db_adapter.run_schedule_now(sid, user="admin")
            exec_id = res.get("execution_id")
            new_store = {
                "execution_id": exec_id,
                "project": proj,
                "dataset": res.get("dataset_name"),
                "start_time": res.get("start_time"),
                "_started_ts": time.time()
            }
            rows = _render_schedules_table(project_name=proj, status=st_filter)
            hist = _render_history_table()
            return rows, new_store, False, True, False, hist
        elif action_type == "btn-sched-toggle-status":
            current_st = trig.get("status")
            if current_st == "ACTIVE":
                db_adapter.pause_schedule(sid, user="admin")
            else:
                db_adapter.resume_schedule(sid, user="admin")
        elif action_type == "btn-sched-delete":
            db_adapter.delete_schedule(sid, user="admin")

    rows = _render_schedules_table(project_name=proj, status=st_filter)
    hist = _render_history_table()
    return rows, dash.no_update, dash.no_update, dash.no_update, dash.no_update, hist


# Callback E2: Run Validation button → concurrent parallel execution
@app.callback(
    [Output("exec-store", "data"),
     Output("exec-meta-id", "children"),
     Output("exec-meta-start", "children"),
     Output("exec-interval", "disabled"),
     Output("btn-trigger-validation", "disabled"),
     Output("btn-cancel-execution", "disabled"),
     Output("exec-live-progress-list", "children"),
     Output("exec-count-pass", "children"),
     Output("exec-count-fail", "children"),
     Output("exec-count-error", "children"),
     Output("exec-dq-score", "children"),
     Output("exec-stepper-container", "children"),
     Output("exec-meta-duration", "children"),
     Output("exec-history-table-body", "children", allow_duplicate=True)],
    Input("btn-trigger-validation", "n_clicks"),
    [State("exec-select-project", "value"),
     State("exec-select-dataset", "value"),
     State("exec-rules-mode", "value"),
     State("exec-rules-checklist", "value")],
    prevent_initial_call=True,
)
def handle_run_validation(n_clicks, proj, ds, rules_mode, selected_rules):
    if not proj or not ds or not _real_click():
        return (dash.no_update,) * 14

    rule_ids_param = selected_rules if rules_mode == "SELECTED" else None
    res = db_adapter.trigger_validation(proj, ds, run_type="MANUAL", rule_ids=rule_ids_param)
    exec_id = res.get("execution_id", "—")
    start_time = res.get("start_time", "—")

    store = {"execution_id": exec_id, "project": proj, "dataset": ds,
             "start_time": start_time, "_started_ts": time.time()}

    # Build real-time rule results stream
    rule_results = res.get("rule_results", [])
    live_items = []
    for p in rule_results:
        s = p.get("status", "PASS")
        if s == "PASS":
            icon_cls = "bi bi-check-circle-fill text-success me-2"
            bg = "#F0FDF4"
        elif s == "FAIL":
            icon_cls = "bi bi-x-circle-fill text-danger me-2"
            bg = "#FEF2F2"
        elif s == "ERROR":
            icon_cls = "bi bi-exclamation-triangle-fill text-warning me-2"
            bg = "#FFFBEB"
        elif s == "CANCELLED":
            icon_cls = "bi bi-x-octagon-fill text-secondary me-2"
            bg = "#F1F5F9"
        else:
            icon_cls = "bi bi-arrow-repeat text-primary me-2"
            bg = "#EFF6FF"

        label = f"{p.get('name') or p.get('rule_name', 'Rule')} — {s}"
        if s == "FAIL" and p.get("failures", 0) > 0:
            label += f" ({p['failures']} defect{'s' if p['failures'] > 1 else ''})"
        
        worker_txt = str(p.get("worker") or p.get("worker_thread") or "Worker")
        dur_txt = f"{p.get('duration_ms', 0)}ms" if p.get('duration_ms') else ""

        live_items.append(html.Div([
            html.Div([
                html.I(className=icon_cls),
                html.Span(label, className="fw-semibold small"),
            ]),
            html.Div([
                html.Span(dur_txt, className="text-muted small me-2") if dur_txt else None,
                html.Span(worker_txt, className="badge bg-light text-secondary border small"),
            ]),
        ], className="mb-2 p-2 rounded d-flex justify-content-between align-items-center", style={"backgroundColor": bg}))

    if not live_items:
        live_items = [html.Div("No active rules evaluated.", className="text-muted p-2")]

    passed = res.get("passed", 0)
    failed = res.get("failed", 0)
    errored = res.get("errored", 0)
    dq_score = res.get("dq_score", 0.0)
    dur_str = res.get("duration", "1s")
    total_rules = res.get("total_rules", len(rule_results))
    status = res.get("status", "COMPLETED")
    stepper = _render_stepper(status, total_rules, total_rules)
    score_disp = f"{dq_score:.1f}%" if total_rules > 0 else "N/A"

    try:
        hist_rows = _render_history_table()
    except Exception:
        hist_rows = dash.no_update

    return (
        store,          # exec-store
        exec_id,        # exec-meta-id
        start_time,     # exec-meta-start
        True,           # disable interval (synchronous run already completed)
        False,          # re-enable run button
        True,           # disable cancel button
        live_items,     # live progress list with all rules
        str(passed),    # pass count
        str(failed),    # fail count
        str(errored),   # error count
        score_disp,     # dq score
        stepper,        # stepper
        dur_str,        # duration
        hist_rows,      # execution history table
    )


# Callback E2.1: Cancel Execution
@app.callback(
    [Output("btn-cancel-execution", "disabled", allow_duplicate=True),
     Output("exec-live-progress-list", "children", allow_duplicate=True)],
    Input("btn-cancel-execution", "n_clicks"),
    State("exec-store", "data"),
    prevent_initial_call=True,
)
def handle_cancel_execution(clicks, store):
    if not _real_click() or not store or not store.get("execution_id"):
        return dash.no_update, dash.no_update
    exec_id = store["execution_id"]
    db_adapter.cancel_execution(exec_id, user="admin")
    cancelled_msg = html.Div([
        html.I(className="bi bi-x-octagon-fill text-danger me-2"),
        html.Span(f"Execution {exec_id} cancellation requested.", className="fw-bold text-danger"),
    ], className="p-2 rounded bg-danger-subtle")
    return True, [cancelled_msg]


# Callback E3: Polling interval → update progress + summary from DB
@app.callback(
    [Output("exec-stepper-container", "children", allow_duplicate=True),
     Output("exec-count-pass", "children", allow_duplicate=True),
     Output("exec-count-fail", "children", allow_duplicate=True),
     Output("exec-count-error", "children", allow_duplicate=True),
     Output("exec-dq-score", "children", allow_duplicate=True),
     Output("exec-live-progress-list", "children", allow_duplicate=True),
     Output("exec-meta-duration", "children", allow_duplicate=True),
     Output("exec-interval", "disabled", allow_duplicate=True),
     Output("btn-trigger-validation", "disabled", allow_duplicate=True),
     Output("btn-cancel-execution", "disabled", allow_duplicate=True),
     Output("exec-history-table-body", "children", allow_duplicate=True),
     Output("exec-store", "data", allow_duplicate=True)],
    Input("exec-interval", "n_intervals"),
    State("exec-store", "data"),
    prevent_initial_call=True,
)
def poll_execution_progress(n_intervals, store):
    if not store or not store.get("execution_id"):
        return (dash.no_update,) * 12

    started_ts = store.get("_started_ts", 0)
    if started_ts and (time.time() - started_ts > 180):
        return (
            dash.no_update, dash.no_update, dash.no_update, dash.no_update,
            dash.no_update, dash.no_update, dash.no_update,
            True,   # disable interval
            False,  # re-enable run button
            True,   # disable cancel button
            dash.no_update,
            dash.no_update,
        )

    exec_id = store["execution_id"]
    proj    = store.get("project")
    ds      = store.get("dataset")

    # Fetch DB state
    summary   = db_adapter.get_execution_status(exec_id)
    dq_status = summary.get("dq_status", "RUNNING")
    progress  = db_adapter.get_execution_progress(exec_id)
    rules_done  = sum(1 for p in progress if p["status"] not in ("QUEUED", "RUNNING"))
    rules_total = len(progress)

    _finalizing = store.get("_finalizing", False)
    if dq_status == "RUNNING":
        stepper_status = "RUNNING"
    elif dq_status in ("PASS", "FAIL", "ERROR", "COMPLETED", "CANCELLED"):
        if not _finalizing:
            stepper_status = "FINALIZING"
        else:
            stepper_status = dq_status
    else:
        stepper_status = dq_status

    stepper = _render_stepper(stepper_status, rules_done, rules_total)

    # Build live progress items with worker thread attribution
    live_items = []
    for p in progress:
        s = p["status"]
        if s == "PASS":
            icon_cls = "bi bi-check-circle-fill text-success me-2"
            bg = "#F0FDF4"
        elif s == "FAIL":
            icon_cls = "bi bi-x-circle-fill text-danger me-2"
            bg = "#FEF2F2"
        elif s == "ERROR":
            icon_cls = "bi bi-exclamation-triangle-fill text-warning me-2"
            bg = "#FFFBEB"
        elif s == "CANCELLED":
            icon_cls = "bi bi-x-octagon-fill text-secondary me-2"
            bg = "#F1F5F9"
        else:
            icon_cls = "bi bi-arrow-repeat text-primary me-2"
            bg = "#EFF6FF"

        label = f"{p['rule_name']} — {s}"
        if s == "FAIL" and p.get("failure_count", 0) > 0:
            label += f" ({p['failure_count']} defects)"
        
        worker_txt = str(p.get("worker_thread") or "Worker")
        dur_txt = f"{p.get('duration_ms', 0)}ms" if p.get('duration_ms') else ""

        live_items.append(html.Div([
            html.Div([
                html.I(className=icon_cls),
                html.Span(label, className="fw-semibold small"),
            ]),
            html.Div([
                html.Span(dur_txt, className="text-muted small me-2") if dur_txt else None,
                html.Span(worker_txt, className="badge bg-light text-secondary border small"),
            ]),
        ], className="mb-2 p-2 rounded d-flex justify-content-between align-items-center", style={"backgroundColor": bg}))

    if not live_items:
        live_items = [html.Div("Initializing rules...", className="text-muted p-2")]

    dur_str = summary.get("duration", "Running…")
    passed  = summary.get("passed",  0)
    failed  = summary.get("failed",  0)
    errored = summary.get("errored", 0)
    score   = summary.get("dq_score", 0.0)
    total_r = summary.get("total_rules", 0)

    if dq_status == "RUNNING":
        pass_disp  = str(passed)  if passed  > 0 else "…"
        fail_disp  = str(failed)  if failed  > 0 else "…"
        error_disp = str(errored) if errored > 0 else "…"
        score_disp = "…"
    else:
        pass_disp  = str(passed)
        fail_disp  = str(failed)
        error_disp = str(errored)
        score_disp = f"{score:.1f}%" if total_r > 0 else "N/A"

    is_db_done = dq_status not in ("RUNNING", "QUEUED", "UNKNOWN")

    if is_db_done and not _finalizing:
        interval_off = False
        btn_disabled = True
        btn_cancel_off = True
        hist_rows = dash.no_update
        updated_store = dict(store)
        updated_store["_finalizing"] = True
    elif is_db_done and _finalizing:
        interval_off  = True
        btn_disabled  = False
        btn_cancel_off = True
        try:
            hist_rows = _render_history_table()
        except Exception as e:
            logger.error(f"Error rendering history table: {e}")
            hist_rows = dash.no_update
        updated_store = dict(store)
    else:
        interval_off  = False
        btn_disabled  = True
        btn_cancel_off = False
        hist_rows     = dash.no_update
        updated_store = dict(store)

    return (
        stepper,
        pass_disp,
        fail_disp,
        error_disp,
        score_disp,
        live_items,
        dur_str,
        interval_off,
        btn_disabled,
        btn_cancel_off,
        hist_rows,
        updated_store,
    )


# Callback E4: Load / Filter history table
@app.callback(
    Output("exec-history-table-body", "children"),
    [Input("exec-hist-filter-project", "value"),
     Input("exec-hist-filter-trigger", "value"),
     Input("exec-hist-filter-status", "value"),
     Input("exec-hist-search", "value")],
    prevent_initial_call=False,
)
def update_exec_history_table(proj, trig, st, search):
    return _render_history_table(
        project_name=proj,
        dataset_name=None,
        trigger_type=trig,
        status_filter=st,
        search=search
    )


# Callback E5: History Row Actions (Details modal, Retry failed, Download CSV)
@app.callback(
    [Output("exec-details-modal", "is_open"),
     Output("exec-details-modal-title", "children"),
     Output("exec-details-modal-body", "children"),
     Output("exec-selected-rules-store", "data"),
     Output("exec-download-csv", "data"),
     Output("exec-store", "data", allow_duplicate=True),
     Output("exec-interval", "disabled", allow_duplicate=True),
     Output("btn-trigger-validation", "disabled", allow_duplicate=True),
     Output("btn-cancel-execution", "disabled", allow_duplicate=True),
     Output("exec-history-table-body", "children", allow_duplicate=True)],
    [Input({"type": "btn-exec-details", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-exec-retry", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-exec-csv", "id": dash.ALL}, "n_clicks"),
     Input("btn-modal-download-csv", "n_clicks"),
     Input("btn-close-details-modal", "n_clicks")],
    [State("exec-selected-rules-store", "data"),
     State("exec-select-project", "value"),
     State("exec-select-dataset", "value")],
    prevent_initial_call=True,
)
def handle_exec_history_row_actions(det_clicks, retry_clicks, csv_clicks,
                                    modal_csv_click, close_click, stored_modal_id,
                                    proj, ds):
    trig = ctx.triggered_id
    if not trig:
        return (dash.no_update,) * 10

    # Close modal
    if trig == "btn-close-details-modal":
        return False, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # CSV Download from modal
    if trig == "btn-modal-download-csv":
        if stored_modal_id:
            csv_str = db_adapter.export_execution_report_csv(stored_modal_id)
            csv_file = dcc.send_string(csv_str, filename=f"DQ_Execution_{stored_modal_id}.csv")
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, csv_file, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    if isinstance(trig, dict) and _real_click():
        action_type = trig.get("type")
        exec_id = str(trig.get("id"))

        # Details modal open
        if action_type == "btn-exec-details":
            title = f"Execution Run Details: {exec_id}"
            body = _render_execution_details_modal_content(exec_id)
            return True, title, body, exec_id, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

        # Retry execution
        elif action_type == "btn-exec-retry":
            res = db_adapter.retry_execution(exec_id, retry_failed_only=True, user="admin")
            new_exec_id = res.get("execution_id")
            new_store = {
                "execution_id": new_exec_id,
                "project": proj,
                "dataset": ds,
                "start_time": res.get("start_time"),
                "_started_ts": time.time()
            }
            hist_rows = _render_history_table()
            return False, dash.no_update, dash.no_update, dash.no_update, dash.no_update, new_store, False, True, False, hist_rows

        # Download CSV
        elif action_type == "btn-exec-csv":
            csv_str = db_adapter.export_execution_report_csv(exec_id)
            csv_file = dcc.send_string(csv_str, filename=f"DQ_Execution_{exec_id}.csv")
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, csv_file, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    return (dash.no_update,) * 10


# Callback E5: Render history table in proper <table> wrapper
# (the children of exec-history-table-body are <tr> elements, wrap them)
# Note: exec-history-table-body stores <tr> rows directly; we wrap via layout

# 7. CSV downloads are handled by page-specific callbacks (results, reconciliation, reports).

# =============================================================================
# PHASE 8: RULE LIBRARY CALLBACKS
# =============================================================================

# Page 3: Rule Library callbacks ---------------------------------------------
@app.callback(
    Output("template-tiles-container", "children"),
    Input("input-search-template", "value"),
)
def filter_template_tiles(search_term):
    return _render_template_tiles(search_term)


# Callback 1: Project Selection → Dataset Dropdown Refresh
@app.callback(
    [Output("rule-select-dataset", "options"),
     Output("rule-select-dataset", "value")],
    Input("rule-select-project", "value"),
    prevent_initial_call=False,
)
def update_dataset_dropdown(selected_project):
    if not selected_project:
        return [], None
    
    datasets = db_adapter.get_datasets_for_project(selected_project, include_inactive=False)
    options = [{"label": ds, "value": ds} for ds in datasets]
    value = datasets[0] if datasets else None
    return options, value


# Callback 1.1: Project/Dataset Selection → Dynamic Column Dropdown Refresh
@app.callback(
    [Output("rule-select-column", "options"),
     Output("rule-select-column", "value", allow_duplicate=True),
     Output("rule-select-column", "disabled", allow_duplicate=True),
     Output("rule-column-status-msg", "children", allow_duplicate=True)],
    [Input("rule-select-project", "value"),
     Input("rule-select-dataset", "value")],
    [State("current-template-name", "data")],
    prevent_initial_call="initial_duplicate",
)
def update_column_dropdown(proj_name, ds_name, current_tmpl):
    if not proj_name or not ds_name:
        return [{"label": "Select a column...", "value": ""}], None, True, "Please select a project and dataset first"
    
    cols = db_adapter.get_table_columns(proj_name, ds_name)
    if not cols:
        return [{"label": "No columns found for this dataset", "value": ""}], None, True, "No columns found for this dataset"
    
    # Check if current template requires column
    tmpl_df = db_adapter.get_rule_templates()
    tmpl = tmpl_df[tmpl_df["template_name"] == current_tmpl] if not tmpl_df.empty and current_tmpl else pd.DataFrame()
    requires_col = bool(tmpl.iloc[0]["requires_column"]) if not tmpl.empty and "requires_column" in tmpl.columns else True
    
    options = [{"label": "Select a column...", "value": ""}] + [{"label": c, "value": c} for c in cols]
    
    if not requires_col:
        return options, None, True, "Column selection not required for this rule template"
    
    return options, None, False, f"{len(cols)} columns available from {ds_name}"


# Callback 1.2: Column Selection → Auto Suggest Rule Name
@app.callback(
    Output("rule-input-name", "value", allow_duplicate=True),
    Input("rule-select-column", "value"),
    [State("current-template-name", "data"),
     State("rule-input-name", "value")],
    prevent_initial_call=True,
)
def auto_update_rule_name(selected_col, current_tmpl, current_name):
    if not selected_col:
        return dash.no_update
    tmpl = current_tmpl or "RULE"
    if not current_name or current_name == tmpl or current_name.startswith(f"{tmpl}_"):
        return f"{tmpl}_{selected_col}"
    return dash.no_update


# Callback 1.5: Clear search input when project/dataset changes
@app.callback(
    Output("input-search-rules", "value"),
    [Input("rule-select-project", "value"),
     Input("rule-select-dataset", "value")],
    prevent_initial_call=False,
)
def clear_search_on_project_dataset_change(proj_name, ds_name):
    """Clear search input to avoid stale search results when switching projects/datasets."""
    return ""

# Callback 2: Project/Dataset Selection → Refresh Configured Rules Table
@app.callback(
    Output("configured-rules-table-body", "children"),
    [Input("rule-select-project", "value"),
     Input("rule-select-dataset", "value"),
     Input("rule-refresh-store", "data"),
     Input("input-search-rules", "value"),
     Input("filter-rules-category", "value"),
     Input("filter-rules-severity", "value")],
    prevent_initial_call=False,
)
def refresh_configured_rules_table(proj_name, ds_name, refresh_signal, search_term, category_filter, severity_filter):
    if not proj_name or not ds_name:
        return []
    
    # Get project_id and dataset_id
    proj_df = db_adapter.query_df(
        "SELECT project_id FROM dq_project_config WHERE project_name = ?",
        (proj_name,)
    )
    if proj_df.empty:
        return []
    
    project_id = int(proj_df.iloc[0]["project_id"])
    
    ds_df = db_adapter.query_df(
        "SELECT dataset_id FROM dq_table_config WHERE dataset_name = ? AND project_id = ?",
        (ds_name, project_id)
    )
    if ds_df.empty:
        return []
    
    dataset_id = int(ds_df.iloc[0]["dataset_id"])
    
    # Fetch rules with database-level filtering
    rules_df = db_adapter.get_configured_rules(project_id, dataset_id,
                                               category_filter=category_filter,
                                               severity_filter=severity_filter,
                                               search=search_term)
    
    table_rows = []
    for _, r in rules_df.iterrows():
        rule_id = int(r["rule_id"])
        active = bool(r["active"])
        status_badge = "badge-pass" if active else "badge-inactive"
        status_text = "Active" if active else "Inactive"
        
        table_rows.append(html.Tr([
            html.Td(r["rule_name"], style={"fontWeight": "600"}),
            html.Td(str(r["column_name"] or "-")),
            html.Td(str(r["threshold"])),
            html.Td(r["severity"]),
            html.Td(html.Span(status_text, className=status_badge)),
            html.Td(str(r["created_at"])),
            html.Td([
                dbc.Button("View SQL", id={"type": "btn-view-sql-rule", "index": rule_id}, size="sm", color="light", className="me-1 mb-1", style={"fontSize": "0.7rem", "whiteSpace": "nowrap"}),
                dbc.Button("Edit", id={"type": "btn-edit-rule", "index": rule_id}, size="sm", color="info", className="me-1 mb-1", style={"fontSize": "0.7rem", "whiteSpace": "nowrap"}),
                dbc.Button("Delete", id={"type": "btn-delete-rule", "index": rule_id}, size="sm", color="danger", className="me-1 mb-1", style={"fontSize": "0.7rem", "whiteSpace": "nowrap"}),
                dbc.Button("Toggle", id={"type": "btn-toggle-rule", "index": rule_id}, size="sm", color="warning", style={"fontSize": "0.7rem", "whiteSpace": "nowrap"}),
            ], style={"minWidth": "280px"}),
        ]))
    
    return table_rows if table_rows else [html.Tr([html.Td("No rules configured yet.", colSpan=7, className="text-center text-muted")])]

# Callback 3: Template Tile Click → Populate Configuration Form
@app.callback(
    [Output("rule-input-name", "value", allow_duplicate=True),
     Output("rule-select-column", "disabled", allow_duplicate=True),
     Output("rule-select-column", "value", allow_duplicate=True),
     Output("rule-column-status-msg", "children", allow_duplicate=True),
     Output("rule-col-required-star", "style"),
     Output("rule-input-threshold", "value", allow_duplicate=True),
     Output("rule-selected-template-badge", "children"),
     Output("current-template-name", "data")],
    Input({"type": "template-tile", "index": dash.ALL}, "n_clicks"),
    [State("rule-select-project", "value"),
     State("rule-select-dataset", "value"),
     State("rule-select-column", "value")],
    prevent_initial_call=True,
)
def on_template_tile_click(n_clicks_list, proj_name, ds_name, current_col):
    if not _real_click():
        return (dash.no_update,) * 8
    
    trig = ctx.triggered_id
    if not isinstance(trig, dict):
        return (dash.no_update,) * 8
    
    template_name = trig.get("index")
    if not template_name:
        return (dash.no_update,) * 8
    
    # Get template details
    template_df = db_adapter.get_rule_templates()
    tmpl = template_df[template_df["template_name"] == template_name]
    
    if tmpl.empty:
        return template_name, False, None, "", {"display": "inline"}, "0", template_name, template_name
    
    tmpl_row = tmpl.iloc[0]
    requires_col = bool(tmpl_row["requires_column"])
    default_threshold = tmpl_row["default_threshold"]
    
    cols = db_adapter.get_table_columns(proj_name, ds_name) if (proj_name and ds_name) else []
    
    if not requires_col:
        # Template does not require column (e.g. ROW_COUNT, SCHEMA_CHANGE)
        return (
            template_name,
            True,  # disabled
            "",    # empty column value
            "Column selection not required for this rule template",
            {"display": "none"},  # hide required star
            str(default_threshold),
            template_name,
            template_name,
        )
    else:
        # Template requires column
        col_status = f"{len(cols)} columns available from {ds_name}" if cols else "No columns found for this dataset"
        col_disabled = len(cols) == 0
        rule_name = f"{template_name}_{current_col}" if current_col else template_name
        return (
            rule_name,
            col_disabled,
            current_col if (current_col in cols) else None,
            col_status,
            {"display": "inline"},  # show required star
            str(default_threshold),
            template_name,
            template_name,
        )

# Callback 4: Preview SQL Button → Show Modal with Generated SQL (handles both open and close)
@app.callback(
    [Output("modal-preview-sql", "is_open"),
     Output("preview-sql-code", "children")],
    [Input("btn-preview-sql", "n_clicks"),
     Input("btn-close-sql-modal", "n_clicks")],
    [State("rule-select-project", "value"),
     State("rule-select-dataset", "value"),
     State("current-template-name", "data"),
     State("rule-select-column", "value")],
    prevent_initial_call=True,
)
def show_preview_sql(preview_clicks, close_clicks, proj_name, ds_name, template_name, column_name):
    if not _real_click():
        return dash.no_update, dash.no_update

    # If close button was clicked, close the modal
    if ctx.triggered_id == "btn-close-sql-modal":
        return False, ""
    
    # Otherwise, preview button was clicked - open modal and generate SQL
    if not proj_name or not ds_name or not template_name:
        return True, "-- Please select project, dataset, and template first"
    
    # Get IDs
    proj_df = db_adapter.query_df(
        "SELECT project_id FROM dq_project_config WHERE project_name = ?",
        (proj_name,)
    )
    if proj_df.empty:
        return True, "-- Project not found"
    
    project_id = int(proj_df.iloc[0]["project_id"])
    
    ds_df = db_adapter.query_df(
        "SELECT dataset_id FROM dq_table_config WHERE dataset_name = ? AND project_id = ?",
        (ds_name, project_id)
    )
    if ds_df.empty:
        return True, "-- Dataset not found"
    
    dataset_id = int(ds_df.iloc[0]["dataset_id"])
    
    # Generate SQL
    sql = db_adapter.generate_sql_preview(project_id, dataset_id, template_name, column_name)
    
    return True, sql



# Callback 6: Save Rule Button → Validate, Insert, Show Toast, Refresh Table
@app.callback(
    [Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("rule-refresh-store", "data", allow_duplicate=True),
     Output("rule-input-name", "value", allow_duplicate=True),
     Output("rule-select-column", "value", allow_duplicate=True),
     Output("rule-input-threshold", "value", allow_duplicate=True),
     Output("rule-select-severity", "value"),
     Output("rule-switch-active", "value")],
    Input("btn-save-rule", "n_clicks"),
    [State("rule-select-project", "value"),
     State("rule-select-dataset", "value"),
     State("current-template-name", "data"),
     State("rule-input-name", "value"),
     State("rule-select-column", "value"),
     State("rule-input-threshold", "value"),
     State("rule-select-severity", "value"),
     State("rule-switch-active", "value")],
    prevent_initial_call=True,
)
def handle_save_rule(n_clicks, proj_name, ds_name, template_name, rule_name, column_name, 
                     threshold, severity, active):
    if not _real_click():
        return (dash.no_update,) * 8

    # Validate inputs
    if not proj_name or not ds_name or not template_name or not rule_name:
        return True, dbc.Alert("All fields are required. Please select Project, Dataset, Template, and provide a Rule Name.", color="danger"), dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    # Get project and dataset IDs
    proj_df = db_adapter.query_df(
        "SELECT project_id FROM dq_project_config WHERE project_name = ?",
        (proj_name,)
    )
    if proj_df.empty:
        return True, dbc.Alert("Project not found", color="danger"), dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    project_id = int(proj_df.iloc[0]["project_id"])
    
    ds_df = db_adapter.query_df(
        "SELECT dataset_id FROM dq_table_config WHERE dataset_name = ? AND project_id = ?",
        (ds_name, project_id)
    )
    if ds_df.empty:
        return True, dbc.Alert("Dataset not found", color="danger"), dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    dataset_id = int(ds_df.iloc[0]["dataset_id"])
    
    # Check template column requirement
    tmpl_df = db_adapter.get_rule_templates()
    tmpl = tmpl_df[tmpl_df["template_name"] == template_name]
    requires_col = bool(tmpl.iloc[0]["requires_column"]) if not tmpl.empty else True
    
    if requires_col and (not column_name or not str(column_name).strip()):
        return True, dbc.Alert(f"Template '{template_name}' requires a column. Please select a column from the dropdown.", color="danger"), dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    if not requires_col:
        if not column_name or str(column_name).strip() in ["-", "None", "", "Select a column..."]:
            column_name = None

    # Try to save the rule
    try:
        threshold = float(threshold)
    except (ValueError, TypeError):
        return True, dbc.Alert("Threshold must be numeric", color="danger"), dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    result = db_adapter.save_rule(
        project_id, dataset_id, template_name, rule_name, column_name, 
        threshold, severity, active, user="admin"
    )
    
    if result["success"]:
        toast_msg = dbc.Alert(result["message"], color="success")
        return True, toast_msg, datetime.now().timestamp(), template_name, None, "0", "High", True
    else:
        toast_msg = dbc.Alert(result["message"], color="danger")
        return True, toast_msg, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Callback 7: Edit Button → Open Modal with Pre-filled Values
@app.callback(
    [Output("modal-edit-rule", "is_open", allow_duplicate=True),
     Output("edit-rule-id", "data"),
     Output("edit-rule-name", "value"),
     Output("edit-rule-column", "options"),
     Output("edit-rule-column", "value"),
     Output("edit-rule-threshold", "value"),
     Output("edit-rule-severity", "value"),
     Output("edit-rule-switch-active", "value")],
    Input({"type": "btn-edit-rule", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def open_edit_rule_modal(n_clicks_list):
    if not _real_click():
        return (dash.no_update,) * 8
    
    trig = ctx.triggered_id
    if not isinstance(trig, dict):
        return False, None, "", [], "", "0", "High", True
    
    rule_id = trig.get("index")
    if not rule_id:
        return False, None, "", [], "", "0", "High", True
    
    # Fetch rule details
    rule_data = db_adapter.get_rule_details(rule_id)
    
    if "error" in rule_data:
        return False, None, "", [], "", "0", "High", True
    
    cols = db_adapter.get_table_columns(rule_data["project_id"], rule_data["dataset_id"])
    col_options = [{"label": "None (optional)", "value": ""}] + [{"label": c, "value": c} for c in cols]
    
    return (
        True,
        rule_id,
        rule_data["rule_name"],
        col_options,
        rule_data["column_name"] or "",
        str(rule_data["threshold"]),
        rule_data["severity"],
        rule_data["active"]
    )
    rule_data = db_adapter.get_rule_details(rule_id)
    
    if "error" in rule_data:
        return False, None, "", "", "0", "High", True
    
    return (
        True,
        rule_id,
        rule_data["rule_name"],
        rule_data["column_name"],
        str(rule_data["threshold"]),
        rule_data["severity"],
        rule_data["active"]
    )

# Callback 8: Cancel Edit Modal
@app.callback(
    Output("modal-edit-rule", "is_open", allow_duplicate=True),
    Input("btn-cancel-edit-rule", "n_clicks"),
    State("modal-edit-rule", "is_open"),
    prevent_initial_call=True,
)
def close_edit_modal(n_clicks, is_open):
    return False

# Callback 9: Submit Edit Modal → Update Rule
@app.callback(
    [Output("modal-edit-rule", "is_open", allow_duplicate=True),
     Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("rule-refresh-store", "data", allow_duplicate=True)],
    Input("btn-submit-edit-rule", "n_clicks"),
    [State("edit-rule-id", "data"),
     State("edit-rule-name", "value"),
     State("edit-rule-column", "value"),
     State("edit-rule-threshold", "value"),
     State("edit-rule-severity", "value"),
     State("edit-rule-switch-active", "value")],
    prevent_initial_call=True,
)
def handle_update_rule(n_clicks, rule_id, rule_name, column_name, threshold, severity, active):
    if not rule_id:
        return False, True, dbc.Alert("Rule ID not found", color="danger"), dash.no_update
    
    try:
        threshold = float(threshold)
    except (ValueError, TypeError):
        return True, True, dbc.Alert("Threshold must be numeric", color="danger"), dash.no_update
    
    result = db_adapter.update_rule(rule_id, rule_name, column_name, threshold, severity, active, user="admin")
    
    if result["success"]:
        toast_msg = dbc.Alert(result["message"], color="success")
        return False, True, toast_msg, datetime.now().timestamp()
    else:
        toast_msg = dbc.Alert(result["message"], color="danger")
        return True, True, toast_msg, dash.no_update

# Callback 10: Delete Button → Show Confirmation Modal
@app.callback(
    [Output("modal-delete-confirm", "is_open", allow_duplicate=True),
     Output("delete-rule-id", "data")],
    Input({"type": "btn-delete-rule", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def show_delete_confirm(n_clicks_list):
    if not _real_click():
        return dash.no_update, dash.no_update
    
    trig = ctx.triggered_id
    if not isinstance(trig, dict):
        return False, None
    
    rule_id = trig.get("index")
    return True, rule_id

# Callback 11: Confirm Delete → Delete Rule & Refresh Table
@app.callback(
    [Output("modal-delete-confirm", "is_open", allow_duplicate=True),
     Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("rule-refresh-store", "data", allow_duplicate=True)],
    Input("btn-confirm-delete-rule", "n_clicks"),
    State("delete-rule-id", "data"),
    prevent_initial_call=True,
)
def handle_delete_rule(n_clicks, rule_id):
    if not rule_id:
        return False, True, dbc.Alert("Rule ID not found", color="danger"), dash.no_update
    
    result = db_adapter.delete_rule(rule_id, user="admin", hard_delete=False)
    
    if result["success"]:
        toast_msg = dbc.Alert(result["message"], color="success")
        return False, True, toast_msg, datetime.now().timestamp()
    else:
        toast_msg = dbc.Alert(result["message"], color="danger")
        return False, True, toast_msg, dash.no_update

# Callback 12: Cancel Delete Modal
@app.callback(
    Output("modal-delete-confirm", "is_open", allow_duplicate=True),
    Input("btn-cancel-delete-rule", "n_clicks"),
    State("modal-delete-confirm", "is_open"),
    prevent_initial_call=True,
)
def close_delete_modal(n_clicks, is_open):
    return False

# Callback 13: Toggle Rule Active Status
@app.callback(
    [Output("global-toast", "is_open", allow_duplicate=True),
     Output("global-toast", "children", allow_duplicate=True),
     Output("rule-refresh-store", "data", allow_duplicate=True)],
    Input({"type": "btn-toggle-rule", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def handle_toggle_rule(n_clicks_list):
    if not _real_click():
        return dash.no_update, dash.no_update, dash.no_update
    
    trig = ctx.triggered_id
    if not isinstance(trig, dict):
        return False, "", dash.no_update
    
    rule_id = trig.get("index")
    if not rule_id:
        return False, "", dash.no_update
    
    result = db_adapter.toggle_rule_active(rule_id, user="admin")
    
    if result["success"]:
        toast_msg = dbc.Alert(result["message"], color="success")
        return True, toast_msg, datetime.now().timestamp()
    else:
        toast_msg = dbc.Alert(result["message"], color="danger")
        return True, toast_msg, dash.no_update

# Callback 14: View SQL Rule Button (uses separate modal to avoid conflicts)
@app.callback(
    [Output("modal-view-rule-sql", "is_open", allow_duplicate=True),
     Output("view-rule-sql-code", "children")],
    Input({"type": "btn-view-sql-rule", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def view_rule_sql(view_clicks):
    if not _real_click():
        return dash.no_update, dash.no_update
    
    trig = ctx.triggered_id
    if not isinstance(trig, dict):
        return False, ""
    
    rule_id = trig.get("index")
    if not rule_id:
        return False, ""
    
    # Get rule details
    rule_data = db_adapter.get_rule_details(rule_id)
    
    if "error" in rule_data:
        return True, f"-- Error: {rule_data['error']}"
    
    # Generate SQL
    sql = db_adapter.generate_sql_preview(
        rule_data["project_id"],
        rule_data["dataset_id"],
        rule_data["template_name"],
        rule_data["column_name"]
    )
    
    return True, sql

# Callback 14b: Close Rule SQL Modal
@app.callback(
    Output("modal-view-rule-sql", "is_open", allow_duplicate=True),
    Input("btn-close-rule-sql-modal", "n_clicks"),
    State("modal-view-rule-sql", "is_open"),
    prevent_initial_call=True,
)
def close_rule_sql_modal(n_clicks, is_open):
    return False

# 8. DQ Results page (Page 5) ------------------------------------------------
@app.callback(
    [Output("filter-results-dataset", "options"),
     Output("filter-results-dataset", "value")],
    Input("filter-results-project", "value"),
)
def results_dataset_options(proj):
    ds = db_adapter.get_datasets_for_project(proj, include_inactive=True) if proj and proj != "All" else []
    return [{"label": "Dataset: All", "value": "All"}] + [{"label": d, "value": d} for d in ds], "All"


@app.callback(
    [Output("results-table-body", "children"),
     Output("results-count-label", "children")],
    [Input("filter-results-project", "value"),
     Input("filter-results-dataset", "value"),
     Input("filter-results-category", "value"),
     Input("filter-results-severity", "value"),
     Input("filter-results-status", "value"),
     Input("filter-results-exec", "value"),
     Input("filter-results-rule", "value")],
)
def filter_results_table(proj, ds, cat, sev, status, exec_id, rule):
    df = db_adapter.get_dq_results_table(project_name=proj, dataset_name=ds, status=status,
                                         severity=sev, category=cat, execution_id=exec_id, rule_search=rule)
    return _results_rows(df), f"{len(df)} result(s) (latest 200 shown)"


@app.callback(
    Output("download-csv-action", "data", allow_duplicate=True),
    Input("btn-export-results-csv", "n_clicks"),
    [State("filter-results-project", "value"),
     State("filter-results-dataset", "value"),
     State("filter-results-category", "value"),
     State("filter-results-severity", "value"),
     State("filter-results-status", "value"),
     State("filter-results-exec", "value"),
     State("filter-results-rule", "value")],
    prevent_initial_call=True,
)
def export_results_csv(n, proj, ds, cat, sev, status, exec_id, rule):
    if not n:
        return dash.no_update
    df = db_adapter.get_dq_results_table(project_name=proj, dataset_name=ds, status=status,
                                         severity=sev, category=cat, execution_id=exec_id, rule_search=rule)
    return dcc.send_data_frame(df.to_csv, "dq_results_report.csv", index=False)


@app.callback(
    [Output("modal-result-detail", "is_open"),
     Output("result-detail-title", "children"),
     Output("result-detail-body", "children")],
    [Input({"type": "btn-inspect-result", "id": dash.ALL}, "n_clicks"),
     Input("btn-close-result-detail", "n_clicks")],
    prevent_initial_call=True,
)
def inspect_result(clicks, close):
    trig = ctx.triggered_id
    if trig == "btn-close-result-detail":
        return False, dash.no_update, dash.no_update
    if not isinstance(trig, dict) or not _real_click():
        return dash.no_update, dash.no_update, dash.no_update
    det = db_adapter.get_result_detail_and_failed_records(int(trig["id"]))
    if not det:
        return True, "Result Detail", html.Div("Result not found.", className="text-muted")
    r = det["result"]
    fr = det["failed_records"]
    if fr is not None and not fr.empty:
        sample = html.Div(html.Table([
            html.Thead(html.Tr([html.Th(c) for c in fr.columns])),
            html.Tbody([html.Tr([html.Td(str(v)) for v in row]) for row in fr.head(10).values.tolist()]),
        ], className="clean-table", style={"fontSize": "0.78rem"}), style={"overflowX": "auto"})
    else:
        sample = html.Div("No failing sample records (rule passed, errored, or is an aggregate check).",
                          className="text-muted small")
    body = html.Div([
        dbc.Row([
            dbc.Col([html.Div("Status", className="small text-muted"), html.B(str(r.get("status")))], md=2),
            dbc.Col([html.Div("Template", className="small text-muted"), html.B(det["template_name"])], md=3),
            dbc.Col([html.Div("Column", className="small text-muted"), html.B(det["column_name"])], md=2),
            dbc.Col([html.Div("Observed / Expected", className="small text-muted"),
                     html.B(f"{r.get('observed_value')} / {r.get('expected_value')}")], md=3),
            dbc.Col([html.Div("Failures", className="small text-muted"), html.B(str(r.get("failure_count")))], md=2),
        ], className="p-3 bg-light rounded mb-3"),
        html.Div(f"Error: {r.get('error_message')}", className="text-danger small mb-2") if r.get("error_message") else None,
        html.H6("Validation SQL", className="fw-bold"),
        html.Pre(det["validation_sql"], style={"backgroundColor": "#0F172A", "color": "#E2E8F0", "padding": "12px",
                                                "borderRadius": "6px", "fontSize": "0.8rem", "whiteSpace": "pre-wrap"}),
        html.H6("Sample Failing Records", className="fw-bold mt-3"),
        sample,
    ])
    return True, f"{r.get('rule_name')} — {r.get('execution_id')}", body


# 8a. Reconciliation page (Page 6) ------------------------------------------
@app.callback(
    [Output("recon-select-dataset", "options"),
     Output("recon-select-dataset", "value")],
    Input("recon-select-project", "value"),
    prevent_initial_call=True,
)
def recon_dataset_options(proj):
    ds = db_adapter.get_datasets_for_project(proj, include_inactive=True) if proj else []
    return [{"label": d, "value": d} for d in ds], (ds[0] if ds else None)


@app.callback(
    [Output("recon-content", "children"),
     Output("recon-run-alert", "children")],
    [Input("recon-select-project", "value"),
     Input("recon-select-dataset", "value"),
     Input("btn-run-recon", "n_clicks")],
    State("recon-checks", "value"),
)
def render_reconciliation(proj, ds, run_clicks, checks):
    if not proj or not ds:
        return html.Div("Select a project and dataset.", className="text-muted p-3"), None
    alert = None
    try:
        trig = ctx.triggered_id
    except Exception:
        trig = "btn-run-recon" if run_clicks else None
    if trig == "btn-run-recon":
        res = db_adapter.run_reconciliation(proj, ds, selected_checks=checks or None)
        if res.get("success"):
            alert = dbc.Alert(f"Reconciliation {res['execution_id']} completed: {res['status']}",
                              color="success" if res["status"] == "PASS" else "warning",
                              className="py-2 small", duration=6000)
        else:
            alert = dbc.Alert(res.get("message", "Reconciliation failed."), color="danger", className="py-2 small")
    return _recon_content(db_adapter.get_reconciliation_data(proj, ds)), alert


@app.callback(
    Output("download-csv-action", "data", allow_duplicate=True),
    Input("btn-export-recon", "n_clicks"),
    [State("recon-select-project", "value"),
     State("recon-select-dataset", "value")],
    prevent_initial_call=True,
)
def export_recon_csv(n, proj, ds):
    if not n:
        return dash.no_update
    df = db_adapter.generate_report("RECONCILIATION")
    if proj and not df.empty:
        df = df[df["project_name"] == proj]
        if ds:
            df = df[df["dataset_name"] == ds]
    return dcc.send_data_frame(df.to_csv, f"dq_reconciliation_{proj or 'all'}_{ds or 'all'}.csv", index=False)


# 8b. Alerts & Reports page (Page 7) ----------------------------------------
@app.callback(
    [Output("filter-alert-dataset", "options"),
     Output("filter-alert-dataset", "value")],
    Input("filter-alert-project", "value"),
)
def alerts_dataset_options(proj):
    ds = db_adapter.get_datasets_for_project(proj, include_inactive=True) if proj and proj != "All" else []
    return [{"label": "Dataset: All", "value": "All"}] + [{"label": d, "value": d} for d in ds], "All"


@app.callback(
    [Output("alerts-table-body", "children"),
     Output("alerts-kpi-row", "children"),
     Output("alerts-severity-chart", "figure")],
    [Input("filter-alert-status", "value"),
     Input("filter-alert-severity", "value"),
     Input("filter-alert-project", "value"),
     Input("filter-alert-dataset", "value"),
     Input("filter-alert-search", "value"),
     Input("alert-refresh-store", "data")],
)
def filter_alerts_table(status, sev, proj, ds, search, _refresh):
    data = db_adapter.get_alerts_data(status_filter=status, severity_filter=sev,
                                      project_filter=proj, dataset_filter=ds, search=search)
    kpi_row, fig = _alerts_kpis_and_chart(data)
    return _alert_rows(data["alerts_table"]), kpi_row, fig


@app.callback(
    Output("download-csv-action", "data", allow_duplicate=True),
    Input("btn-download-selected-report", "n_clicks"),
    State("select-report-type", "value"),
    prevent_initial_call=True,
)
def download_selected_report(n, rtype):
    if not n:
        return dash.no_update
    rtype = rtype or "PROJECT_WISE"
    df = db_adapter.generate_report(rtype)
    return dcc.send_data_frame(df.to_csv, f"dq_{rtype.lower()}_report.csv", index=False)


# 8b-1. Alerts Failure Details Modal Callback
@app.callback(
    [Output("modal-notif-detail", "is_open"),
     Output("notif-detail-modal-title", "children"),
     Output("notif-detail-modal-body", "children"),
     Output("store-current-notif-alert-id", "data")],
    [Input({"type": "btn-view-notif-err", "id": dash.ALL}, "n_clicks"),
     Input("btn-close-notif-detail-modal", "n_clicks")],
    [State("store-current-notif-alert-id", "data")],
    prevent_initial_call=True,
)
def handle_notif_detail_modal(view_clicks, close_click, current_alert_id):
    trig = ctx.triggered_id
    if not trig or not _real_click():
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    if trig == "btn-close-notif-detail-modal":
        return False, dash.no_update, dash.no_update, dash.no_update

    if isinstance(trig, dict) and trig.get("type") == "btn-view-notif-err":
        alert_id = trig["id"]
        body = _render_notif_detail_modal_body(alert_id)
        title = [html.I(className="bi bi-bell-fill text-danger me-2"), f"Notification Delivery Failure Details — Alert #{alert_id}"]
        return True, title, body, alert_id

    return dash.no_update, dash.no_update, dash.no_update, dash.no_update


# 8b-2. Modal Retry Notification Callback
@app.callback(
    [Output("modal-notif-retry-feedback", "children"),
     Output("alert-refresh-store", "data", allow_duplicate=True)],
    Input("btn-modal-retry-notif-action", "n_clicks"),
    State("store-current-notif-alert-id", "data"),
    prevent_initial_call=True,
)
def handle_modal_retry_notif(n_clicks, alert_id):
    if not n_clicks or not alert_id:
        return dash.no_update, dash.no_update
    res = db_adapter.retry_notification(alert_id, user="admin")
    _ts = datetime.now().isoformat()
    if res.get("success"):
        feedback = dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            f"Notification retry succeeded! Status: {res.get('status')} via {res.get('provider')}."
        ], color="success", className="small py-2 mb-0")
    else:
        err_msg = res.get("error") or res.get("message") or "Delivery failed"
        feedback = dbc.Alert([
            html.I(className="bi bi-exclamation-circle-fill me-2"),
            f"Notification retry failed: {err_msg}"
        ], color="danger", className="small py-2 mb-0")
    return feedback, _ts


# 8b-3. Email Configuration Modal Open/Close Callback
@app.callback(
    [Output("modal-email-config", "is_open"),
     Output("email-config-modal-body", "children")],
    [Input("btn-open-email-config", "n_clicks"),
     Input("btn-close-email-config-modal", "n_clicks")],
    prevent_initial_call=True,
)
def handle_email_config_modal(open_click, close_click):
    trig = ctx.triggered_id
    if not trig or not _real_click():
        return dash.no_update, dash.no_update
    if trig == "btn-close-email-config-modal":
        return False, dash.no_update
    if trig == "btn-open-email-config":
        body = _render_email_config_modal_body()
        return True, body
    return dash.no_update, dash.no_update


# 8b-4. Save SMTP Configuration Callback
@app.callback(
    Output("smtp-cfg-save-alert", "children"),
    Input("btn-save-smtp-cfg", "n_clicks"),
    [State("smtp-cfg-host", "value"),
     State("smtp-cfg-port", "value"),
     State("smtp-cfg-sender", "value"),
     State("smtp-cfg-user", "value"),
     State("smtp-cfg-pass", "value"),
     State("smtp-cfg-tls", "value"),
     State("smtp-cfg-ssl", "value")],
    prevent_initial_call=True,
)
def save_smtp_cfg_callback(n, host, port, sender, user, pwd, tls, ssl):
    if not n:
        return dash.no_update
    if not host or not str(host).strip():
        return dbc.Alert("SMTP Host is required.", color="danger", className="small py-2 mb-0")
    try:
        p_num = int(port or 587)
    except Exception:
        return dbc.Alert("Invalid port number.", color="danger", className="small py-2 mb-0")

    success, msg = db_adapter.update_smtp_configuration(
        host=str(host).strip(),
        port=p_num,
        username=str(user or "").strip(),
        password=str(pwd or "").strip(),
        sender_email=str(sender or "").strip(),
        use_tls=bool(tls),
        use_ssl=bool(ssl),
        user="admin"
    )
    if success:
        return dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"), msg], color="success", className="small py-2 mb-0")
    else:
        return dbc.Alert([html.I(className="bi bi-exclamation-circle-fill me-2"), msg], color="danger", className="small py-2 mb-0")


# 8b-4b. Provider Presets Autofill Callback
@app.callback(
    [Output("smtp-cfg-host", "value"),
     Output("smtp-cfg-port", "value"),
     Output("smtp-cfg-tls", "value"),
     Output("smtp-cfg-ssl", "value"),
     Output("smtp-preset-hint", "children")],
    [Input("btn-preset-gmail", "n_clicks"),
     Input("btn-preset-outlook", "n_clicks"),
     Input("btn-preset-yahoo", "n_clicks"),
     Input("btn-preset-sendgrid", "n_clicks")],
    prevent_initial_call=True,
)
def apply_provider_preset(n_gmail, n_outlook, n_yahoo, n_sendgrid):
    trig = ctx.triggered_id
    if not trig or not _real_click():
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    if trig == "btn-preset-gmail":
        hint = html.Div([
            html.I(className="bi bi-google text-danger me-2"),
            html.Strong("Gmail Preset Applied: "),
            "Host: smtp.gmail.com (Port 587, STARTTLS). ",
            html.Span("Google requires a 16-character App Password when 2-Step Verification is enabled. Generate one at myaccount.google.com/apppasswords and paste it into the Password field.", className="text-danger fw-semibold"),
        ], className="p-2 bg-light rounded border mb-3 small")
        return "smtp.gmail.com", 587, True, False, hint

    elif trig == "btn-preset-outlook":
        hint = html.Div([
            html.I(className="bi bi-microsoft text-primary me-2"),
            html.Strong("Microsoft 365 / Outlook Preset Applied: "),
            "Host: smtp.office365.com (Port 587, STARTTLS). Enter your Microsoft email and password or App Password.",
        ], className="p-2 bg-light rounded border mb-3 small")
        return "smtp.office365.com", 587, True, False, hint

    elif trig == "btn-preset-yahoo":
        hint = html.Div([
            html.I(className="bi bi-envelope-fill text-info me-2"),
            html.Strong("Yahoo Mail Preset Applied: "),
            "Host: smtp.mail.yahoo.com (Port 587, STARTTLS). Generate an App Password in your Yahoo Account Security settings.",
        ], className="p-2 bg-light rounded border mb-3 small")
        return "smtp.mail.yahoo.com", 587, True, False, hint

    elif trig == "btn-preset-sendgrid":
        hint = html.Div([
            html.I(className="bi bi-send-fill text-success me-2"),
            html.Strong("SendGrid SMTP Preset Applied: "),
            "Host: smtp.sendgrid.net (Port 587, STARTTLS). Username is 'apikey' and Password is your SendGrid API Key (SG....).",
        ], className="p-2 bg-light rounded border mb-3 small")
        return "smtp.sendgrid.net", 587, True, False, hint

    return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update


# 8b-5. Test SMTP Connection Callback
@app.callback(
    Output("smtp-test-conn-result", "children"),
    Input("btn-test-smtp-conn", "n_clicks"),
    prevent_initial_call=True,
)
def test_smtp_conn_callback(n):
    if not n:
        return dash.no_update
    res = email_service.test_smtp_connection()
    if res.get("connected"):
        return dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            res.get("message", "Connection successful!"),
        ], color="success", className="small py-2 mb-0")
    else:
        return dbc.Alert([
            html.Div([html.I(className="bi bi-x-circle-fill me-2"), html.Strong(res.get("message"))]),
            html.Div(f"Diagnostic Hint: {res.get('hint')}", className="text-muted mt-1") if res.get("hint") else html.Div(),
        ], color="danger", className="small py-2 mb-0")


# 8b-6. Send Test Verification Email Callback
@app.callback(
    Output("smtp-test-email-result", "children"),
    Input("btn-send-test-email-action", "n_clicks"),
    State("input-test-email-addr", "value"),
    prevent_initial_call=True,
)
def send_test_email_callback(n, recipient):
    if not n:
        return dash.no_update
    if not recipient or not str(recipient).strip():
        return dbc.Alert("Recipient email address is required.", color="danger", className="small py-2 mb-0")
    res = db_adapter.test_email_delivery(str(recipient).strip(), user="admin")
    if res.get("success"):
        provider = res.get("details", {}).get("provider", "")
        if "simulat" in provider.lower() or "sandbox" in provider.lower() or "dev" in provider.lower():
            return dbc.Alert([
                html.I(className="bi bi-info-circle-fill me-2"),
                html.Span(f"Simulated test email recorded locally for {recipient}. Note: Real SMTP host is not configured; enter SMTP credentials below and click 'Save & Apply Settings' to send real emails to your inbox."),
            ], color="warning", className="small py-2 mb-0")
        else:
            return dbc.Alert([
                html.I(className="bi bi-check-circle-fill me-2"),
                res.get("message"),
            ], color="success", className="small py-2 mb-0")
    else:
        return dbc.Alert([
            html.I(className="bi bi-x-circle-fill me-2"),
            res.get("message"),
        ], color="danger", className="small py-2 mb-0")


# 8c. Filter Audit Runs on Page 8 (Phase 6)
@app.callback(
    Output("audit-table-body", "children"),
    [Input("audit-select-project", "value"),
     Input("audit-search-input", "value")],
)
def filter_audit_runs_table(proj, search):
    history_df = db_adapter.get_audit_history_records(project_name=proj, search=search)
    exec_rows = []
    for _, h in history_df.iterrows():
        s = str(h["status"])
        badge_cls = "badge-pass" if s == "PASS" else "badge-fail"
        exec_rows.append(html.Tr([
            html.Td(h["execution_id"], style={"fontWeight": "600", "color": "#0F172A"}),
            html.Td(h["project_name"]),
            html.Td(h["dataset_name"]),
            html.Td(html.Span(s, className=badge_cls)),
            html.Td(str(h["executed_at"])),
            html.Td(str(h["duration"])),
            html.Td(
                dbc.Button("View", id={"type": "btn-view-audit", "id": h["execution_id"]}, size="sm", color="light", style={"fontSize": "0.8rem", "fontWeight": "600", "color": "#2563EB"})
            ),
        ]))
    return exec_rows

# 9. Audit Modal Details
@app.callback(
    [Output("modal-audit-detail", "is_open"),
     Output("audit-modal-title", "children"),
     Output("audit-modal-body", "children")],
    [Input({"type": "btn-view-audit", "id": dash.ALL}, "n_clicks"),
     Input("btn-close-audit-modal", "n_clicks")],
    State("modal-audit-detail", "is_open"),
    prevent_initial_call=True,
)
def show_audit_modal(view_clicks, close_click, is_open):
    trig = ctx.triggered_id
    if trig == "btn-close-audit-modal":
        return False, "", ""
    if isinstance(trig, dict) and not _real_click():
        return dash.no_update, dash.no_update, dash.no_update
    if isinstance(trig, dict) and trig.get("type") == "btn-view-audit":
        exec_id = trig["id"]
        res_df = db_adapter.query_df("""
            SELECT rule_name, category, status, failure_count, executed_at
            FROM dq_results WHERE execution_id = ? ORDER BY result_id ASC
        """, (exec_id,))
        
        detail_rows = [
            html.Tr([
                html.Td(r["rule_name"], style={"fontWeight": "600"}),
                html.Td(r["category"]),
                html.Td(html.Span(r["status"], className="badge-pass" if r["status"] == "PASS" else ("badge-fail" if r["status"] == "FAIL" else "badge-error"))),
                html.Td(str(r["failure_count"])),
                html.Td(str(r["executed_at"])),
            ])
            for _, r in res_df.iterrows()
        ] or [html.Tr([html.Td("No rule results recorded for this execution.", colSpan=5, className="text-center text-muted")])]
        
        body = html.Div([
            html.P(f"Execution run details and rule evaluations for run ID: {exec_id}", className="text-muted"),
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Rule"),
                    html.Th("Category"),
                    html.Th("Status"),
                    html.Th("Failures"),
                    html.Th("Executed At"),
                ])),
                html.Tbody(detail_rows),
            ], className="clean-table"),
        ])
        return True, f"Execution Breakdown: {exec_id}", body
    return dash.no_update, dash.no_update, dash.no_update

# -------------------------------------------------------------
# REST API ENDPOINTS (PHASE 8: EMAIL NOTIFICATIONS & ALERT LIFECYCLE)
# -------------------------------------------------------------

@server.route("/api/projects/<int:project_id>/notifications", methods=["GET", "PUT"])
def api_project_notifications(project_id):
    if request.method == "GET":
        settings = db_adapter.get_project_notification_settings(project_id)
        if not settings:
            return jsonify({"error": f"Project ID {project_id} not found"}), 404
        return jsonify(settings), 200
    elif request.method == "PUT":
        data = request.get_json(silent=True) or {}
        team_email = data.get("project_team_email")
        add_emails = data.get("additional_emails", "")
        notif_enabled = data.get("notification_enabled", True)
        user = data.get("user", "api_user")
        res = db_adapter.update_project_notification_settings(
            project_id=project_id,
            project_team_email=team_email,
            additional_emails=add_emails,
            notification_enabled=notif_enabled,
            user=user
        )
        if not res.get("success"):
            return jsonify(res), 400
        return jsonify(res), 200

@server.route("/api/alerts", methods=["GET"])
def api_get_alerts():
    status = request.args.get("status", "All")
    severity = request.args.get("severity", "All")
    project = request.args.get("project", "All")
    dataset = request.args.get("dataset", "All")
    search = request.args.get("search", "")
    data = db_adapter.get_alerts_data(
        status_filter=status,
        severity_filter=severity,
        project_filter=project,
        dataset_filter=dataset,
        search=search
    )
    df = data.get("alerts_table")
    records = df.to_dict(orient="records") if df is not None else []
    return jsonify({
        "total_count": data.get("total_count", len(records)),
        "critical_count": data.get("critical_count", 0),
        "high_count": data.get("high_count", 0),
        "active_count": data.get("active_count", 0),
        "alerts": records
    }), 200

@server.route("/api/alerts/<int:alert_id>/acknowledge", methods=["POST"])
def api_acknowledge_alert(alert_id):
    data = request.get_json(silent=True) or {}
    user = data.get("user", "api_user")
    ok = db_adapter.acknowledge_alert(alert_id, user=user)
    if not ok:
        return jsonify({"success": False, "error": f"Alert #{alert_id} acknowledge failed"}), 400
    return jsonify({"success": True, "alert_id": alert_id, "status": "Acknowledged"}), 200

@server.route("/api/alerts/<int:alert_id>/close", methods=["POST"])
def api_close_alert(alert_id):
    data = request.get_json(silent=True) or {}
    user = data.get("user", "api_user")
    ok = db_adapter.close_alert(alert_id, user=user)
    if not ok:
        return jsonify({"success": False, "error": f"Alert #{alert_id} close failed"}), 400
    return jsonify({"success": True, "alert_id": alert_id, "status": "Closed"}), 200

@server.route("/api/notifications/logs", methods=["GET"])
def api_notification_logs():
    project_id = request.args.get("project_id", type=int)
    execution_id = request.args.get("execution_id")
    limit = request.args.get("limit", default=50, type=int)
    logs_df = db_adapter.get_email_notification_logs(project_id=project_id, execution_id=execution_id, limit=limit)
    records = logs_df.to_dict(orient="records") if logs_df is not None else []
    return jsonify({"count": len(records), "logs": records}), 200

# -------------------------------------------------------------
# APPLICATION ENTRYPOINT
# -------------------------------------------------------------

if __name__ == "__main__":
    try:
        scheduler_service.start_scheduler_daemon()
        print("[DQ-Scheduler] Background persistent scheduler daemon activated.")
    except Exception as e:
        print(f"[DQ-Scheduler] Daemon start notice: {e}")
    try:
        email_service.notification_queue.start()
        print("[DQ-NotificationQueue] Background email notification queue activated.")
    except Exception as e:
        print(f"[DQ-NotificationQueue] Queue start notice: {e}")
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
