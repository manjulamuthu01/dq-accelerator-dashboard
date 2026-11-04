"""
Dynamic Data Quality Accelerator - Management & Monitoring Dashboard (Phase 5)
Enterprise Databricks App built with Dash & Plotly.
Clean enterprise layout, zero emojis, Bootstrap Icons, real database actions.
"""

import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

import dash
from dash import Dash, html, dcc, Input, Output, State, ctx, dash_table
import dash_bootstrap_components as dbc

import db_adapter
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
            width: 35%;
            height: 3px;
            background-color: #10B981;
            z-index: 2;
        }
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
    ("management", "Projects & Datasets", "bi bi-layers"),
    ("rules", "Rule Library", "bi bi-book"),
    ("execution", "Execution & Monitoring", "bi bi-play-circle"),
    ("results", "DQ Results", "bi bi-check2-square"),
    ("reconciliation", "Reconciliation", "bi bi-arrow-left-right"),
    ("alerts", "Alerts & Reports", "bi bi-bell"),
    ("audit", "Audit History", "bi bi-clock-history"),
]

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
            html.Div(className="status-dot"),
            html.Span("Running", style={"fontWeight": "500", "color": "#10B981"}),
        ], className="sidebar-footer"),
    ], className="sidebar")

# -------------------------------------------------------------
# ROOT LAYOUT
# -------------------------------------------------------------

app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Download(id="download-csv-action"),
    # Store that triggers a live refresh of the projects table after any mutation.
    # Value is a timestamp string updated after Add/Edit/Toggle actions.
    dcc.Store(id="project-refresh-store", data=""),
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

# -------------------------------------------------------------
# PAGE 1: OVERVIEW DASHBOARD
# -------------------------------------------------------------

def page_overview():
    kpis = db_adapter.get_overview_kpis()
    trend_df = db_adapter.get_trend_history()
    health = db_adapter.get_project_health()
    recent_execs = db_adapter.get_recent_executions()

    # 1. Top KPI Row (6 cards matching Screen 1)
    kpi_cards = [
        ("Projects", kpis["projects_count"], "bi bi-layers", "#EFF6FF", "#2563EB"),
        ("Tables", kpis["tables_count"], "bi bi-table", "#FAF5FF", "#9333EA"),
        ("PASS", kpis["pass_count"], "bi bi-check-circle-fill", "#DCFCE7", "#15803D"),
        ("FAIL", kpis["fail_count"], "bi bi-x-circle-fill", "#FEE2E2", "#B91C1C"),
        ("ERROR", kpis["error_count"], "bi bi-exclamation-triangle-fill", "#FEF3C7", "#B45309"),
        ("DQ Score", f"{int(kpis['dq_score'])}%", "bi bi-shield-check", "#CCFBF1", "#0F766E"),
    ]

    kpi_cols = [
        dbc.Col(
            html.Div([
                html.Div([
                    html.Div(title, className="metric-title"),
                    html.Div(str(val), className="metric-val"),
                ]),
                html.Div(
                    html.I(className=icon, style={"color": fg}),
                    className="metric-icon-box",
                    style={"backgroundColor": bg},
                ),
            ], className="metric-pill-card"),
            xs=12, sm=6, md=4, lg=2, className="mb-3",
        )
        for title, val, icon, bg, fg in kpi_cards
    ]

    # 2. Middle Row: Trend Line Chart (65%) & Project Health Donut (35%)
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=trend_df["Date"], y=trend_df["DQ Score"],
        mode="lines+markers", name="DQ Score",
        line=dict(color="#2563EB", width=2.5),
        marker=dict(size=6, color="#2563EB")
    ))
    fig_trend.add_trace(go.Scatter(
        x=trend_df["Date"], y=trend_df["Target"],
        mode="lines", name="Target (95%)",
        line=dict(color="#94A3B8", width=1.5, dash="dash")
    ))
    fig_trend.update_layout(
        margin=dict(l=40, r=20, t=10, b=30),
        height=260,
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(range=[60, 102], gridcolor="#F1F5F9", ticksuffix="%"),
        xaxis=dict(gridcolor="#F1F5F9"),
    )

    # Donut Chart
    fig_donut = go.Figure(data=[go.Pie(
        labels=["Healthy", "At Risk", "Critical"],
        values=[health["healthy"], health["at_risk"], health["critical"]],
        hole=0.72,
        marker=dict(colors=["#10B981", "#F59E0B", "#EF4444"]),
        textinfo="none",
        sort=False,
    )])
    fig_donut.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        height=220,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.0),
        annotations=[dict(text=f"<b>{health['health_score']}%</b>", x=0.35, y=0.5, font_size=24, font_family="Inter", showarrow=False, font_color="#0F172A")],
        paper_bgcolor="#FFFFFF",
    )

    # 3. Bottom Row: Recent Executions Table & Recent Critical Failures
    exec_rows = []
    for _, row in recent_execs.iterrows():
        s = str(row["Status"])
        b_class = "badge-pass" if s == "PASS" else ("badge-fail" if s == "FAIL" else "badge-error")
        exec_rows.append(html.Tr([
            html.Td(row["Time"]),
            html.Td(row["Project"], style={"fontWeight": "500"}),
            html.Td(html.Span(s, className=b_class)),
            html.Td(row["DQ Score"], style={"fontWeight": "600", "color": "#0F172A"}),
        ]))

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-speedometer2 text-primary"),
                "Overview Dashboard"
            ], className="page-main-title"),
            html.Div([
                html.I(className="bi bi-calendar3"),
                f"Last Updated: {datetime.now().strftime('%d %b %Y, %I:%M %p')}",
            ], className="header-meta"),
        ], className="page-header-bar"),

        # Top 6 KPI Cards
        dbc.Row(kpi_cols, className="g-3 mb-3"),

        # Middle Row: Trend & Donut
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.Div("Data Quality Trend", className="dq-card-title"),
                    dcc.Graph(figure=fig_trend, config={"displayModeBar": False}),
                ], className="dq-card"),
                lg=7, className="mb-4",
            ),
            dbc.Col(
                html.Div([
                    html.Div("Project Health", className="dq-card-title"),
                    dcc.Graph(figure=fig_donut, config={"displayModeBar": False}),
                ], className="dq-card"),
                lg=5, className="mb-4",
            ),
        ], className="g-3"),

        # Bottom Row: Executions & Failures
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.Div("Recent Executions", className="dq-card-title"),
                    html.Table([
                        html.Thead(html.Tr([
                            html.Th("Time"),
                            html.Th("Project"),
                            html.Th("Status"),
                            html.Th("DQ Score"),
                        ])),
                        html.Tbody(exec_rows),
                    ], className="clean-table"),
                ], className="dq-card"),
                lg=8, className="mb-4",
            ),
            dbc.Col(
                html.Div([
                    html.Div("Recent Critical Failures", className="dq-card-title"),
                    html.Div([
                        html.Div(
                            html.I(className="bi bi-exclamation-triangle-fill text-danger", style={"fontSize": "1.8rem"}),
                            style={"marginBottom": "12px"}
                        ),
                        html.Div("1 project has critical issues", style={"fontWeight": "600", "color": "#1E293B", "marginBottom": "6px"}),
                        html.A([
                            "View Details ",
                            html.I(className="bi bi-arrow-right"),
                        ], href="/alerts", style={"color": "#2563EB", "fontSize": "0.88rem", "fontWeight": "600", "textDecoration": "none"}),
                    ], style={"padding": "24px", "backgroundColor": "#FEF2F2", "borderRadius": "8px", "border": "1px solid #FEE2E2", "textAlign": "center"}),
                ], className="dq-card"),
                lg=4, className="mb-4",
            ),
        ], className="g-3"),
    ])

# -------------------------------------------------------------
# PAGE 2: PROJECT & DATASET MANAGEMENT
# -------------------------------------------------------------

def _build_management_table_rows(projects_df):
    """
    Build the list of <tr> elements for the projects management table.
    Extracted as a standalone helper so the refresh callback can call it
    without re-rendering the full page layout.
    """
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

        rows.append(html.Tr([
            html.Td([
                html.Div([
                    html.I(className="bi bi-folder-fill text-primary me-2"),
                    html.Span(p["project_name"], style={"fontWeight": "600", "fontSize": "0.95rem"}),
                ]),
                html.Div(f"Owner: {owner_name}", className="small text-muted ps-4"),
            ]),
            html.Td(p["description"] or "—"),
            html.Td(owner_name),
            # Dataset count badge
            html.Td(
                html.Span(
                    f"{ds_count} dataset{'s' if ds_count != 1 else ''}",
                    className="badge text-bg-secondary px-2 py-1",
                    style={"fontSize": "0.78rem"},
                )
            ),
            html.Td(status_badge),
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
    return rows


def page_management():
    projects_df = db_adapter.get_projects_list()
    rows = _build_management_table_rows(projects_df)

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-layers text-primary"),
                "Projects & Dataset Management"
            ], className="page-main-title"),
            dbc.Button([
                html.I(className="bi bi-plus-lg"),
                "Add Project"
            ], id="btn-open-add-project", className="btn-primary-action"),
        ], className="page-header-bar"),

        # Projects Table Card — tbody uses id="management-table-body" so the
        # refresh callback can update it in-place after Add / Edit / Toggle.
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Project Name"),
                    html.Th("Description"),
                    html.Th("Project Owner"),
                    html.Th("Datasets"),
                    html.Th("Status"),
                    html.Th("Actions"),
                ])),
                html.Tbody(rows, id="management-table-body"),
            ], className="clean-table"),
        ], className="dq-card mb-4"),

        # Add Project Modal (Matching Screen Images)
        dbc.Modal([
            dbc.ModalHeader([
                html.Div([
                    html.H5("Create New Project", className="fw-bold mb-0 text-dark"),
                ]),
                dbc.Button("Example", id="btn-autofill-example", size="sm", color="primary", outline=True, className="ms-auto rounded-pill px-3 py-1", style={"fontSize": "0.8rem", "fontWeight": "600"}),
            ], close_button=True),
            dbc.ModalBody([
                dbc.Form([
                    # Section 1: PROJECT DETAILS
                    html.Div("PROJECT DETAILS", className="fw-bold text-muted small mb-3", style={"letterSpacing": "0.05em"}),
                    html.Div([
                        dbc.Label("Project Name *", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-name", placeholder="e.g. Banking", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Description", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-desc", placeholder="Customer transaction data quality", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Project Owner", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-owner", placeholder="Team or owner name", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Business Domain", className="small fw-semibold text-secondary mb-1"),
                        dbc.Select(
                            id="new-project-domain",
                            options=[
                                {"label": "Banking", "value": "Banking"},
                                {"label": "Healthcare", "value": "Healthcare"},
                                {"label": "Logistics & Supply Chain", "value": "Logistics"},
                                {"label": "Sales & CRM", "value": "Sales"},
                                {"label": "Retail & E-Commerce", "value": "Retail"},
                                {"label": "Finance & Billing", "value": "Finance"},
                            ],
                            value="Banking",
                            className="rounded-pill px-3 py-2",
                        ),
                    ], className="mb-4"),

                    # Section 2: DATASET DETAILS
                    html.Div("DATASET DETAILS", className="fw-bold text-muted small mb-3", style={"letterSpacing": "0.05em"}),
                    html.Div([
                        dbc.Label("Source Table *", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-source-table", placeholder="catalog.schema.source_table", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Target Table", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-target-table", placeholder="catalog.schema.target_table", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),
                    html.Div([
                        dbc.Label("Business Key", className="small fw-semibold text-secondary mb-1"),
                        dbc.Input(id="new-project-business-key", placeholder="e.g. customer_id", type="text", className="rounded-pill px-3 py-2"),
                    ], className="mb-3"),

                    # Test Connectivity action
                    html.Div([
                        dbc.Button([
                            html.I(className="bi bi-check2-circle me-1"),
                            "Test Connectivity"
                        ], id="btn-test-connectivity", color="outline-secondary", size="sm", className="rounded-pill px-3"),
                        html.Div(id="connectivity-test-alert", className="mt-2"),
                    ], className="mb-3"),

                    html.Div(id="add-project-result-alert"),
                ]),
            ]),
            dbc.ModalFooter([
                dbc.Button("Cancel", id="btn-cancel-add-project", color="light", className="rounded-pill px-4"),
                dbc.Button("Create Project", id="btn-submit-add-project", color="primary", className="rounded-pill px-4 fw-semibold"),
            ]),
        ], id="modal-add-project", is_open=False, size="lg"),

        # View Project Details Modal
        dbc.Modal([
            dbc.ModalHeader([
                html.H5("Project Details", id="modal-view-project-title", className="fw-bold mb-0 text-dark"),
            ], close_button=True),
            dbc.ModalBody(id="modal-view-project-body"),
            dbc.ModalFooter([
                dbc.Button("Close", id="btn-close-view-project", color="light", className="rounded-pill px-3"),
                dbc.Button([
                    html.I(className="bi bi-shield-check me-1"),
                    "Configure DQ Rules"
                ], href="/rules", color="primary", className="rounded-pill px-3 fw-semibold"),
            ]),
        ], id="modal-view-project", is_open=False, size="lg"),

        # Edit Project Modal
        dbc.Modal([
            dbc.ModalHeader([
                html.H5("Edit Project Configuration", className="fw-bold mb-0 text-dark"),
            ], close_button=True),
            dbc.ModalBody([
                dcc.Store(id="edit-project-id"),
                dcc.Store(id="edit-dataset-id"),
                html.Div([
                    dbc.Label("Project Name", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-name", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Description", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-desc", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Project Owner", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-owner", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Source Table", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-source", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Target Table", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-target", type="text", className="mb-3 rounded-pill px-3"),
                    dbc.Label("Business Key", className="fw-semibold small text-secondary"),
                    dbc.Input(id="edit-project-business-key", type="text", className="mb-3 rounded-pill px-3"),
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

def page_rules():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    datasets = db_adapter.get_datasets_for_project("Hospital")
    
    # Template Cards matching left panel of Screen 3
    template_tiles = []
    icon_map = {
        "NULL_COUNT": ("bi bi-check-circle-fill", "#2563EB", "#EFF6FF"),
        "NULL_PERCENT": ("bi bi-percent", "#9333EA", "#FAF5FF"),
        "UNIQUE_COUNT": ("bi bi-shield-check", "#10B981", "#DCFCE7"),
        "DUPLICATE_COUNT": ("bi bi-copy", "#F59E0B", "#FEF3C7"),
        "ROW_COUNT": ("bi bi-list-ol", "#06B6D4", "#ECFEFF"),
        "BLANK_COUNT": ("bi bi-dash-circle", "#64748B", "#F1F5F9"),
        "DATA_FRESHNESS": ("bi bi-clock-history", "#F97316", "#FFF7ED"),
        "REFERENTIAL_INTEGRITY": ("bi bi-diagram-3-fill", "#8B5CF6", "#F5F3FF"),
        "SCHEMA_CHANGE": ("bi bi-gear-fill", "#3B82F6", "#EFF6FF"),
        "REQUIRED_COLUMNS": ("bi bi-ui-checks", "#10B981", "#DCFCE7"),
        "SOURCE_TARGET_ROW_COUNT": ("bi bi-arrow-left-right", "#EC4899", "#FDF2F8"),
    }
    
    for t_name, t_info in RULE_TEMPLATES.items():
        icon, fg, bg = icon_map.get(t_name, ("bi bi-check-circle", "#2563EB", "#EFF6FF"))
        template_tiles.append(
            html.Div([
                html.Div(
                    html.I(className=icon, style={"color": fg}),
                    style={"width": "34px", "height": "34px", "borderRadius": "8px", "backgroundColor": bg, "display": "flex", "alignItems": "center", "justifyContent": "center", "fontSize": "1.1rem"}
                ),
                html.Div([
                    html.Div(t_name, style={"fontWeight": "600", "fontSize": "0.85rem", "color": "#0F172A"}),
                    html.Div(t_info["description"], style={"fontSize": "0.75rem", "color": "#64748B"}),
                ]),
            ], id={"type": "template-tile", "id": t_name}, className="template-tile", n_clicks=0)
        )

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
                    value="Hospital",
                    style={"width": "160px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="rule-select-dataset",
                    options=[{"label": d, "value": d} for d in datasets],
                    value="source_table",
                    style={"width": "180px"},
                    className="me-3",
                ),
                dbc.Button([
                    html.I(className="bi bi-plus-lg"),
                    "Add Rule"
                ], id="btn-add-rule-trigger", className="btn-primary-action"),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),

        # 2-Column Layout matching Screen 3
        dbc.Row([
            # Left: Available Rule Templates
            dbc.Col([
                html.Div([
                    html.Div("Available Rule Templates", className="dq-card-title"),
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-search")),
                        dbc.Input(id="input-search-template", placeholder="Search rules...", type="text"),
                    ], className="mb-3"),
                    html.Div(template_tiles, id="template-tiles-container", style={"maxHeight": "520px", "overflowY": "auto"}),
                ], className="dq-card"),
            ], lg=5, className="mb-4"),

            # Right: Rule Details Form
            dbc.Col([
                html.Div([
                    html.Div("Rule Details", className="dq-card-title"),
                    dbc.Form([
                        html.Div([
                            dbc.Label("Rule Name", className="fw-semibold"),
                            dbc.Input(id="rule-input-name", value="NULL_COUNT", type="text"),
                        ], className="mb-3"),
                        html.Div([
                            dbc.Label("Column", className="fw-semibold"),
                            dbc.Input(id="rule-input-column", value="patient_name", type="text"),
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
    ])

# -------------------------------------------------------------
# PAGE 4: EXECUTION & MONITORING
# -------------------------------------------------------------

def page_execution():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    datasets = db_adapter.get_datasets_for_project("Hospital")

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-play-circle text-primary"),
                "Execution & Monitoring"
            ], className="page-main-title"),
            html.Div([
                dbc.Select(
                    id="exec-select-project",
                    options=[{"label": p, "value": p} for p in projects],
                    value="Hospital",
                    style={"width": "160px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="exec-select-dataset",
                    options=[{"label": d, "value": d} for d in datasets],
                    value="source_table",
                    style={"width": "180px"},
                    className="me-3",
                ),
                dbc.Button([
                    html.I(className="bi bi-play-fill"),
                    "Run Validation"
                ], id="btn-trigger-validation", className="btn-primary-action"),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),

        # Phase 7: Databricks Workflow Job Automation Status Banner
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.I(className="bi bi-cpu-fill text-primary me-2 fs-5"),
                        html.Span(f"Databricks Workflow Job #{db_adapter.DQ_JOB_ID}", style={"fontWeight": "700", "fontSize": "0.95rem"}),
                    ]),
                    html.Div("Automated multi-task orchestrator with retry policy (2 retries, 60s backoff) & schedule trigger.", className="text-muted small"),
                ], md=7),
                dbc.Col([
                    html.Div([
                        html.Span("Trigger: ", className="text-muted small"),
                        html.Span("Scheduled & Manual", className="badge-pass me-2", style={"fontSize": "0.75rem"}),
                        html.Span("Schedule: ", className="text-muted small"),
                        html.Span("Quartz Cron (Configurable)", className="badge-active", style={"fontSize": "0.75rem"}),
                    ], className="text-md-end pt-1"),
                ], md=5),
            ], className="align-items-center"),
        ], className="p-3 mb-3 rounded bg-white border shadow-sm"),

        # Pipeline Stepper Card matching Screen 4
        html.Div([
            html.Div([
                html.Div(className="stepper-line"),
                html.Div(id="stepper-progress-line", className="stepper-line-active"),
                
                # Step 1: Initializing
                html.Div([
                    html.Div(html.I(className="bi bi-check-lg"), className="step-circle done"),
                    html.Div("Initializing", className="step-label"),
                ], className="step-node"),

                # Step 2: Running Rules
                html.Div([
                    html.Div(html.I(className="bi bi-arrow-repeat"), className="step-circle active"),
                    html.Div("Running Rules (3/6)", className="step-label active"),
                ], className="step-node"),

                # Step 3: Finalizing
                html.Div([
                    html.Div(html.I(className="bi bi-circle"), className="step-circle"),
                    html.Div("Finalizing", className="step-label"),
                ], className="step-node"),

                # Step 4: Completed
                html.Div([
                    html.Div(html.I(className="bi bi-check"), className="step-circle"),
                    html.Div("Completed", className="step-label"),
                ], className="step-node"),
            ], className="stepper-container"),

            # Meta strip
            dbc.Row([
                dbc.Col([
                    html.Div("Execution ID", className="text-muted small"),
                    html.Div(id="exec-meta-id", children="EXEC-20261101-001", style={"fontWeight": "600", "color": "#0F172A"}),
                ], xs=4),
                dbc.Col([
                    html.Div("Start Time", className="text-muted small"),
                    html.Div(id="exec-meta-start", children="01 Nov 2026, 10:30 AM", style={"fontWeight": "600", "color": "#0F172A"}),
                ], xs=4),
                dbc.Col([
                    html.Div("Duration", className="text-muted small"),
                    html.Div(id="exec-meta-duration", children="2m 15s", style={"fontWeight": "600", "color": "#0F172A"}),
                ], xs=4),
            ], className="text-center pt-3 border-top"),
        ], className="dq-card mb-4"),

        # Bottom Section: Results Cards (Left) & Live Progress Checklist (Right)
        dbc.Row([
            # Left 3 Cards
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.Div([
                            html.I(className="bi bi-check-circle-fill text-success fs-3 mb-2"),
                            html.Div("PASS", className="small fw-semibold text-muted"),
                            html.Div(id="exec-count-pass", children="4", className="fs-2 fw-bold text-success"),
                        ], className="dq-card text-center", style={"backgroundColor": "#F0FDF4", "borderColor": "#DCFCE7"}),
                    ], xs=4),
                    dbc.Col([
                        html.Div([
                            html.I(className="bi bi-x-circle-fill text-danger fs-3 mb-2"),
                            html.Div("FAIL", className="small fw-semibold text-muted"),
                            html.Div(id="exec-count-fail", children="1", className="fs-2 fw-bold text-danger"),
                        ], className="dq-card text-center", style={"backgroundColor": "#FEF2F2", "borderColor": "#FEE2E2"}),
                    ], xs=4),
                    dbc.Col([
                        html.Div([
                            html.I(className="bi bi-exclamation-triangle-fill text-warning fs-3 mb-2"),
                            html.Div("ERROR", className="small fw-semibold text-muted"),
                            html.Div(id="exec-count-error", children="1", className="fs-2 fw-bold text-warning"),
                        ], className="dq-card text-center", style={"backgroundColor": "#FFFBEB", "borderColor": "#FEF3C7"}),
                    ], xs=4),
                ], className="g-2"),
            ], lg=6, className="mb-4"),

            # Right Live Progress Checklist
            dbc.Col([
                html.Div([
                    html.Div("Live Progress", className="dq-card-title"),
                    html.Div(id="exec-live-progress-list", children=[
                        html.Div([
                            html.I(className="bi bi-check-circle-fill text-success me-2"),
                            html.Span("NULL_COUNT - PASS", className="fw-semibold"),
                        ], className="mb-2 p-2 rounded", style={"backgroundColor": "#F0FDF4"}),
                        html.Div([
                            html.I(className="bi bi-x-circle-fill text-danger me-2"),
                            html.Span("AGE_NON_NEGATIVE - FAIL", className="fw-semibold"),
                        ], className="mb-2 p-2 rounded", style={"backgroundColor": "#FEF2F2"}),
                        html.Div([
                            html.I(className="bi bi-check-circle-fill text-success me-2"),
                            html.Span("DATA_FRESHNESS - PASS", className="fw-semibold"),
                        ], className="mb-2 p-2 rounded", style={"backgroundColor": "#F0FDF4"}),
                        html.Div([
                            html.I(className="bi bi-exclamation-triangle-fill text-warning me-2"),
                            html.Span("REQUIRED_COLUMNS - ERROR", className="fw-semibold"),
                        ], className="p-2 rounded", style={"backgroundColor": "#FFFBEB"}),
                    ]),
                ], className="dq-card"),
            ], lg=6, className="mb-4"),
        ], className="g-3"),
    ])

# -------------------------------------------------------------
# PAGE 5: DATA QUALITY RESULTS
# -------------------------------------------------------------

def page_results():
    projects = db_adapter.get_projects_list()["project_name"].tolist()
    datasets = db_adapter.get_datasets_for_project("Hospital")
    results_df = db_adapter.get_dq_results_table()

    table_rows = []
    for _, r in results_df.iterrows():
        s = str(r["status"])
        badge_cls = "badge-pass" if s == "PASS" else ("badge-fail" if s == "FAIL" else "badge-error")
        table_rows.append(html.Tr([
            html.Td(r["rule_name"], style={"fontWeight": "600"}),
            html.Td(r["category"]),
            html.Td(html.Span(s, className=badge_cls)),
            html.Td(str(r["failure_count"])),
            html.Td(str(r["observed_value"] or "-")),
            html.Td(str(r["expected_value"] or "-")),
            html.Td(str(r["executed_at"])),
        ]))

    return html.Div([
        # Header Bar with Filters & Export Button
        html.Div([
            html.H3([
                html.I(className="bi bi-check2-square text-primary"),
                "DQ Results"
            ], className="page-main-title"),
            html.Div([
                dbc.Select(
                    id="filter-results-project",
                    options=[{"label": p, "value": p} for p in projects],
                    value="Hospital",
                    style={"width": "140px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="filter-results-dataset",
                    options=[{"label": d, "value": d} for d in datasets],
                    value="source_table",
                    style={"width": "160px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="filter-results-status",
                    options=[
                        {"label": "Status: All", "value": "All"},
                        {"label": "PASS", "value": "PASS"},
                        {"label": "FAIL", "value": "FAIL"},
                        {"label": "ERROR", "value": "ERROR"},
                    ],
                    value="All",
                    style={"width": "130px"},
                    className="me-2",
                ),
                dbc.Select(
                    id="filter-results-days",
                    options=[
                        {"label": "Last 7 Days", "value": "7"},
                        {"label": "Last 30 Days", "value": "30"},
                    ],
                    value="7",
                    style={"width": "140px"},
                    className="me-3",
                ),
                dbc.Button([
                    html.I(className="bi bi-download"),
                    "Export CSV"
                ], id="btn-export-results-csv", className="btn-primary-action"),
            ], className="d-flex align-items-center"),
        ], className="page-header-bar"),

        # Results Table Card
        html.Div([
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Rule Name"),
                    html.Th("Category"),
                    html.Th("Status"),
                    html.Th("Failure Count"),
                    html.Th("Observed Value"),
                    html.Th("Expected Value"),
                    html.Th("Executed At"),
                ])),
                html.Tbody(id="results-table-body", children=table_rows),
            ], className="clean-table"),
        ], className="dq-card mb-4"),
    ])

# -------------------------------------------------------------
# PAGE 6: SOURCE-TO-TARGET RECONCILIATION
# -------------------------------------------------------------

def page_reconciliation():
    recon = db_adapter.get_reconciliation_data()

    # 4 Top KPI Cards matching Screen 6
    kpi_cards = [
        ("Source Count", recon["source_count"], "#EFF6FF", "#1E293B"),
        ("Target Count", recon["target_count"], "#EFF6FF", "#1E293B"),
        ("Difference", recon["difference"], "#EFF6FF", "#1E293B"),
        ("Status", recon["status"], "#FEE2E2", "#DC2626"),
    ]

    cols = [
        dbc.Col(
            html.Div([
                html.Div(title, className="small text-muted fw-semibold mb-1"),
                html.Div(val, style={"fontSize": "1.75rem", "fontWeight": "700", "color": fg}),
            ], className="dq-card text-center", style={"backgroundColor": bg}),
            xs=6, md=3, className="mb-3",
        )
        for title, val, bg, fg in kpi_cards
    ]

    # Sample missing rows
    samples_df = recon["samples"]
    sample_rows = []
    for _, row in samples_df.iterrows():
        sample_rows.append(html.Tr([
            html.Td(str(row["patient_id"])),
            html.Td(str(row["patient_name"])),
        ]))

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-arrow-left-right text-primary"),
                "Source-to-Target Reconciliation"
            ], className="page-main-title"),
        ], className="page-header-bar"),

        # Top 4 KPI Cards
        dbc.Row(cols, className="g-3 mb-4"),

        # Bottom 2 Columns: Validation Summary & Missing Records Sample
        dbc.Row([
            # Validation Summary
            dbc.Col([
                html.Div([
                    html.Div("Validation Summary", className="dq-card-title"),
                    html.Div([
                        html.Div([
                            html.I(className="bi bi-check-circle-fill text-success me-2"),
                            html.Span("Matching Records", className="fw-semibold"),
                            html.Span(recon["matching"], className="float-end fw-bold"),
                        ], className="p-3 mb-2 rounded bg-light"),
                        html.Div([
                            html.I(className="bi bi-x-circle-fill text-danger me-2"),
                            html.Span("Missing in Target", className="fw-semibold"),
                            html.Span(recon["missing"], className="float-end fw-bold text-danger"),
                        ], className="p-3 mb-2 rounded bg-light"),
                        html.Div([
                            html.I(className="bi bi-check-circle-fill text-success me-2"),
                            html.Span("Extra in Target", className="fw-semibold"),
                            html.Span(recon["extra"], className="float-end fw-bold"),
                        ], className="p-3 mb-2 rounded bg-light"),
                        html.Div([
                            html.I(className="bi bi-check-circle-fill text-success me-2"),
                            html.Span("Duplicate Keys", className="fw-semibold"),
                            html.Span(recon["duplicate"], className="float-end fw-bold"),
                        ], className="p-3 rounded bg-light"),
                    ]),
                ], className="dq-card"),
            ], lg=6, className="mb-4"),

            # Missing Records (Sample)
            dbc.Col([
                html.Div([
                    html.Div([
                        html.Span("Missing Records (Sample)"),
                        html.A("View All ->", href="#", id="btn-view-all-missing", style={"fontSize": "0.85rem", "fontWeight": "600", "color": "#2563EB", "textDecoration": "none"}),
                    ], className="dq-card-title"),
                    html.Table([
                        html.Thead(html.Tr([
                            html.Th("patient_id"),
                            html.Th("patient_name"),
                        ])),
                        html.Tbody(sample_rows),
                    ], className="clean-table"),
                ], className="dq-card"),
            ], lg=6, className="mb-4"),
        ], className="g-3"),
    ])

# -------------------------------------------------------------
# PAGE 7: ALERTS & REPORTS (PHASE 6 ENHANCED)
# -------------------------------------------------------------

def page_alerts():
    alerts_data = db_adapter.get_alerts_data()
    alerts_df = alerts_data["alerts_table"]

    # Top KPI cards
    kpi_cards = [
        ("Critical Alerts", alerts_data["critical_count"], "bi bi-exclamation-triangle-fill", "#FEE2E2", "#DC2626"),
        ("High Alerts", alerts_data["high_count"], "bi bi-exclamation-circle-fill", "#FFEDD5", "#EA580C"),
        ("Active Alerts", alerts_data["active_count"], "bi bi-bell-fill", "#EFF6FF", "#2563EB"),
        ("Total Alerts", alerts_data["total_count"], "bi bi-shield-check", "#F1F5F9", "#475569"),
    ]

    kpi_cols = [
        dbc.Col(
            html.Div([
                html.Div([
                    html.Div(title, className="metric-title"),
                    html.Div(str(val), className="metric-val"),
                ]),
                html.Div(
                    html.I(className=icon, style={"color": fg}),
                    className="metric-icon-box",
                    style={"backgroundColor": bg},
                ),
            ], className="metric-pill-card"),
            xs=12, sm=6, md=3, className="mb-3",
        )
        for title, val, icon, bg, fg in kpi_cards
    ]

    # Alerts by Severity Bar Chart
    sev_counts = [alerts_data["critical_count"], alerts_data["high_count"], alerts_data["medium_count"], alerts_data["low_count"]]
    sev_labels = ["Critical", "High", "Medium", "Low"]
    fig_bar = go.Figure(data=[go.Bar(
        x=sev_labels,
        y=sev_counts,
        text=sev_counts,
        textposition="outside",
        marker_color=["#EF4444", "#F97316", "#F59E0B", "#10B981"],
    )])
    fig_bar.update_layout(
        margin=dict(l=10, r=10, t=25, b=25),
        height=160,
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        yaxis=dict(visible=False, range=[0, max(sev_counts + [3]) + 1]),
        xaxis=dict(tickfont=dict(size=12, color="#475569")),
    )

    # Alerts table rows
    alert_rows = []
    for _, a in alerts_df.iterrows():
        a_id = int(a["alert_id"])
        sev = str(a["severity"])
        st = str(a["status"])
        
        sev_color = "#EF4444" if sev == "Critical" else ("#F97316" if sev == "High" else ("#F59E0B" if sev == "Medium" else "#10B981"))
        badge_style = {"backgroundColor": sev_color + "22", "color": sev_color, "fontWeight": "600", "padding": "4px 8px", "borderRadius": "12px", "fontSize": "0.75rem"}
        
        actions = []
        if st == "Active":
            actions.append(dbc.Button("Acknowledge", id={"type": "btn-ack-alert", "id": a_id}, size="sm", color="primary", outline=True, style={"fontSize": "0.75rem", "padding": "2px 8px"}, className="me-1"))
            actions.append(dbc.Button("Close", id={"type": "btn-close-alert", "id": a_id}, size="sm", color="danger", outline=True, style={"fontSize": "0.75rem", "padding": "2px 8px"}))
        elif st == "Acknowledged":
            actions.append(dbc.Button("Close", id={"type": "btn-close-alert", "id": a_id}, size="sm", color="danger", outline=True, style={"fontSize": "0.75rem", "padding": "2px 8px"}, className="me-1"))
            actions.append(html.Span(f"Ack by {a.get('acknowledged_by') or 'admin'}", className="text-muted small"))
        else:
            actions.append(html.Span(f"Closed by {a.get('closed_by') or 'admin'}", className="text-muted small"))

        alert_rows.append(html.Tr([
            html.Td(a["date_time"]),
            html.Td(a["project_name"], style={"fontWeight": "500"}),
            html.Td(a["rule_name"]),
            html.Td(html.Span(sev, style=badge_style)),
            html.Td(str(a.get("failure_count", 0))),
            html.Td(html.Span(st, className="badge-pass" if st == "Closed" else ("badge-fail" if st == "Active" else "badge-error"))),
            html.Td(actions),
        ]))

    return html.Div([
        # Header Bar
        html.Div([
            html.H3([
                html.I(className="bi bi-bell text-primary"),
                "Alerts & Reports"
            ], className="page-main-title"),
        ], className="page-header-bar"),

        # Top Row: 4 KPI Cards
        dbc.Row(kpi_cols, className="g-3 mb-2"),

        # Middle Row: Severity Bar Chart & Report Downloader
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.Div("Alerts by Severity", className="dq-card-title", style={"marginBottom": "8px"}),
                    dcc.Graph(figure=fig_bar, config={"displayModeBar": False}),
                ], className="dq-card"),
                lg=5, className="mb-4",
            ),
            dbc.Col(
                html.Div([
                    html.Div("Download Standard Reports (CSV)", className="dq-card-title"),
                    html.P("Export aggregate or detailed data quality intelligence across projects, datasets, and rules.", className="text-muted small mb-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Select Report Type", className="fw-semibold small"),
                            dbc.Select(
                                id="select-report-type",
                                options=[
                                    {"label": "Project-wise Summary Report", "value": "PROJECT_WISE"},
                                    {"label": "Dataset-wise Definitions & Health", "value": "DATASET_WISE"},
                                    {"label": "Rule-wise Detailed Results", "value": "RULE_WISE"},
                                    {"label": "Execution-wise Run History", "value": "EXECUTION_WISE"},
                                ],
                                value="PROJECT_WISE",
                                className="mb-3",
                            ),
                        ], md=8),
                        dbc.Col([
                            dbc.Button([
                                html.I(className="bi bi-download me-2"),
                                "Download CSV"
                            ], id="btn-download-selected-report", color="primary", className="w-100 fw-semibold", style={"marginTop": "28px"}),
                        ], md=4),
                    ]),
                ], className="dq-card"),
                lg=7, className="mb-4",
            ),
        ], className="g-3"),

        # Recent Alerts Table Card
        html.Div([
            html.Div([
                html.Span("Alerts Lifecycle & Resolution"),
                html.Div([
                    dbc.Select(
                        id="filter-alert-status",
                        options=[
                            {"label": "Status: All", "value": "All"},
                            {"label": "Active", "value": "Active"},
                            {"label": "Acknowledged", "value": "Acknowledged"},
                            {"label": "Closed", "value": "Closed"},
                        ],
                        value="All",
                        style={"width": "150px"},
                        size="sm",
                        className="d-inline-block me-2",
                    ),
                    dbc.Select(
                        id="filter-alert-severity",
                        options=[
                            {"label": "Severity: All", "value": "All"},
                            {"label": "Critical", "value": "Critical"},
                            {"label": "High", "value": "High"},
                            {"label": "Medium", "value": "Medium"},
                            {"label": "Low", "value": "Low"},
                        ],
                        value="All",
                        style={"width": "150px"},
                        size="sm",
                        className="d-inline-block",
                    ),
                ], className="d-flex"),
            ], className="dq-card-title"),
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Date & Time"),
                    html.Th("Project"),
                    html.Th("Rule"),
                    html.Th("Severity"),
                    html.Th("Failures"),
                    html.Th("Status"),
                    html.Th("Actions"),
                ])),
                html.Tbody(id="alerts-table-body", children=alert_rows),
            ], className="clean-table"),
        ], className="dq-card mb-4"),
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

# -------------------------------------------------------------
# ROUTER CALLBACK
# -------------------------------------------------------------

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
)
def render_page(pathname):
    page = (pathname or "/overview").strip("/").lower()
    if page == "management":
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
     Output("new-project-domain", "value"),
     Output("new-project-source-table", "value"),
     Output("new-project-target-table", "value"),
     Output("new-project-business-key", "value")],
    Input("btn-autofill-example", "n_clicks"),
    prevent_initial_call=True,
)
def handle_autofill_example(n_clicks):
    return (
        "Banking",
        "Customer transaction data quality",
        "Data Engineering Team",
        "Banking",
        "banking_catalog.finance.customer_source",
        "banking_catalog.finance.customer_target",
        "customer_id",
    )

# 2. Test Table Connectivity
@app.callback(
    Output("connectivity-test-alert", "children"),
    Input("btn-test-connectivity", "n_clicks"),
    State("new-project-source-table", "value"),
    prevent_initial_call=True,
)
def handle_test_connectivity(n_clicks, src_table):
    if not src_table or not src_table.strip():
        return dbc.Alert("Please provide a Source Table before testing connectivity.", color="warning", className="small py-2 mb-0")
    
    res = db_adapter.test_table_connectivity(src_table.strip())
    if res["success"]:
        return dbc.Alert([
            html.I(className="bi bi-check-circle-fill me-2"),
            res["message"]
        ], color="success", className="small py-2 mb-0")
    else:
        return dbc.Alert([
            html.I(className="bi bi-x-circle-fill me-2"),
            res["message"]
        ], color="danger", className="small py-2 mb-0")

# 3. Add Project & Dataset Registration
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
     State("new-project-domain", "value"),
     State("new-project-source-table", "value"),
     State("new-project-target-table", "value"),
     State("new-project-business-key", "value")],
    prevent_initial_call=True,
)
def handle_add_project(open_click, cancel_click, submit_click, is_open, name, desc, owner, domain, src_tbl, tgt_tbl, b_key):
    trig = ctx.triggered_id
    _ts = datetime.now().isoformat()
    if trig == "btn-open-add-project":
        return True, None, False, "", "info", dash.no_update
    elif trig == "btn-cancel-add-project":
        return False, None, False, "", "info", dash.no_update
    elif trig == "btn-submit-add-project":
        res = db_adapter.add_new_project_with_dataset(
            project_name=name,
            description=desc or "",
            owner=owner or "Data Engineering Team",
            domain=domain or "Banking",
            source_table=src_tbl or "",
            target_table=tgt_tbl or src_tbl,
            business_key=b_key or "id",
            user="admin"
        )
        if res["success"]:
            success_content = dbc.Alert([
                html.Div([
                    html.I(className="bi bi-check-circle-fill me-2 text-success fs-5"),
                    html.Span("Project Registered Successfully!", className="fw-bold fs-6"),
                ], className="d-flex align-items-center mb-2"),
                html.P(f"Project '{res['project_name']}' (ID: {res['project_id']}) and dataset '{res['dataset_name']}' (ID: {res['dataset_id']}) are now active.", className="small mb-3"),
                dbc.Button([
                    html.I(className="bi bi-arrow-right-circle me-1"),
                    "Configure DQ Rules"
                ], href="/rules", color="success", size="sm", className="rounded-pill px-3 fw-semibold"),
            ], color="success", className="mb-0")
            # Close modal, show success, refresh table
            return False, success_content, True, f"Project '{name}' registered successfully!", "success", _ts
        else:
            err_content = dbc.Alert([
                html.I(className="bi bi-exclamation-triangle-fill me-2"),
                res["message"]
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
    if trig == "btn-close-view-project":
        return False, "Project Details", None
    
    if isinstance(trig, dict) and trig.get("type") == "btn-view-proj":
        p_id = trig["id"]
        details = db_adapter.get_project_details(p_id)
        if not details:
            return False, "Project Details", None
        
        proj = details["project"]
        datasets = details["datasets"]
        rules = details["rules"]
        last_exec = details["last_execution"]
        avg_score = details["avg_dq_score"]
        p_name = proj["project_name"]

        # Datasets Table Rows
        ds_rows = []
        for d in datasets:
            ds_rows.append(html.Tr([
                html.Td(d["dataset_name"], style={"fontWeight": "600"}),
                html.Td(html.Code(d["source_table"])),
                html.Td(html.Code(d["target_table"] or "—")),
                html.Td(html.Badge(d["business_key"] or "id", color="secondary", className="px-2 py-1")),
                html.Td(html.Span("Active", className="badge-active") if d["active"] else html.Span("Inactive", className="badge-inactive")),
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

            # Execution History Section
            html.H6("Recent Execution Status", className="fw-bold text-dark mb-2"),
            exec_widget,

            # Registered Datasets Section
            html.H6(f"Registered Datasets ({len(datasets)})", className="fw-bold text-dark mb-2"),
            html.Table([
                html.Thead(html.Tr([
                    html.Th("Dataset"),
                    html.Th("Source Table"),
                    html.Th("Target Table"),
                    html.Th("Business Key"),
                    html.Th("Status"),
                ])),
                html.Tbody(ds_rows if ds_rows else [html.Tr([html.Td("No datasets registered", colSpan=5, className="text-center text-muted")])]),
            ], className="clean-table mb-4"),

            # Configured DQ Rules Section
            html.H6(f"Active DQ Rules ({len(rules)})", className="fw-bold text-dark mb-2"),
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
        ])

        return True, f"Project Overview: {p_name}", body

    return is_open, "Project Details", None

# 5. Edit Project Configuration Action
@app.callback(
    [Output("modal-edit-project", "is_open"),
     Output("edit-project-id", "data"),
     Output("edit-dataset-id", "data"),
     Output("edit-project-name", "value"),
     Output("edit-project-desc", "value"),
     Output("edit-project-owner", "value"),
     Output("edit-project-source", "value"),
     Output("edit-project-target", "value"),
     Output("edit-project-business-key", "value"),
     Output("project-refresh-store", "data", allow_duplicate=True)],
    [Input({"type": "btn-edit-proj", "id": dash.ALL}, "n_clicks"),
     Input("btn-cancel-edit-project", "n_clicks"),
     Input("btn-submit-edit-project", "n_clicks")],
    [State("modal-edit-project", "is_open"),
     State("edit-project-id", "data"),
     State("edit-dataset-id", "data"),
     State("edit-project-name", "value"),
     State("edit-project-desc", "value"),
     State("edit-project-owner", "value"),
     State("edit-project-source", "value"),
     State("edit-project-target", "value"),
     State("edit-project-business-key", "value")],
    prevent_initial_call=True,
)
def handle_edit_project(edit_clicks, cancel_click, submit_click, is_open, cur_p_id, cur_d_id, p_name, p_desc, p_owner, p_src, p_tgt, p_key):
    trig = ctx.triggered_id
    _ts = datetime.now().isoformat()
    if trig == "btn-cancel-edit-project":
        return False, None, None, "", "", "", "", "", "", dash.no_update
    elif trig == "btn-submit-edit-project":
        if cur_p_id and p_name:
            db_adapter.update_project_and_dataset(
                project_id=int(cur_p_id),
                project_name=p_name.strip(),
                description=p_desc or "",
                owner=p_owner or "Data Engineering Team",
                dataset_id=int(cur_d_id) if cur_d_id else None,
                source_table=p_src,
                target_table=p_tgt,
                business_key=p_key,
                user="admin"
            )
        # Refresh table after saving edits
        return False, None, None, "", "", "", "", "", "", _ts
    elif isinstance(trig, dict) and trig.get("type") == "btn-edit-proj":
        p_id = trig["id"]
        details = db_adapter.get_project_details(p_id)
        if not details:
            return False, None, None, "", "", "", "", "", "", dash.no_update

        proj = details["project"]
        ds_list = details["datasets"]
        d = ds_list[0] if ds_list else {}

        return (
            True,
            proj["project_id"],
            d.get("dataset_id"),
            proj["project_name"],
            proj["description"] or "",
            proj.get("owner") or "Data Engineering Team",
            d.get("source_table") or "",
            d.get("target_table") or "",
            d.get("business_key") or "id",
            dash.no_update,
        )
    return is_open, None, None, "", "", "", "", "", "", dash.no_update

# 2. Toggle Project Active Status & Alert Lifecycle
@app.callback(
    [Output("global-toast", "is_open"),
     Output("global-toast", "children"),
     Output("global-toast", "icon"),
     Output("project-refresh-store", "data", allow_duplicate=True)],
    [Input({"type": "btn-toggle-proj", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-ack-alert", "id": dash.ALL}, "n_clicks"),
     Input({"type": "btn-close-alert", "id": dash.ALL}, "n_clicks")],
    prevent_initial_call=True,
)
def handle_table_actions(toggle_clicks, ack_clicks, close_clicks):
    trig = ctx.triggered_id
    _ts = datetime.now().isoformat()
    if not trig:
        return False, "", "info", dash.no_update

    if isinstance(trig, dict) and trig.get("type") == "btn-toggle-proj":
        p_id = trig["id"]
        db_adapter.toggle_project(p_id)
        # Refresh table so Enable/Disable label flips immediately
        return True, f"Project ID {p_id} status updated successfully.", "success", _ts

    if isinstance(trig, dict) and trig.get("type") == "btn-ack-alert":
        a_id = trig["id"]
        db_adapter.acknowledge_alert(a_id, user="admin")
        return True, f"Alert #{a_id} marked as Acknowledged.", "info", dash.no_update

    if isinstance(trig, dict) and trig.get("type") == "btn-close-alert":
        a_id = trig["id"]
        db_adapter.close_alert(a_id, user="admin")
        return True, f"Alert #{a_id} marked as Closed.", "success", dash.no_update

    return False, "", "info", dash.no_update

# 2b. Live refresh of the management projects table body
# Triggered whenever project-refresh-store is updated (after Add / Edit / Toggle).
# Only active when the management page is currently displayed.
@app.callback(
    Output("management-table-body", "children"),
    Input("project-refresh-store", "data"),
    prevent_initial_call=True,
)
def refresh_management_table(refresh_ts):
    """Re-fetch projects from DB and rebuild the table rows in-place."""
    projects_df = db_adapter.get_projects_list()
    return _build_management_table_rows(projects_df)

# 3. Rule Template Selection & SQL Preview
@app.callback(
    [Output("rule-input-name", "value"),
     Output("rule-input-column", "value"),
     Output("rule-input-threshold", "value")],
    Input({"type": "template-tile", "id": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_template(clicks):
    trig = ctx.triggered_id
    if trig and isinstance(trig, dict):
        t_id = trig["id"]
        tmpl = RULE_TEMPLATES.get(t_id, {})
        col = "patient_name" if tmpl.get("requires_column") else ""
        thresh = str(tmpl.get("default_threshold", 0))
        return t_id, col, thresh
    return dash.no_update, dash.no_update, dash.no_update

# 4. Preview SQL Modal
@app.callback(
    [Output("modal-preview-sql", "is_open"),
     Output("preview-sql-code", "children")],
    [Input("btn-preview-sql", "n_clicks"),
     Input("btn-close-sql-modal", "n_clicks")],
    [State("modal-preview-sql", "is_open"),
     State("rule-input-name", "value"),
     State("rule-input-column", "value"),
     State("rule-input-threshold", "value")],
    prevent_initial_call=True,
)
def toggle_preview_sql(open_click, close_click, is_open, rule_name, column, threshold):
    if ctx.triggered_id == "btn-preview-sql":
        rendered = render_sql(
            template_name=rule_name or "NULL_COUNT",
            source_table="hospital_catalog.healthcare.patients_source",
            target_table="hospital_catalog.healthcare.patients_target",
            column_name=column or "patient_name",
            threshold=float(threshold) if threshold and threshold.isdigit() else 0,
        )
        return True, rendered
    return False, ""

# 5. Save Rule Action
@app.callback(
    Output("btn-save-rule", "children"),
    Input("btn-save-rule", "n_clicks"),
    [State("rule-select-project", "value"),
     State("rule-select-dataset", "value"),
     State("rule-input-name", "value"),
     State("rule-input-column", "value"),
     State("rule-input-threshold", "value"),
     State("rule-select-severity", "value"),
     State("rule-switch-active", "value")],
    prevent_initial_call=True,
)
def save_rule_action(n_clicks, proj, ds, name, col, thr, sev, active):
    if name:
        thr_val = float(thr) if (thr and thr.replace(".", "", 1).isdigit()) else 0.0
        db_adapter.add_new_rule(
            project_name=proj or "Hospital",
            dataset_name=ds or "source_table",
            rule_name=name.strip(),
            template_name=name.strip(),
            column_name=col.strip() if col else None,
            threshold=thr_val,
            severity=sev or "High",
            active=bool(active),
        )
        return [html.I(className="bi bi-check2 me-1"), "Saved!"]
    return "Save Rule"

# 6. Trigger Validation Run
@app.callback(
    [Output("exec-meta-id", "children"),
     Output("exec-meta-start", "children"),
     Output("exec-meta-duration", "children"),
     Output("exec-count-pass", "children"),
     Output("exec-count-fail", "children"),
     Output("exec-count-error", "children"),
     Output("exec-live-progress-list", "children")],
    Input("btn-trigger-validation", "n_clicks"),
    [State("exec-select-project", "value"),
     State("exec-select-dataset", "value")],
    prevent_initial_call=True,
)
def handle_run_validation(n_clicks, proj, ds):
    res = db_adapter.trigger_validation(proj or "Hospital", ds or "source_table")
    
    rule_results = res.get("rule_results", [])
    live_items = []
    for r in rule_results:
        s = r.get("status", "PASS")
        r_name = r.get("name", "RULE")
        if s == "PASS":
            icon_cls = "bi bi-check-circle-fill text-success me-2"
            bg = "#F0FDF4"
        elif s == "FAIL":
            icon_cls = "bi bi-x-circle-fill text-danger me-2"
            bg = "#FEF2F2"
        else:
            icon_cls = "bi bi-exclamation-triangle-fill text-warning me-2"
            bg = "#FFFBEB"
        live_items.append(html.Div([
            html.I(className=icon_cls),
            html.Span(f"{r_name} - {s}", className="fw-semibold"),
        ], className="mb-2 p-2 rounded", style={"backgroundColor": bg}))
    
    if not live_items:
        live_items = [html.Div("No rule evaluations recorded.", className="text-muted p-2")]
    
    return (
        res["execution_id"],
        res["start_time"],
        res["duration"],
        str(res["passed"]),
        str(res["failed"]),
        str(res["errored"]),
        live_items,
    )

# 7. Downloads: Export CSV & Download Reports (Phase 6 Multi-Report Engine)
@app.callback(
    Output("download-csv-action", "data"),
    [Input("btn-export-results-csv", "n_clicks"),
     Input("btn-download-alert-report", "n_clicks"),
     Input("btn-download-selected-report", "n_clicks")],
    State("select-report-type", "value"),
    prevent_initial_call=True,
)
def handle_csv_downloads(results_click, alert_click, selected_report_click, selected_report_type):
    trig = ctx.triggered_id
    if trig == "btn-export-results-csv":
        df = db_adapter.get_dq_results_table()
        return dcc.send_data_frame(df.to_csv, "dq_results_report.csv", index=False)
    elif trig == "btn-download-alert-report":
        data = db_adapter.get_alerts_data()
        return dcc.send_data_frame(data["alerts_table"].to_csv, "dq_alerts_summary.csv", index=False)
    elif trig == "btn-download-selected-report":
        rtype = selected_report_type or "PROJECT_WISE"
        df = db_adapter.generate_report(rtype)
        fname = f"dq_{rtype.lower()}_report.csv"
        return dcc.send_data_frame(df.to_csv, fname, index=False)
    return None

# 8. Filter Results on Page 5
@app.callback(
    Output("results-table-body", "children"),
    [Input("filter-results-status", "value"),
     Input("filter-results-project", "value")],
)
def filter_results_table(status_filter, proj_filter):
    df = db_adapter.get_dq_results_table(status=status_filter)
    rows = []
    for _, r in df.iterrows():
        s = str(r["status"])
        badge_cls = "badge-pass" if s == "PASS" else ("badge-fail" if s == "FAIL" else "badge-error")
        rows.append(html.Tr([
            html.Td(r["rule_name"], style={"fontWeight": "600"}),
            html.Td(r["category"]),
            html.Td(html.Span(s, className=badge_cls)),
            html.Td(str(r["failure_count"])),
            html.Td(str(r["observed_value"] or "-")),
            html.Td(str(r["expected_value"] or "-")),
            html.Td(str(r["executed_at"])),
        ]))
    return rows

# 8b. Filter Alerts on Page 7 (Phase 6)
@app.callback(
    Output("alerts-table-body", "children"),
    [Input("filter-alert-status", "value"),
     Input("filter-alert-severity", "value")],
)
def filter_alerts_table(status, sev):
    data = db_adapter.get_alerts_data(status_filter=status, severity_filter=sev)
    df = data["alerts_table"]
    alert_rows = []
    for _, a in df.iterrows():
        a_id = int(a["alert_id"])
        severity = str(a["severity"])
        st = str(a["status"])
        
        sev_color = "#EF4444" if severity == "Critical" else ("#F97316" if severity == "High" else ("#F59E0B" if severity == "Medium" else "#10B981"))
        badge_style = {"backgroundColor": sev_color + "22", "color": sev_color, "fontWeight": "600", "padding": "4px 8px", "borderRadius": "12px", "fontSize": "0.75rem"}
        
        actions = []
        if st == "Active":
            actions.append(dbc.Button("Acknowledge", id={"type": "btn-ack-alert", "id": a_id}, size="sm", color="primary", outline=True, style={"fontSize": "0.75rem", "padding": "2px 8px"}, className="me-1"))
            actions.append(dbc.Button("Close", id={"type": "btn-close-alert", "id": a_id}, size="sm", color="danger", outline=True, style={"fontSize": "0.75rem", "padding": "2px 8px"}))
        elif st == "Acknowledged":
            actions.append(dbc.Button("Close", id={"type": "btn-close-alert", "id": a_id}, size="sm", color="danger", outline=True, style={"fontSize": "0.75rem", "padding": "2px 8px"}, className="me-1"))
            actions.append(html.Span(f"Ack by {a.get('acknowledged_by') or 'admin'}", className="text-muted small"))
        else:
            actions.append(html.Span(f"Closed by {a.get('closed_by') or 'admin'}", className="text-muted small"))

        alert_rows.append(html.Tr([
            html.Td(a["date_time"]),
            html.Td(a["project_name"], style={"fontWeight": "500"}),
            html.Td(a["rule_name"]),
            html.Td(html.Span(severity, style=badge_style)),
            html.Td(str(a.get("failure_count", 0))),
            html.Td(html.Span(st, className="badge-pass" if st == "Closed" else ("badge-fail" if st == "Active" else "badge-error"))),
            html.Td(actions),
        ]))
    return alert_rows

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
    if isinstance(trig, dict) and trig.get("type") == "btn-view-audit":
        exec_id = trig["id"]
        res_df = db_adapter.get_dq_results_table()
        
        detail_rows = [
            html.Tr([
                html.Td(r["rule_name"], style={"fontWeight": "600"}),
                html.Td(r["category"]),
                html.Td(html.Span(r["status"], className="badge-pass" if r["status"] == "PASS" else "badge-fail")),
                html.Td(str(r["failure_count"])),
                html.Td(str(r["executed_at"])),
            ])
            for _, r in res_df.iterrows()
        ]
        
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
    return is_open, "", ""

# -------------------------------------------------------------
# APPLICATION ENTRYPOINT
# -------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
