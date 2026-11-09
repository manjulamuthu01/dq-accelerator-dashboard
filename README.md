# DQ Accelerator – Enterprise Data Quality Monitoring & Governance Dashboard

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Dash](https://img.shields.io/badge/Dash-2.17%2B-008DE4.svg)](https://dash.plotly.com/)
[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621.svg)](https://www.databricks.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An enterprise-ready, production-grade Data Quality (DQ) monitoring and governance platform built with **Dash**, **Plotly**, and **Python**. It provides unified rule authoring, parallel execution orchestration, source-to-target reconciliation, automated email alerting with retry resilience, Unity Catalog Delta table discovery, and comprehensive multi-view reporting.

---

## Table of Contents

1. [Overview](#overview)
2. [Key Features](#key-features)
3. [Architecture & Technology Stack](#architecture--technology-stack)
4. [Application Modules (8 Pages)](#application-modules-8-pages)
5. [Repository Structure](#repository-structure)
6. [System Requirements](#system-requirements)
7. [Installation & Setup](#installation--setup)
8. [Configuration & Environment Variables](#configuration--environment-variables)
9. [Running the Application](#running-the-application)
10. [Databricks Deployment](#databricks-deployment)
11. [Troubleshooting & Diagnostics](#troubleshooting--diagnostics)
12. [Contributing & License](#contributing--license)

---

## Overview

Modern data platforms require robust, continuous quality validation to ensure downstream analytics, machine learning models, and executive dashboards are powered by accurate data. **DQ Accelerator** bridges data engineering and data governance by offering:

* **Dual Execution Modes:** Connects to **Databricks SQL Warehouses** (validating Unity Catalog Delta tables) or runs completely offline on high-performance local persistent **SQLite**.
* **Zero Hardcoded Secrets:** Credentials, API tokens, and connection strings are managed through standard `.env` variables and secret managers.
* **Non-Blocking Orchestration:** Parallel rule validation running across multiple worker threads with resource-aware CPU concurrency throttling.
* **Resilient Alert Lifecycle:** Email notifications for failure events with exponential backoff retries, provider auto-detection (Gmail, Microsoft 365, SendGrid, Amazon SES), and self-service diagnostics.

---

## Key Features

* **Dynamic Unity Catalog Table Discovery:** Interactively discover Catalogs, Schemas, and Tables from your Databricks workspace using secure OAuth or personal access tokens.
* **14+ Parameterized Quality Rules:** Built-in SQL rule templates spanning Completeness, Uniqueness, Validity, Consistency, Integrity, and Accuracy.
* **Parallel Execution Engine:** Validates rules concurrently using Python's `ThreadPoolExecutor` with real-time progress indicators, duration metrics, and worker thread distribution.
* **Source-to-Target Reconciliation:** Automated multi-check reconciliation comparing record counts, missing keys, duplicate business keys, and column-level value discrepancies.
* **Multi-View Reporting Engine:** One-click CSV exports for Project-wise summaries, Dataset health, Rule-level audit, Critical failures, and Trend analytics.
* **Interactive Alert Lifecycle Management:** Track alerts through `Active`, `Acknowledged`, and `Closed` states with historical configuration audit logging (`dq_config_audit`).
* **Automated & Scheduled Triggers:** Persistent in-process cron scheduler supporting Daily, Weekly, and custom cron expressions.

---

## Architecture & Technology Stack

```
                     +---------------------------------------+
                     |         DQ Accelerator UI             |
                     |  (Dash / Plotly / Bootstrap / Flask)  |
                     +-------------------+-------------------+
                                         |
                                         v
                     +---------------------------------------+
                     |            Core Orchestrator          |
                     |     (dash_app.py & db_adapter.py)     |
                     +---+-------------------------------+---+
                         |                               |
        +----------------+---------------+               |
        v                                v               v
+---------------+                +---------------+ +---------------+
|  Rule Engine  |                | Email Service | |   Scheduler   |
| (Templates &  |                | (SMTP/SendGrid| |  (Persistent  |
| SQL Compiler) |                | Queue/Retry)  | | Background)   |
+-------+-------+                +-------+-------+ +-------+-------+
        |                                |                 |
        +----------------+---------------+-----------------+
                         |
                         v
       +-----------------+-----------------+
       |         Storage & Compute         |
       +-----------------+-----------------+
       | Local: SQLite3 (dq_accelerator.db)|
       | Cloud: Databricks Unity Catalog   |
       |        SQL Warehouse / Delta Lake |
       +-----------------------------------+
```

### Technology Stack

* **Frontend & Web Framework:** Dash 2.17+, Dash Bootstrap Components 1.6+, Plotly 5.20+, Flask
* **Data Manipulation & Engines:** Pandas 2.0+, PyArrow, OpenPyXL, SQLite3
* **Databricks Cloud Platform:** Databricks SQL Connector 3.4+, Databricks SDK 0.32+
* **Email & Notifications:** Standard Library `smtplib`, `ssl`, SendGrid v3 HTTPS REST API
* **Process Management:** Python `threading`, `concurrent.futures`, `queue`

---

## Application Modules (8 Pages)

| Page | Route | Description |
| :--- | :--- | :--- |
| **1. Overview** | `/` | Executive KPIs, total datasets monitored, rule pass/fail distributions, active alert feeds, and DQ score trends. |
| **2. Projects & Datasets** | `/management` | Register projects, configure primary/additional team email recipients, upload local datasets (CSV, JSON, Parquet, Excel), or connect via Unity Catalog discovery. |
| **3. Rule Library** | `/rule-library` | Catalog of DQ rules categorized by dimensions. Dynamic rule builder with SQL preview, parameter inputs, and threshold configuration. |
| **4. Execution & Monitoring** | `/execution` | Orchestrate validations via **Manual Trigger** ("Run Now") or **Scheduled Trigger** (Cron). Displays active rules, live execution timeline, duration, and DQ score. |
| **5. DQ Results** | `/results` | Detailed breakdown of validated rules, observed vs. expected metrics, record failure counts, and row-level anomaly inspection modals. |
| **6. Reconciliation** | `/reconciliation` | Source vs. target dataset reconciliation: row count matching, missing keys, duplicate keys, and column discrepancy sampling. |
| **7. Alerts & Reports** | `/alerts` | Alert resolution workflow, severity breakdown, email delivery tracking (`SENT`, `FAILED`, `RETRYING`), self-service SMTP diagnostic tools, and standard CSV reports. |
| **8. Audit History** | `/audit` | Complete execution log history and entity configuration change tracking (`dq_config_audit`). |

---

## Repository Structure

```text
dq-accelerator-dashboard/
├── accelerator/
│   ├── __init__.py               # Accelerator module package init
│   └── orchestrator.py           # Databricks workflow integration wrapper
├── notebooks/
│   ├── 01_environment_setup.py   # Databricks workspace & schema initialization
│   ├── 02_onboard_sample_projects.py # Sample datasets & reference onboarding
│   ├── 03_dq_orchestrator.py     # Databricks notebook job validation orchestrator
│   └── 04_dq_reconciliation.py   # Automated cross-table Delta reconciliation
├── sql/
│   ├── 01_create_all_tables.sql  # Database DDL for all accelerator tables
│   └── 02_seed_sample_data.sql   # Baseline seed data for offline demo mode
├── .databricksignore             # Files excluded from Databricks sync
├── .env.example                  # Environment configuration template
├── .gitignore                    # Git exclusions (credentials, venv, runtime DB)
├── app.yaml                      # Databricks Apps application manifest
├── dash_app.py                   # Main Dash UI, page layouts, and callback router
├── databricks_job.json           # Databricks Workflow Job definition
├── db_adapter.py                 # Dual-mode persistent database connector & orchestration
├── dq_orchestrator_job.py        # Standalone runner for Databricks CLI / job tasks
├── email_service.py              # Resilient SMTP & SendGrid email notification engine
├── requirements.txt              # Production Python package dependencies
├── rule_engine.py                # Parameterized SQL rule templates & validation logic
├── scheduler_service.py          # Persistent schedule executor daemon
├── setup.py                      # Standard package installer
└── unity_catalog_service.py      # Databricks SDK & Unity Catalog discovery service
```

---

## System Requirements

* **Operating System:** Windows 10/11, macOS 12+, or Linux (Ubuntu 20.04+, RHEL 8+)
* **Python:** Version 3.10, 3.11, or 3.12
* **Memory:** Minimum 4 GB RAM (8 GB recommended)
* **Disk Space:** 500 MB free space
* **Network:** Outbound HTTPS (Port 443) for Databricks / SendGrid, Outbound SMTP (Port 587 or 465) for email alerts

---

## Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/manjulamuthu01/dq-accelerator-dashboard.git
cd dq-accelerator-dashboard
```

### 2. Create and Activate Virtual Environment

**On Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**On macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Configuration & Environment Variables

Create a local `.env` file by copying `.env.example`:

```bash
cp .env.example .env
```

Open `.env` in your text editor and configure your environment:

```env
# ==============================================================================
# DQ Accelerator - Environment Configuration
# ==============================================================================

# --- Email Notifications (Optional: configure if failure emails are desired) ---
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-gmail-app-password
SMTP_SENDER_EMAIL=dq-alerts@company.com
SMTP_USE_TLS=true
SMTP_USE_SSL=false
SMTP_TIMEOUT=15

# SendGrid Alternative (Optional):
# SENDGRID_API_KEY=SG.your-sendgrid-api-key

# --- Databricks Integration (Optional: only needed if connecting to Unity Catalog) ---
# DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
# DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
# DATABRICKS_TOKEN=dapi-your-personal-access-token
# DQ_CATALOG_SCHEMA=your_catalog.your_schema
# DQ_JOB_ID=your-workflow-job-id

# --- Server Port ---
DATABRICKS_APP_PORT=8000
```

> **Note on Local Offline Mode:** If `DATABRICKS_HOST` is left commented out or unset, the application automatically uses the embedded, high-performance local SQLite database engine (`dq_accelerator.db`). All 8 pages, rule execution, reconciliation, and reports will work out of the box with zero external configuration!

---

## Running the Application

Start the Dash web application:

```bash
python dash_app.py
```

Output:
```text
[DQ-Scheduler] Background persistent scheduler daemon activated.
[DQ-NotificationQueue] Background email notification queue activated.
Dash is running on http://0.0.0.0:8000/
```

Open your browser and navigate to:
**`http://127.0.0.1:8000`**

---

## Databricks Deployment

The project includes native integration for **Databricks Apps** and **Databricks Workflows**.

### 1. Sync Workspace Files

```bash
databricks sync --watch . /Workspace/Users/<your-email>/dq-accelerator-dashboard
```

### 2. Deploy as a Databricks App

Ensure [`app.yaml`](file:///c:/Users/manju/Downloads/dq-accelerator-dashboard/dq-accelerator-dashboard/app.yaml) is configured:
```yaml
command:
  - "python"
  - "dash_app.py"
env:
  - name: "DATABRICKS_APP_PORT"
    value: "8000"
```

Deploy the application:
```bash
databricks apps deploy dq-accelerator-dashboard --source-code-path /Workspace/Users/<your-email>/dq-accelerator-dashboard
```

---

## Troubleshooting & Diagnostics

### 1. Email Notifications show `FAILED` in Alerts & Reports
* Navigate to **Alerts & Reports** (`/alerts`).
* Click the **`[Email Configuration & Health Check]`** button at the top right.
* Click **`Test SMTP Connection`** to view connection diagnostics.
* If using Gmail, ensure you have 2-Factor Authentication enabled and generate a dedicated **Google App Password** (do not use your regular account password).
* Enter your recipient address and click **`Send Verification Email`** to test end-to-end delivery.

### 2. Port Conflict (`Address already in use`)
Set a custom port before starting:
```bash
# Windows PowerShell:
$env:DATABRICKS_APP_PORT="8050"
python dash_app.py

# Linux / macOS:
export DATABRICKS_APP_PORT=8050
python dash_app.py
```

### 3. Missing Modules on Startup
Ensure your virtual environment is active and run:
```bash
pip install -r requirements.txt
```

---

## Contributing & License

Contributions, feedback, and pull requests are welcome!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/NewFeature`)
3. Commit your Changes (`git commit -m 'feat: Add NewFeature'`)
4. Push to the Branch (`git push origin feature/NewFeature`)
5. Open a Pull Request

Distributed under the **MIT License**. See `LICENSE` for more information.
