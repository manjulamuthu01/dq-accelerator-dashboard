"""
Email Notification Service for DQ Accelerator
=============================================
Provides real-time, resilient email notifications (SMTP, Microsoft 365, SendGrid)
for data quality rule failures and alert lifecycle events. Reads configuration
dynamically from .env and environment variables without hardcoded credentials,
supports secure TLS/SSL authentication, controlled retries with exponential backoff,
error classification, background queue processing, and comprehensive delivery logging.
"""

import os
import re
import ssl
import json
import smtplib
import time
import logging
import threading
import queue
import urllib.request
import urllib.error
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger("dq_email_service")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


def load_env_file() -> Dict[str, str]:
    """
    Auto-detects and loads key-value pairs from .env file into os.environ.
    Checks workspace root, parent directories, and script directory.
    Does not overwrite existing environment variables if already set.
    """
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"),
    ]

    loaded = {}
    for path in candidates:
        if os.path.exists(path) and os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            key = k.strip()
                            val = v.strip().strip("'\"")
                            loaded[key] = val
                            if key not in os.environ:
                                os.environ[key] = val
                logger.info(f"[DQ-Email] Successfully loaded configuration from {path}")
                break
            except Exception as e:
                logger.warning(f"[DQ-Email] Error loading .env from {path}: {e}")
    return loaded


# Automatically load on module import
load_env_file()


def save_smtp_config_to_env(
    host: str,
    port: int,
    username: str,
    password: str,
    sender_email: str,
    use_tls: bool = True,
    use_ssl: bool = False,
    timeout: int = 15,
) -> Tuple[bool, str]:
    """
    Persists SMTP configuration to the local .env file securely
    and updates os.environ immediately.
    """
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        
        # Read existing file to preserve other variables (e.g. Databricks vars)
        existing_lines = []
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                existing_lines = f.readlines()

        keys_to_set = {
            "SMTP_HOST": host.strip(),
            "SMTP_PORT": str(port),
            "SMTP_USERNAME": username.strip(),
            "SMTP_SENDER_EMAIL": sender_email.strip(),
            "SMTP_USE_TLS": "true" if use_tls else "false",
            "SMTP_USE_SSL": "true" if use_ssl else "false",
            "SMTP_TIMEOUT": str(timeout),
        }
        if password:
            keys_to_set["SMTP_PASSWORD"] = password.strip()

        # Update in-memory os.environ
        for k, v in keys_to_set.items():
            os.environ[k] = v

        # Write or update lines in .env
        new_lines = []
        found_keys = set()
        for line in existing_lines:
            sline = line.strip()
            if sline and not sline.startswith("#") and "=" in sline:
                k, _ = sline.split("=", 1)
                k = k.strip()
                if k in keys_to_set:
                    new_lines.append(f"{k}={keys_to_set[k]}\n")
                    found_keys.add(k)
                    continue
            new_lines.append(line)

        # Append missing keys
        for k, v in keys_to_set.items():
            if k not in found_keys:
                new_lines.append(f"{k}={v}\n")

        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

        return True, "SMTP configuration successfully updated and saved to .env."
    except Exception as e:
        return False, f"Failed to persist configuration to .env: {str(e)}"


def get_smtp_config() -> Dict[str, Any]:
    """
    Retrieves SMTP and email provider configuration from environment variables.
    Provides provider detection, credential masking, and configuration status.
    """
    host = os.environ.get("SMTP_HOST", "").strip()
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USERNAME", "").strip() or os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip() or os.environ.get("SMTP_PASS", "").strip()
    use_tls = os.environ.get("SMTP_USE_TLS", "true").lower() in ("true", "1", "yes")
    use_ssl = os.environ.get("SMTP_USE_SSL", "false").lower() in ("true", "1", "yes")
    sender = os.environ.get("SMTP_SENDER_EMAIL", "").strip() or os.environ.get("SMTP_SENDER", "").strip() or os.environ.get("DEFAULT_FROM_EMAIL", "").strip() or user or "dq-accelerator@company.com"
    sendgrid_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    timeout = int(os.environ.get("SMTP_TIMEOUT", "15"))

    # Detect provider name
    if sendgrid_key:
        provider = "SendGrid Web API"
    elif host:
        host_lower = host.lower()
        if "gmail" in host_lower:
            provider = "Gmail SMTP"
        elif "office365" in host_lower or "outlook" in host_lower:
            provider = "Microsoft 365 / Outlook"
        elif "sendgrid" in host_lower:
            provider = "SendGrid SMTP"
        elif "amazonaws" in host_lower or "ses" in host_lower:
            provider = "Amazon SES"
        else:
            provider = f"Custom SMTP ({host})"
    else:
        provider = "Not Configured"

    # Mask user & password for safe presentation
    masked_user = ""
    if user:
        if "@" in user:
            u_name, domain = user.split("@", 1)
            masked_user = (u_name[0] + "***" + u_name[-1] if len(u_name) > 1 else u_name + "***") + "@" + domain
        else:
            masked_user = user[:2] + "***" + user[-1:] if len(user) > 2 else "***"

    return {
        "host": host,
        "port": port,
        "user": user,
        "masked_user": masked_user,
        "password": password,
        "has_password": bool(password),
        "use_tls": use_tls,
        "use_ssl": use_ssl,
        "sender": sender,
        "sendgrid_key": sendgrid_key,
        "provider": provider,
        "timeout": timeout,
        "configured": bool(host or sendgrid_key),
    }


def validate_email_address(email: str) -> bool:
    """Validates a single email address string."""
    if not email or not isinstance(email, str):
        return False
    return bool(EMAIL_REGEX.match(email.strip()))


def parse_and_validate_emails(email_input: Any) -> Tuple[bool, List[str], str]:
    """
    Parses a string or list of emails (comma, semicolon, or newline separated),
    validates each address, and returns (is_valid, clean_list, error_msg).
    """
    if not email_input:
        return True, [], ""

    if isinstance(email_input, list):
        raw_items = email_input
    else:
        raw_items = re.split(r"[,;\s\n]+", str(email_input))

    clean_emails = []
    invalid_emails = []

    for item in raw_items:
        clean = item.strip().lower()
        if not clean:
            continue
        if validate_email_address(clean):
            if clean not in clean_emails:
                clean_emails.append(clean)
        else:
            invalid_emails.append(clean)

    if invalid_emails:
        return False, clean_emails, f"Invalid email format: {', '.join(invalid_emails)}"

    return True, clean_emails, ""


def classify_error(err_str: str) -> Tuple[str, bool]:
    """
    Classifies an error string into a standard category and determines
    whether it is a transient error eligible for automatic retries.
    Returns (category, can_retry).
    """
    s = err_str.lower()
    if "not configured" in s or "missing" in s:
        return "CONFIG_MISSING", False
    if "invalid email" in s or "recipient" in s or "550" in s or "553" in s:
        return "INVALID_RECIPIENT", False
    if "winerror 10013" in s or "access forbidden" in s or "permission denied" in s:
        return "NETWORK_RESTRICTED", False
    if "535" in s or "authentication failed" in s or "badcredentials" in s or "password" in s:
        return "AUTH_ERROR", False
    if "timed out" in s or "timeout" in s or "421" in s or "451" in s or "452" in s or "connection refused" in s:
        return "CONNECTION_TIMEOUT", True
    if "connection reset" in s or "broken pipe" in s or "unexpected eof" in s:
        return "CONNECTION_ERROR", True
    return "GENERAL_ERROR", False


def build_alert_email_content(
    project_name: str,
    dataset_name: str,
    execution_id: str,
    execution_time: str,
    failed_rules: List[Dict[str, Any]],
    total_rules: int,
    passed_rules: int,
    failed_count: int,
    dq_score: float,
) -> Tuple[str, str, str]:
    """
    Builds subject, plain text, and HTML body for a consolidated DQ failure alert.
    """
    severities = [(r.get("sev") or r.get("severity") or "High").capitalize() for r in failed_rules]
    if "Critical" in severities:
        top_sev = "CRITICAL"
    elif "High" in severities:
        top_sev = "HIGH"
    elif "Medium" in severities:
        top_sev = "MEDIUM"
    else:
        top_sev = "LOW"

    subject = f"[{top_sev}] Data Quality Alert – {project_name} – {dataset_name}"

    # Plain text version
    rules_text_lines = []
    rules_text_lines.append(f"{'Rule Name':<28} | {'Column':<18} | {'Failure Count':<14} | {'Threshold':<12} | {'Severity':<10}")
    rules_text_lines.append("-" * 90)
    for r in failed_rules:
        r_name = str(r.get("name") or r.get("rule_name", "UNKNOWN"))
        r_col = str(r.get("column") or r.get("column_name", "—"))
        r_fail = str(r.get("failures") or r.get("failure_count", 0))
        r_thresh = str(r.get("threshold", "0"))
        r_sev = str(r.get("sev") or r.get("severity", "High")).capitalize()
        rules_text_lines.append(f"{r_name:<28} | {r_col:<18} | {r_fail:<14} | {r_thresh:<12} | {r_sev:<10}")

    rules_table_text = "\n".join(rules_text_lines)

    plain_body = f"""Hello Project Team,

A data quality validation failure has been detected in your registered project.

Project Details:
Project Name: {project_name}
Dataset Name: {dataset_name}
Execution ID: {execution_id}
Execution Date and Time: {execution_time}
Overall Execution Status: FAILED

Failed Rule Details:
{rules_table_text}

Additional Information:
Total Rules Executed: {total_rules}
Rules Passed: {passed_rules}
Rules Failed: {failed_count}
Overall Data Quality Score: {dq_score}%

Action Required:
Please log in to the DQ Accelerator application and review the failed data quality validations.
Alert Status: ACTIVE

Regards,
DQ Accelerator – Automated Data Quality Monitoring System
"""

    # HTML table rows
    html_rows = []
    for r in failed_rules:
        r_name = str(r.get("name") or r.get("rule_name", "UNKNOWN"))
        r_col = str(r.get("column") or r.get("column_name", "—"))
        r_fail = str(r.get("failures") or r.get("failure_count", 0))
        r_thresh = str(r.get("threshold", "0"))
        r_sev = str(r.get("sev") or r.get("severity", "High")).capitalize()

        badge_color = "#DC2626" if r_sev == "Critical" else ("#EA580C" if r_sev == "High" else "#D97706")
        badge_bg = "#FEE2E2" if r_sev == "Critical" else ("#FFEDD5" if r_sev == "High" else "#FEF3C7")

        html_rows.append(f"""
        <tr style="border-bottom: 1px solid #E2E8F0;">
            <td style="padding: 10px 12px; font-weight: 600; color: #1E293B;">{r_name}</td>
            <td style="padding: 10px 12px; color: #475569; font-family: monospace;">{r_col}</td>
            <td style="padding: 10px 12px; font-weight: bold; color: #DC2626;">{r_fail}</td>
            <td style="padding: 10px 12px; color: #64748B;">{r_thresh}</td>
            <td style="padding: 10px 12px;">
                <span style="background-color: {badge_bg}; color: {badge_color}; padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: bold;">
                    {r_sev}
                </span>
            </td>
        </tr>
        """)

    html_table = "".join(html_rows)

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.5; color: #1E293B; background-color: #F8FAFC; margin: 0; padding: 20px; }}
            .container {{ max-width: 680px; margin: 0 auto; background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); }}
            .header {{ background: linear-gradient(135deg, #1E3A8A 0%, #2563EB 100%); color: white; padding: 24px 28px; }}
            .header h2 {{ margin: 0 0 6px 0; font-size: 20px; font-weight: 700; }}
            .content {{ padding: 24px 28px; }}
            .section-title {{ font-size: 14px; font-weight: 700; color: #0F172A; text-transform: uppercase; letter-spacing: 0.05em; margin: 20px 0 10px 0; }}
            .info-grid {{ width: 100%; border-collapse: collapse; margin-bottom: 18px; }}
            .info-grid td {{ padding: 6px 0; font-size: 13px; }}
            .info-label {{ color: #64748B; font-weight: 600; width: 190px; }}
            .info-val {{ color: #0F172A; font-weight: 500; }}
            .status-failed {{ color: #DC2626; font-weight: bold; }}
            table.rules-table {{ width: 100%; border-collapse: collapse; margin: 12px 0 20px 0; font-size: 13px; }}
            table.rules-table th {{ background-color: #F1F5F9; color: #475569; font-weight: 700; text-align: left; padding: 10px 12px; font-size: 12px; border-bottom: 2px solid #CBD5E1; }}
            .score-box {{ background-color: #EFF6FF; border: 1px solid #BFDBFE; border-radius: 8px; padding: 14px 18px; margin: 18px 0; }}
            .action-box {{ background-color: #FEF2F2; border-left: 4px solid #DC2626; padding: 14px 18px; border-radius: 4px; margin: 20px 0; }}
            .footer {{ background-color: #F8FAFC; border-top: 1px solid #E2E8F0; padding: 16px 28px; font-size: 12px; color: #64748B; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>Data Quality Validation Alert</h2>
                <div style="font-size: 13px; opacity: 0.9;">DQ Accelerator Automated Monitoring</div>
            </div>
            <div class="content">
                <p style="margin-top: 0; font-size: 15px;">Hello Project Team,</p>
                <p style="color: #475569; font-size: 14px;">
                    A data quality validation failure has been detected in your registered project.
                </p>

                <div class="section-title">Project Details</div>
                <table class="info-grid">
                    <tr><td class="info-label">Project Name:</td><td class="info-val">{project_name}</td></tr>
                    <tr><td class="info-label">Dataset Name:</td><td class="info-val">{dataset_name}</td></tr>
                    <tr><td class="info-label">Execution ID:</td><td class="info-val" style="font-family: monospace;">{execution_id}</td></tr>
                    <tr><td class="info-label">Execution Date and Time:</td><td class="info-val">{execution_time}</td></tr>
                    <tr><td class="info-label">Overall Execution Status:</td><td class="info-val status-failed">FAILED</td></tr>
                </table>

                <div class="section-title">Failed Rule Details ({len(failed_rules)})</div>
                <table class="rules-table">
                    <thead>
                        <tr>
                            <th>Rule Name</th>
                            <th>Column</th>
                            <th>Failure Count</th>
                            <th>Threshold</th>
                            <th>Severity</th>
                        </tr>
                    </thead>
                    <tbody>
                        {html_table}
                    </tbody>
                </table>

                <div class="score-box">
                    <div style="font-size: 12px; font-weight: 700; color: #1E40AF; text-transform: uppercase; margin-bottom: 6px;">Execution Metrics</div>
                    <div style="display: flex; justify-content: space-between; font-size: 13px;">
                        <span>Total Rules: <strong>{total_rules}</strong></span> &bull;
                        <span>Passed: <strong style="color: #16A34A;">{passed_rules}</strong></span> &bull;
                        <span>Failed: <strong style="color: #DC2626;">{failed_count}</strong></span> &bull;
                        <span>Overall DQ Score: <strong style="color: #2563EB;">{dq_score}%</strong></span>
                    </div>
                </div>

                <div class="action-box">
                    <div style="font-weight: 700; color: #991B1B; font-size: 13px; margin-bottom: 4px;">Action Required</div>
                    <div style="font-size: 13px; color: #7F1D1D;">
                        Please log in to the DQ Accelerator application to review the failed data quality validations and remediate anomalies.<br>
                        <strong>Alert Status: ACTIVE</strong>
                    </div>
                </div>
            </div>
            <div class="footer">
                Regards,<br>
                <strong>DQ Accelerator</strong> – Automated Data Quality Monitoring System
            </div>
        </div>
    </body>
    </html>
    """

    return subject, plain_body, html_body


def build_test_email_content(
    recipient: str,
    user: str = "admin",
    provider: str = "SMTP",
) -> Tuple[str, str, str]:
    """
    Builds subject and body for a verification test email.
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    subject = f"[TEST] DQ Accelerator Email Notification Verification ({ts})"
    plain_body = f"""Hello,

This is a test notification sent from the DQ Accelerator Data Quality Monitoring platform.
If you are receiving this email, your notification configuration is functioning properly.

Delivery Details:
- Recipient: {recipient}
- Timestamp: {ts}
- Triggered By: {user}
- Configured Provider: {provider}

Regards,
DQ Accelerator Automated Monitoring
"""
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background: #F8FAFC; padding: 20px;">
        <div style="max-width: 600px; margin: 0 auto; background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 8px; overflow: hidden;">
            <div style="background: #10B981; color: white; padding: 18px 24px;">
                <h3 style="margin: 0;">Email Notification Verification</h3>
                <div style="font-size: 13px; opacity: 0.9;">DQ Accelerator Health Check</div>
            </div>
            <div style="padding: 24px;">
                <p>Hello,</p>
                <p>This is a test notification sent from your <strong>DQ Accelerator</strong> instance.</p>
                <div style="background: #ECFDF5; border: 1px solid #A7F3D0; border-radius: 6px; padding: 14px; margin: 16px 0; font-size: 13px;">
                    <div><strong>Recipient:</strong> {recipient}</div>
                    <div><strong>Timestamp:</strong> {ts}</div>
                    <div><strong>Triggered By:</strong> {user}</div>
                    <div><strong>Provider:</strong> {provider}</div>
                    <div><strong>Status:</strong> <span style="color: #047857; font-weight: bold;">CONNECTED & VERIFIED</span></div>
                </div>
                <p style="font-size: 13px; color: #64748B;">All failure notifications will be dispatched to project stakeholders using these settings.</p>
            </div>
            <div style="background: #F8FAFC; border-top: 1px solid #E2E8F0; padding: 14px 24px; font-size: 12px; color: #64748B;">
                DQ Accelerator &bull; Real-time Data Quality Management
            </div>
        </div>
    </body>
    </html>
    """
    return subject, plain_body, html_body


def send_via_sendgrid(
    api_key: str,
    sender: str,
    recipients: List[str],
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Dispatches email via SendGrid v3 REST API.
    Zero external dependencies required (uses standard urllib).
    """
    url = "https://api.sendgrid.com/v3/mail/send"
    personalizations = [{"to": [{"email": r} for r in recipients]}]
    content = [{"type": "text/plain", "value": body_text}]
    if body_html:
        content.append({"type": "text/html", "value": body_html})

    payload = {
        "personalizations": personalizations,
        "from": {"email": sender, "name": "DQ Accelerator"},
        "subject": subject,
        "content": content,
    }

    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data_bytes,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "DQ-Accelerator-Client/1.0",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            status_code = resp.getcode()
            msg_id = resp.headers.get("X-Message-Id", f"sg-{int(time.time())}")
            if status_code in (200, 202):
                return {
                    "success": True,
                    "status": "SENT",
                    "recipients": recipients,
                    "error": None,
                    "provider": "SendGrid Web API",
                    "provider_message_id": msg_id,
                    "retry_count": 0,
                    "can_retry": False,
                }
            else:
                return {
                    "success": False,
                    "status": "FAILED",
                    "recipients": recipients,
                    "error": f"SendGrid returned HTTP {status_code}",
                    "provider": "SendGrid Web API",
                    "provider_message_id": None,
                    "retry_count": 0,
                    "can_retry": True,
                }
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8", errors="ignore")
        return {
            "success": False,
            "status": "FAILED",
            "recipients": recipients,
            "error": f"SendGrid HTTP {e.code}: {err_msg}",
            "provider": "SendGrid Web API",
            "provider_message_id": None,
            "retry_count": 0,
            "can_retry": e.code in (429, 500, 502, 503, 504),
        }
    except Exception as e:
        return {
            "success": False,
            "status": "FAILED",
            "recipients": recipients,
            "error": f"SendGrid error: {str(e)}",
            "provider": "SendGrid Web API",
            "provider_message_id": None,
            "retry_count": 0,
            "can_retry": True,
        }


def send_email(
    recipients: List[str],
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    max_retries: int = 2,
) -> Dict[str, Any]:
    """
    Sends an email to a list of recipients with controlled exponential backoff retries.
    Supports standard SMTP, TLS/SSL, and SendGrid Web API.
    Never raises unhandled exceptions to the caller.
    """
    if not recipients:
        return {
            "success": False,
            "status": "FAILED",
            "recipients": [],
            "error": "No recipient email addresses provided.",
            "error_category": "INVALID_RECIPIENT",
            "provider": "None",
            "provider_message_id": None,
            "retry_count": 0,
            "can_retry": False,
        }

    # Validate all recipient email formats
    valid_recipients = [r.strip() for r in recipients if validate_email_address(r)]
    if not valid_recipients:
        return {
            "success": False,
            "status": "FAILED",
            "recipients": recipients,
            "error": f"Invalid recipient email address format: {', '.join(recipients)}",
            "error_category": "INVALID_RECIPIENT",
            "provider": "None",
            "provider_message_id": None,
            "retry_count": 0,
            "can_retry": False,
        }

    cfg = get_smtp_config()

    # Route 1: SendGrid Web API if configured
    if cfg.get("sendgrid_key"):
        return send_via_sendgrid(
            api_key=cfg["sendgrid_key"],
            sender=cfg["sender"],
            recipients=valid_recipients,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
        )

    # Route 2: SMTP Dispatch
    host = cfg["host"]
    port = cfg["port"]
    sender = cfg["sender"]
    user = cfg["user"]
    password = cfg["password"]
    use_tls = cfg["use_tls"]
    use_ssl = cfg["use_ssl"]
    timeout = cfg["timeout"]
    provider = cfg["provider"]

    if not host:
        err_msg = "SMTP server host (SMTP_HOST) is not configured in environment variables or .env file."
        logger.warning(f"[DQ-Email] {err_msg}")
        return {
            "success": False,
            "status": "FAILED",
            "recipients": valid_recipients,
            "error": err_msg,
            "error_category": "CONFIG_MISSING",
            "provider": provider,
            "provider_message_id": None,
            "retry_count": 0,
            "can_retry": False,
        }

    # Prepare MIME Message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(valid_recipients)

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    if body_html:
        msg.attach(MIMEText(body_html, "html", "utf-8"))

    last_error = ""
    last_cat = "GENERAL_ERROR"
    can_retry_flag = False
    retries_done = 0

    for attempt in range(max_retries + 1):
        try:
            if use_ssl:
                ssl_ctx = ssl.create_default_context()
                server = smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl_ctx)
            else:
                server = smtplib.SMTP(host, port, timeout=timeout)

            server.ehlo()
            if use_tls and not use_ssl:
                ssl_ctx = ssl.create_default_context()
                server.starttls(context=ssl_ctx)
                server.ehlo()

            if user and password:
                server.login(user, password)

            server.sendmail(sender, valid_recipients, msg.as_string())
            server.quit()

            gen_msg_id = f"msg-{int(time.time())}-{os.urandom(4).hex()}"
            logger.info(f"[DQ-Email] Successfully delivered alert email via {provider} to {valid_recipients} (attempt {attempt + 1})")
            return {
                "success": True,
                "status": "SENT",
                "recipients": valid_recipients,
                "error": None,
                "error_category": None,
                "provider": provider,
                "provider_message_id": gen_msg_id,
                "retry_count": attempt,
                "can_retry": False,
            }

        except Exception as e:
            raw_err = str(e)
            retries_done = attempt
            cat, is_transient = classify_error(raw_err)
            last_cat = cat
            can_retry_flag = is_transient

            # Diagnostic message formatting
            if "10013" in raw_err:
                last_error = (
                    f"Outbound network socket blocked by local system/sandbox policy (WinError 10013). "
                    f"Outbound SMTP connection to {host}:{port} was rejected by the OS. "
                    f"When running on Databricks Apps or outside isolated sandbox, delivery will complete normally."
                )
            elif cat == "AUTH_ERROR":
                last_error = f"SMTP Authentication failed with {host}:{port} for user '{cfg['masked_user']}'. Check username and password (or App Password)."
            elif cat == "CONNECTION_TIMEOUT":
                last_error = f"SMTP Connection timed out connecting to {host}:{port} (timeout={timeout}s)."
            else:
                last_error = f"SMTP delivery error: {raw_err}"

            logger.warning(f"[DQ-Email] Delivery attempt {attempt + 1} failed for {valid_recipients}: {last_error}")

            if is_transient and attempt < max_retries:
                backoff_time = (2 ** attempt) * 1.5
                time.sleep(backoff_time)
            elif not is_transient:
                # Do not retry permanent errors
                break

    return {
        "success": False,
        "status": "FAILED",
        "recipients": valid_recipients,
        "error": last_error,
        "error_category": last_cat,
        "provider": provider,
        "provider_message_id": None,
        "retry_count": retries_done,
        "can_retry": can_retry_flag,
    }


def test_smtp_connection() -> Dict[str, Any]:
    """
    Tests SMTP connectivity without sending an actual email.
    Verifies DNS, socket connection, TLS/SSL negotiation, and authentication.
    """
    cfg = get_smtp_config()
    if not cfg["host"] and not cfg.get("sendgrid_key"):
        return {
            "connected": False,
            "provider": "Not Configured",
            "message": "SMTP host (SMTP_HOST) or SENDGRID_API_KEY is not configured.",
            "hint": "Add SMTP_HOST, SMTP_PORT, SMTP_USERNAME, and SMTP_PASSWORD to your .env file.",
            "config": {k: (v if k != "password" and k != "sendgrid_key" else "***") for k, v in cfg.items()},
        }

    if cfg.get("sendgrid_key"):
        return {
            "connected": True,
            "provider": "SendGrid Web API",
            "message": "SendGrid API Key detected. Ready for email delivery.",
            "hint": "SendGrid delivers alerts via secure REST HTTPS API.",
            "config": {k: (v if k != "password" and k != "sendgrid_key" else "***") for k, v in cfg.items()},
        }

    try:
        host = cfg["host"]
        port = cfg["port"]
        timeout = cfg["timeout"]

        if cfg["use_ssl"]:
            ssl_ctx = ssl.create_default_context()
            server = smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl_ctx)
        else:
            server = smtplib.SMTP(host, port, timeout=timeout)

        server.ehlo()
        if cfg["use_tls"] and not cfg["use_ssl"]:
            ssl_ctx = ssl.create_default_context()
            server.starttls(context=ssl_ctx)
            server.ehlo()

        if cfg["user"] and cfg["password"]:
            server.login(cfg["user"], cfg["password"])

        server.quit()
        return {
            "connected": True,
            "provider": cfg["provider"],
            "message": f"Successfully connected and authenticated with {cfg['provider']} ({host}:{port}).",
            "hint": "All configuration parameters are valid.",
            "config": {k: (v if k != "password" else "***") for k, v in cfg.items()},
        }
    except Exception as e:
        raw_err = str(e)
        cat, _ = classify_error(raw_err)
        if "10013" in raw_err:
            hint = "Windows sandbox blocks local socket connection. Run application outside sandbox or deploy to Databricks Apps."
        elif cat == "AUTH_ERROR":
            hint = "Check your SMTP_USERNAME and SMTP_PASSWORD. For Gmail, generate and use a 16-character Google App Password."
        elif cat == "CONNECTION_TIMEOUT":
            hint = f"Connection to {cfg['host']}:{cfg['port']} timed out. Check firewall and ensure port is open."
        else:
            hint = "Check your SMTP host, port, and security settings."

        return {
            "connected": False,
            "provider": cfg["provider"],
            "message": f"Connection test failed: {raw_err}",
            "hint": hint,
            "config": {k: (v if k != "password" else "***") for k, v in cfg.items()},
        }


def send_test_email(recipient_email: str, user: str = "admin") -> Dict[str, Any]:
    """
    Sends an actual test verification email to the specified recipient.
    """
    is_valid = validate_email_address(recipient_email)
    if not is_valid:
        return {
            "success": False,
            "status": "FAILED",
            "message": f"Invalid recipient email address format: '{recipient_email}'",
        }

    cfg = get_smtp_config()
    subject, plain, html = build_test_email_content(
        recipient=recipient_email,
        user=user,
        provider=cfg["provider"],
    )

    res = send_email(
        recipients=[recipient_email],
        subject=subject,
        body_text=plain,
        body_html=html,
        max_retries=1,
    )

    if res["success"]:
        return {
            "success": True,
            "status": "SENT",
            "message": f"Test email successfully dispatched to {recipient_email} via {cfg['provider']}.",
            "details": res,
        }
    else:
        return {
            "success": False,
            "status": "FAILED",
            "message": f"Test email delivery failed: {res.get('error')}",
            "details": res,
        }


# -------------------------------------------------------------------------
# Notification Background Queue & Asynchronous Processing Daemon
# -------------------------------------------------------------------------

class NotificationQueueService:
    """
    Background worker service that processes email notifications asynchronously
    and retries failed transient notifications with exponential backoff.
    """
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.queue = queue.Queue()
        self.running = False
        self.worker_thread = None

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def start(self):
        if self.running:
            return
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="DQNotificationWorker")
        self.worker_thread.start()
        logger.info("[DQ-NotificationQueue] Worker thread started.")

    def stop(self):
        self.running = False

    def enqueue_notification(self, task: Dict[str, Any]):
        """
        Enqueues an email dispatch task.
        task schema:
        {
            "alert_ids": List[int],
            "execution_id": str,
            "project_id": int,
            "project_name": str,
            "recipients": List[str],
            "subject": str,
            "plain_text": str,
            "html_body": str,
            "callback": Callable[[Dict[str, Any]], None] (optional)
        }
        """
        self.queue.put(task)

    def _worker_loop(self):
        while self.running:
            try:
                task = self.queue.get(timeout=2.0)
            except queue.Empty:
                continue

            try:
                recipients = task.get("recipients", [])
                subject = task.get("subject", "")
                plain = task.get("plain_text", "")
                html = task.get("html_body", "")

                res = send_email(
                    recipients=recipients,
                    subject=subject,
                    body_text=plain,
                    body_html=html,
                    max_retries=2,
                )

                cb = task.get("callback")
                if cb and callable(cb):
                    cb(res)

            except Exception as e:
                logger.error(f"[DQ-NotificationQueue] Unhandled error processing notification: {e}")
            finally:
                self.queue.task_done()


# Global queue singleton
notification_queue = NotificationQueueService.get_instance()
