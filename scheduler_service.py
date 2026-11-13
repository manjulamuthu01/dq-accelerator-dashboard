"""
DQ Accelerator - Persistent Enterprise Scheduler Service
Handles schedule calculation, recurrence patterns, cron evaluation,
and autonomous background job execution independent of browser sessions.
"""

import os
import sys
import time
import threading
import datetime
import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("dq_scheduler")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [DQ-Scheduler] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

WEEKDAY_MAP = {
    "MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6,
    "MONDAY": 0, "TUESDAY": 1, "WEDNESDAY": 2, "THURSDAY": 3, "FRIDAY": 4, "SATURDAY": 5, "SUNDAY": 6
}
WEEKDAY_REV_MAP = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}


def parse_datetime_safe(dt_str: Optional[str]) -> Optional[datetime.datetime]:
    if not dt_str:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%d %b %Y, %I:%M %p",
        "%d %b %Y, %H:%M",
        "%d %b %Y"
    ):
        try:
            return datetime.datetime.strptime(str(dt_str).strip(), fmt)
        except ValueError:
            continue
    return None


def calculate_next_run(
    frequency: str,
    start_date: str,
    start_time: str,
    weekdays: Optional[str] = None,
    day_of_month: Optional[int] = None,
    repeat_interval: int = 1,
    cron_expression: Optional[str] = None,
    end_date: Optional[str] = None,
    timezone_str: str = "UTC",
    after_dt: Optional[datetime.datetime] = None
) -> Optional[str]:
    """
    Calculates the exact next execution timestamp (formatted as 'YYYY-MM-DD HH:MM:SS').
    Returns None if schedule has expired or is invalid.
    """
    now = after_dt or datetime.datetime.now()
    repeat_interval = max(1, int(repeat_interval or 1))

    # Parse base time
    try:
        t_parts = start_time.strip().split(":")
        hour = int(t_parts[0])
        minute = int(t_parts[1]) if len(t_parts) > 1 else 0
    except Exception:
        hour, minute = 0, 0

    freq_upper = (frequency or "DAILY").strip().upper()

    # For recurring schedules, if end_date was set on or before start_date, ignore it as an accidental constraint
    if freq_upper != "ONCE" and end_date and str(end_date).strip():
        e_check = parse_datetime_safe(end_date)
        s_check = parse_datetime_safe(start_date)
        if e_check and s_check and e_check.date() <= s_check.date():
            end_date = None

    # 1. ONCE
    if freq_upper == "ONCE":
        s_dt = parse_datetime_safe(f"{start_date} {start_time}")
        if not s_dt:
            s_dt = parse_datetime_safe(start_date)
            if s_dt:
                s_dt = s_dt.replace(hour=hour, minute=minute, second=0)
        if s_dt:
            if after_dt and s_dt <= after_dt:
                return None
            return s_dt.strftime("%Y-%m-%d %H:%M:%S")
        return None

    # 2. HOURLY
    if freq_upper == "HOURLY":
        # Start from base date & time
        s_dt = parse_datetime_safe(f"{start_date} {start_time}")
        if not s_dt:
            s_dt = now.replace(minute=minute, second=0)
        cand = s_dt
        while cand <= now:
            cand += datetime.timedelta(hours=repeat_interval)
        if end_date and str(end_date).strip():
            e_dt = parse_datetime_safe(end_date)
            if e_dt and cand.date() > e_dt.date():
                return None
        return cand.strftime("%Y-%m-%d %H:%M:%S")

    # 3. DAILY
    if freq_upper == "DAILY":
        # Candidate today at hour:minute
        cand = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if cand <= now:
            cand += datetime.timedelta(days=repeat_interval)
        # Verify against start_date
        s_date_dt = parse_datetime_safe(start_date)
        if s_date_dt and cand.date() < s_date_dt.date():
            cand = s_date_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if cand <= now:
                cand += datetime.timedelta(days=repeat_interval)
        if end_date and str(end_date).strip():
            e_dt = parse_datetime_safe(end_date)
            if e_dt and cand.date() > e_dt.date():
                return None
        return cand.strftime("%Y-%m-%d %H:%M:%S")

    # 4. WEEKLY
    if freq_upper == "WEEKLY":
        target_days = []
        if weekdays:
            for item in weekdays.split(","):
                clean = item.strip().upper()
                if clean in WEEKDAY_MAP:
                    target_days.append(WEEKDAY_MAP[clean])
        if not target_days:
            target_days = [0]  # default to Monday

        # Find the next matching weekday
        cand = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if cand <= now:
            cand += datetime.timedelta(days=1)
        for _ in range(21):  # search up to 3 weeks ahead
            if cand.weekday() in target_days:
                s_date_dt = parse_datetime_safe(start_date)
                if s_date_dt and cand.date() < s_date_dt.date():
                    cand += datetime.timedelta(days=1)
                    continue
                if end_date and str(end_date).strip():
                    e_dt = parse_datetime_safe(end_date)
                    if e_dt and cand.date() > e_dt.date():
                        return None
                return cand.strftime("%Y-%m-%d %H:%M:%S")
            cand += datetime.timedelta(days=1)
        return None

    # 5. MONTHLY
    if freq_upper == "MONTHLY":
        dom = int(day_of_month or 1)
        dom = max(1, min(dom, 31))
        # Candidate this month
        year = now.year
        month = now.month
        # Try candidate in current month
        try:
            cand = datetime.datetime(year, month, dom, hour, minute, 0)
        except ValueError:
            # Handle months with fewer days (e.g. Feb 30 -> Feb 28)
            cand = datetime.datetime(year, month, 28, hour, minute, 0)
        if cand <= now:
            # Advance to next month
            month += repeat_interval
            while month > 12:
                year += 1
                month -= 12
            try:
                cand = datetime.datetime(year, month, dom, hour, minute, 0)
            except ValueError:
                cand = datetime.datetime(year, month, 28, hour, minute, 0)
        if end_date and str(end_date).strip():
            e_dt = parse_datetime_safe(end_date)
            if e_dt and cand.date() > e_dt.date():
                return None
        return cand.strftime("%Y-%m-%d %H:%M:%S")

    # 6. CUSTOM CRON
    if freq_upper == "CUSTOM" and cron_expression:
        parts = cron_expression.strip().split()
        if len(parts) >= 5:
            # Basic cron evaluator: step through future minutes
            c_min, c_hr, c_dom, c_mon, c_dow = parts[0], parts[1], parts[2], parts[3], parts[4]
            cand = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
            for _ in range(60 * 24 * 35):  # up to 35 days ahead
                # Check minute
                if c_min != "*" and not _cron_match(cand.minute, c_min):
                    cand += datetime.timedelta(minutes=1)
                    continue
                # Check hour
                if c_hr != "*" and not _cron_match(cand.hour, c_hr):
                    cand = cand.replace(minute=0) + datetime.timedelta(hours=1)
                    continue
                # Check day of month
                if c_dom != "*" and not _cron_match(cand.day, c_dom):
                    cand = cand.replace(hour=0, minute=0) + datetime.timedelta(days=1)
                    continue
                # Check month
                if c_mon != "*" and not _cron_match(cand.month, c_mon):
                    cand = cand.replace(day=1, hour=0, minute=0) + datetime.timedelta(days=28)
                    continue
                # Check day of week
                if c_dow != "*" and not _cron_match(cand.weekday(), c_dow, is_dow=True):
                    cand = cand.replace(hour=0, minute=0) + datetime.timedelta(days=1)
                    continue
                if end_date and str(end_date).strip():
                    e_dt = parse_datetime_safe(end_date)
                    if e_dt and cand.date() > e_dt.date():
                        return None
                return cand.strftime("%Y-%m-%d %H:%M:%S")
        # Fallback to hourly if custom cron parsing fails
        return (now + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    # Default fallback: daily
    cand = (now + datetime.timedelta(days=1)).replace(hour=hour, minute=minute, second=0)
    return cand.strftime("%Y-%m-%d %H:%M:%S")


def _cron_match(val: int, expr: str, is_dow: bool = False) -> bool:
    """Matches an integer against a cron sub-expression like '*', '*/15', '1,3,5', '1-5'."""
    expr = expr.strip()
    if expr == "*":
        return True
    if "/" in expr:
        try:
            base, step = expr.split("/")
            step_int = int(step)
            return (val % step_int) == 0
        except Exception:
            return False
    if "," in expr:
        for sub in expr.split(","):
            if _cron_match(val, sub, is_dow):
                return True
        return False
    if "-" in expr:
        try:
            low, high = expr.split("-")
            return int(low) <= val <= int(high)
        except Exception:
            return False
    try:
        match_val = int(expr)
        if is_dow and match_val == 7:  # Cron Sunday 0 or 7
            match_val = 0
        return val == match_val
    except Exception:
        return False


class DQSchedulerService:
    """
    Autonomous background scheduler daemon that polls dq_schedules
    and dispatches execution jobs when due.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DQSchedulerService, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, check_interval_seconds: int = 15):
        if self._initialized:
            return
        self.check_interval = check_interval_seconds
        self.stop_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._initialized = True

    def start(self):
        with self._lock:
            if self._running and self.worker_thread and self.worker_thread.is_alive():
                logger.info("Scheduler daemon already active.")
                return
            self.stop_event.clear()
            self._running = True
            self.worker_thread = threading.Thread(
                target=self._scheduler_loop,
                name="DQ-Scheduler-Daemon",
                daemon=True
            )
            self.worker_thread.start()
            logger.info("Background persistent scheduler daemon started.")

    def stop(self):
        with self._lock:
            if not self._running:
                return
            self._running = False
            self.stop_event.set()
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=3)
            logger.info("Background persistent scheduler daemon stopped.")

    def is_alive(self) -> bool:
        return self._running and self.worker_thread is not None and self.worker_thread.is_alive()

    def _scheduler_loop(self):
        # Allow database to initialize before first scan
        time.sleep(2)
        while not self.stop_event.is_set():
            try:
                self.check_and_trigger_due_schedules()
            except Exception as e:
                logger.error(f"Error during schedule scan: {e}", exc_info=True)
            self.stop_event.wait(self.check_interval)

    def check_and_trigger_due_schedules(self):
        """Scans dq_schedules for active schedules whose next_run_at has arrived."""
        import db_adapter
        now_dt = datetime.datetime.now()
        now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

        query = """
            SELECT schedule_id, schedule_name, project_id, project_name, dataset_id, dataset_name,
                   trigger_type, frequency, cron_expression, start_date, start_time, timezone,
                   weekdays, day_of_month, repeat_interval, end_date, rules_mode, selected_rules,
                   concurrency_policy
            FROM dq_schedules
            WHERE status = 'ACTIVE'
              AND next_run_at IS NOT NULL
              AND next_run_at <= ?
        """
        try:
            schedules_df = db_adapter.query_df(query, (now_str,))
        except Exception as e:
            # Table might not exist yet if migrations haven't run
            return

        if schedules_df.empty:
            return

        logger.info(f"Discovered {len(schedules_df)} due schedule(s) to execute.")

        for _, s in schedules_df.iterrows():
            sched_id = int(s["schedule_id"])
            sched_name = str(s["schedule_name"])
            proj_name = str(s["project_name"])
            ds_name = str(s["dataset_name"])
            concurrency_policy = str(s.get("concurrency_policy") or "QUEUE").upper()
            rules_mode = str(s.get("rules_mode") or "ALL")
            selected_rules = str(s.get("selected_rules") or "")

            # Calculate next execution time before launch
            next_run = calculate_next_run(
                frequency=s["frequency"],
                start_date=s["start_date"],
                start_time=s["start_time"],
                weekdays=s.get("weekdays"),
                day_of_month=s.get("day_of_month"),
                repeat_interval=s.get("repeat_interval", 1),
                cron_expression=s.get("cron_expression"),
                end_date=s.get("end_date"),
                timezone_str=s.get("timezone", "UTC"),
                after_dt=now_dt
            )
            new_status = "ACTIVE" if next_run else "COMPLETED"

            # Execute validation
            try:
                res = db_adapter.trigger_validation(
                    project_name=proj_name,
                    dataset_name=ds_name,
                    run_type="SCHEDULED",
                    user="SchedulerDaemon",
                    schedule_id=sched_id,
                    rules_mode=rules_mode,
                    selected_rule_ids=selected_rules
                )
                exec_id = res.get("execution_id", "—")
                st = res.get("status", "PASS")
                logger.info(f"Scheduled validation launched: {sched_name} -> {exec_id} ({st})")

                # Update schedule metadata
                db_adapter.execute_cmd("""
                    UPDATE dq_schedules
                    SET last_run_at = ?,
                        last_status = ?,
                        last_execution_id = ?,
                        next_run_at = ?,
                        status = ?,
                        updated_at = ?
                    WHERE schedule_id = ?
                """, (now_str, st, exec_id, next_run, new_status, now_str, sched_id))

                db_adapter.log_config_audit(
                    entity_type="SCHEDULE",
                    entity_id=str(sched_id),
                    action="TRIGGER_SCHEDULED_RUN",
                    old_value=None,
                    new_value={"execution_id": exec_id, "status": st, "next_run_at": next_run},
                    changed_by="SchedulerDaemon"
                )
            except Exception as ex:
                logger.error(f"Failed to trigger scheduled run for '{sched_name}': {ex}", exc_info=True)


# Global singleton access
_scheduler_service_instance: Optional[DQSchedulerService] = None

def get_scheduler_service() -> DQSchedulerService:
    global _scheduler_service_instance
    if _scheduler_service_instance is None:
        _scheduler_service_instance = DQSchedulerService()
    return _scheduler_service_instance

def start_scheduler_daemon():
    svc = get_scheduler_service()
    svc.start()
    return svc

