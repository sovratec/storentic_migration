"""
logger.py — Logging setup for Storentic migration.
Writes to console and to logs/ directory with timestamped filenames.
"""

import logging
import csv
import os
import sys
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────────────
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Derive script name from the entry-point so every script gets its own files.
# e.g.  migrate_customers.py  →  "migrate_customers"
_script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0] or "migration"

RUN_TS        = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE      = os.path.join(LOGS_DIR, f"migration_{_script_name}_{RUN_TS}.log")
ERROR_CSV     = os.path.join(LOGS_DIR, f"errors_{_script_name}_{RUN_TS}.csv")
SKIPPED_CSV   = os.path.join(LOGS_DIR, f"skipped_{_script_name}_{RUN_TS}.csv")
SUMMARY_FILE  = os.path.join(LOGS_DIR, f"summary_{_script_name}_{RUN_TS}.txt")

# ── Logger ─────────────────────────────────────────────────────────────────────
sys.stdout.reconfigure(encoding="utf-8")

_fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s")

# File handler — full detail (INFO+), never shown on screen
_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(_fmt)

# Console handler — CRITICAL only (silent in normal runs).
# All warnings, errors, and info go to the log file.
# Use print() directly for user-facing progress/summary output.
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.CRITICAL)
_console_handler.setFormatter(_fmt)

logger = logging.getLogger("storentic_migration")
logger.setLevel(logging.INFO)
logger.addHandler(_file_handler)
logger.addHandler(_console_handler)
logger.propagate = False  # don't double-log via root logger

# ── Error CSV writer ───────────────────────────────────────────────────────────
_error_file = open(ERROR_CSV, "w", newline="", encoding="utf-8")
_error_writer = csv.DictWriter(
    _error_file,
    fieldnames=["timestamp", "row_number", "unit_name", "field", "error", "original_value"],
)
_error_writer.writeheader()


def log_error(row_number: int, unit_name, field: str, error: str, original_value=None):
    """Log an unexpected row-level error to the main log and errors CSV."""
    logger.warning(f"Row {row_number} | unit={unit_name} | field={field} | {error} | value={original_value!r}")
    _error_writer.writerow({
        "timestamp":      datetime.now().isoformat(),
        "row_number":     row_number,
        "unit_name":      unit_name,
        "field":          field,
        "error":          error,
        "original_value": original_value,
    })
    _error_file.flush()


# ── Skipped CSV writer ─────────────────────────────────────────────────────────
_skipped_file   = open(SKIPPED_CSV, "w", newline="", encoding="utf-8")
_skipped_writer = csv.DictWriter(
    _skipped_file,
    fieldnames=["timestamp", "row_number", "record_id", "field", "skip_reason", "extra_info"],
)
_skipped_writer.writeheader()


def log_skipped(row_number: int, record_id, field: str, reason: str, extra_info=None):
    """Log an intentionally skipped record (FK not found, required field missing, etc.)."""
    logger.warning(f"SKIPPED Row {row_number} | id={record_id} | field={field} | {reason} | extra={extra_info!r}")
    _skipped_writer.writerow({
        "timestamp":   datetime.now().isoformat(),
        "row_number":  row_number,
        "record_id":   record_id,
        "field":       field,
        "skip_reason": reason,
        "extra_info":  extra_info,
    })
    _skipped_file.flush()


def write_summary(stats: dict):
    """Write a migration summary to the summary file and console."""
    mode = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 60,
        "  STORENTIC UNITS MIGRATION — SUMMARY",
        f"  Run timestamp : {RUN_TS}",
        f"  Output mode   : {mode}",
        "=" * 60,
        f"  Total rows read      : {stats.get('total', 0)}",
        f"  Successfully written : {stats.get('inserted', 0)}",
        f"  Updated (--update)   : {stats.get('updated', 0)}",
        f"  Skipped (duplicates) : {stats.get('skipped', 0)}",
        f"  Errors / rejected    : {stats.get('errors', 0)}",
        f"  Dry run mode         : {stats.get('dry_run', False)}",
        "=" * 60,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output : {stats['excel_output']}")
    lines += [
        f"  Log file     : {LOG_FILE}",
        f"  Error CSV    : {ERROR_CSV}",
        f"  Skipped CSV  : {SKIPPED_CSV}",
        f"  Summary      : {SUMMARY_FILE}",
        "=" * 60,
    ]
    text = "\n".join(lines)
    print("\n" + text)
    logger.info(text)
    with open(SUMMARY_FILE, "w") as f:
        f.write(text + "\n")


def close():
    _error_file.close()
    _skipped_file.close()
