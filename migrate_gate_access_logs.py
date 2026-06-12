"""
migrate_gate_access_logs.py — ETL: SiteLink Access Activities → storentic.gate_access_log

Source  : Access_Activities.xlsx (SiteLink export)
Target  : storentic.gate_access_log

Usage:
    python migrate_gate_access_logs.py --file "data/Access_Activities.xlsx" --output db
    python migrate_gate_access_logs.py --file "data/Access_Activities.xlsx" --output db --dry-run
    python migrate_gate_access_logs.py --file "data/Access_Activities.xlsx" --output excel

Arguments:
    --file      Path to Access_Activities.xlsx (required)
    --output    'db' (default) or 'excel'
    --out-file  [excel mode] Output Excel file path
    --dry-run   [db mode] Preview without writing to DB

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    LOCATION_ID   — written to facility_id on every row (required)
    BATCH_SIZE    — rows per execute_values call (default: 500)

Column mapping:
    Occurred           → created_at           (datetime, preserves historical timestamp)
    Description        → pdk_response_status  (VARCHAR — stored as-is e.g. 'Granted')
    AccessPointName    → command              (truncated to 32 chars)
    LastName, FirstName → triggered_by        (e.g. "Crowe, Howie"; max 100 chars)
    Description=='Granted' → success          (True / False)
    Description!='Granted' → error_message    (Description text; NULL when Granted)
    LOCATION_ID env    → facility_id          (required, NOT NULL)
    pdk_device_id      → NULL                 (no source column available)

NOTE:
    pdk_response_status is stored as VARCHAR. Run the following SQL before importing
    if the column is still INTEGER on your target DB:

        ALTER TABLE storentic.gate_access_log
            ALTER COLUMN pdk_response_status TYPE VARCHAR(100);
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── SQL ────────────────────────────────────────────────────────────────────────

_INSERT_COLS = [
    "facility_id", "triggered_by", "pdk_device_id", "command",
    "pdk_response_status", "success", "error_message", "created_at",
]

_INSERT_SQL = """
    INSERT INTO storentic.gate_access_log (
        facility_id,
        triggered_by,
        pdk_device_id,
        command,
        pdk_response_status,
        success,
        error_message,
        created_at
    ) VALUES %s
"""

EXCEL_OUTPUT_COLUMNS = [
    "facility_id", "triggered_by", "pdk_device_id", "command",
    "pdk_response_status", "success", "error_message", "created_at",
]


# ── Row transformer ────────────────────────────────────────────────────────────

def _parse_dt(val):
    """Parse Occurred datetime — returns Python datetime or None."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    try:
        ts = pd.to_datetime(val)
        return None if pd.isna(ts) else ts.to_pydatetime()
    except Exception:
        return None


def transform_row(row_idx: int, row: pd.Series, facility_id: int) -> dict | None:
    """Map one Access Activities row to a gate_access_log insert dict."""

    # ── Occurred → created_at ─────────────────────────────────────────────────
    created_at = _parse_dt(row.get("OCCURRED"))
    if created_at is None:
        log_skipped(row_idx + 2, str(row.get("OCCURRED")), "OCCURRED",
                    "Occurred is blank or unparseable — row rejected", None)
        return None

    # ── Description ───────────────────────────────────────────────────────────
    description = str(row.get("DESCRIPTION") or "").strip()

    # ── AccessPointName → command (VARCHAR 32) ────────────────────────────────
    raw_command = str(row.get("ACCESSPOINTNAME") or "").strip()
    command     = raw_command[:32] if raw_command else None

    # ── triggered_by: "LastName, FirstName" (VARCHAR 100) ────────────────────
    first_name = str(row.get("ACCESSORFIRSTNAME") or "").strip()
    last_name  = str(row.get("ACCESSORLASTNAME")  or "").strip()
    if last_name and first_name:
        triggered_by = f"{last_name}, {first_name}"
    elif last_name:
        triggered_by = last_name
    elif first_name:
        triggered_by = first_name
    else:
        triggered_by = None
    if triggered_by:
        triggered_by = triggered_by[:100]

    # ── success and error_message from Description ────────────────────────────
    is_granted    = description.lower() == "granted"
    success       = is_granted
    error_message = None if is_granted else (description or None)

    return {
        "facility_id":         facility_id,
        "triggered_by":        triggered_by,
        "pdk_device_id":       None,
        "command":             command,
        "pdk_response_status": description or None,   # VARCHAR — stored as-is
        "success":             success,
        "error_message":       error_message,
        "created_at":          created_at,
    }


# ── Excel writer ───────────────────────────────────────────────────────────────

def _write_excel(records: list[dict], out_path: str):
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="GateAccessLogs")
        ws = writer.sheets["GateAccessLogs"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(c.value)) if c.value is not None else 0 for c in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    logger.info(f"📊  Excel written: {out_path}  ({len(records):,} rows)")
    print(f"  Excel output: {out_path}  ({len(records):,} rows)", flush=True)


# ── Summary ────────────────────────────────────────────────────────────────────

def _print_summary(stats: dict, run_ts: str):
    from scripts.logger import LOG_FILE, ERROR_CSV
    mode  = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 60,
        "  STORENTIC GATE ACCESS LOG MIGRATION — SUMMARY",
        f"  Run timestamp : {run_ts}",
        f"  Output mode   : {mode}",
        f"  Dry run       : {stats.get('dry_run', False)}",
        "=" * 60,
        f"  Total rows read    : {stats.get('total', 0):,}",
        f"  Successfully written: {stats.get('inserted', 0):,}",
        f"  Errors / rejected  : {stats.get('errors', 0):,}",
        "=" * 60,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output  : {stats['excel_output']}")
    lines += [f"  Log file      : {LOG_FILE}", f"  Error CSV     : {ERROR_CSV}", "=" * 60]
    text = "\n".join(lines)
    print("\n" + text)
    logger.info(text)


# ── Argument parsing ───────────────────────────────────────────────────────────

def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="SiteLink Access Activities → storentic.gate_access_log migration"
    )
    parser.add_argument("--file",     required=True, help="Path to Access_Activities.xlsx")
    parser.add_argument("--output",   default="db",  choices=["db", "excel"],
                        help="Output destination: 'db' (default) or 'excel'")
    parser.add_argument("--out-file", default=None,  help="[excel mode] Output file path")
    parser.add_argument("--dry-run",  action="store_true",
                        help="[db mode] Preview without writing to DB")
    return parser.parse_args(args)


# ── Main ───────────────────────────────────────────────────────────────────────

def main(args=None):
    if args is None:
        args = parse_args()

    run_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_mode = args.output
    dry_run     = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    facility_id = int(os.getenv("LOCATION_ID", 1))
    batch_size  = int(os.getenv("BATCH_SIZE", 500))

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"gate_access_logs_{ts}.xlsx")

    print(
        f"\n  Gate Access Log Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}"
        f"  |  facility_id={facility_id}\n",
        flush=True,
    )
    logger.info("=" * 60)
    logger.info("GATE ACCESS LOG ETL MIGRATION STARTING")
    logger.info(f"File        : {args.file}")
    logger.info(f"Output mode : {output_mode.upper()}")
    logger.info(f"Dry run     : {dry_run}")
    logger.info(f"Facility ID : {facility_id}")
    logger.info(f"Batch size  : {batch_size}")
    logger.info("=" * 60)

    # ── Load source file ──────────────────────────────────────────────────────
    logger.info(f"📂  Loading source file: {args.file}")
    if args.file.lower().endswith(".csv"):
        df = pd.read_csv(args.file, dtype=str, encoding="utf-8")
    else:
        df = pd.read_excel(args.file, dtype=str)
    df.columns = df.columns.str.strip().str.upper()
    df = df.fillna("").apply(lambda c: c.str.strip() if c.dtype == "object" else c)

    required = {"OCCURRED", "DESCRIPTION", "ACCESSPOINTNAME"}
    missing  = required - set(df.columns)
    if missing:
        print(f"\n❌  Missing required columns: {missing}\n", flush=True)
        sys.exit(1)

    # Drop rows with no Occurred value
    df = df[df["OCCURRED"].str.strip() != ""].copy().reset_index(drop=True)
    logger.info(f"    Total rows with Occurred: {len(df):,}")

    # ── DB connection ─────────────────────────────────────────────────────────
    engine = None
    if output_mode == "db" and not dry_run:
        try:
            from scripts.db import get_engine
            engine = get_engine()
            logger.info("✅  Database connection established.")
        except Exception as exc:
            print(f"\n  ERROR: Cannot connect to DB: {exc}\n", flush=True)
            sys.exit(1)

    # ── Process rows ──────────────────────────────────────────────────────────
    stats = {
        "total": 0, "inserted": 0, "errors": 0,
        "dry_run": dry_run, "output_mode": output_mode,
    }
    excel_records = []
    insert_tuples: list[tuple] = []

    for row_idx, row in enumerate(df.itertuples(index=False)):
        stats["total"] += 1
        row_series = pd.Series(row._asdict())

        record = transform_row(row_idx, row_series, facility_id)
        if record is None:
            stats["errors"] += 1
            continue

        if output_mode == "excel":
            excel_records.append(record)
            stats["inserted"] += 1
            continue

        if dry_run:
            logger.debug(
                f"[DRY RUN] facility={facility_id}  triggered_by={record['triggered_by']}"
                f"  command={record['command']}  occurred={record['created_at']}"
            )
            stats["inserted"] += 1
            continue

        insert_tuples.append(tuple(record[c] for c in _INSERT_COLS))

    # ── Bulk INSERT ───────────────────────────────────────────────────────────
    if insert_tuples and not dry_run and output_mode == "db":
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_values(cur, _INSERT_SQL, insert_tuples, page_size=batch_size)
            raw_conn.commit()
        except Exception as exc:
            raw_conn.rollback()
            logger.error(f"Bulk insert failed: {exc}")
            print(f"\n  ❌  Bulk insert failed: {exc}\n", flush=True)
            raise
        finally:
            raw_conn.close()
        stats["inserted"] = len(insert_tuples)
        logger.info(f"✅  Bulk inserted {stats['inserted']:,} gate access log rows.")
        print(f"\n  ✅  Inserted {stats['inserted']:,} rows into gate_access_log.\n", flush=True)

    # ── Excel output ──────────────────────────────────────────────────────────
    if output_mode == "excel":
        if excel_records:
            _write_excel(excel_records, out_file)
            stats["excel_output"] = out_file
        else:
            logger.warning("⚠️   No records to write.")

    _print_summary(stats, run_ts)
    close_logger()


if __name__ == "__main__":
    main()
