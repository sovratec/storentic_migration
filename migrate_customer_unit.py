"""
migrate_customer_unit.py — ETL script for loading legacy customer_unit data.

Usage (standalone):
    python migrate_customer_unit.py --file data/CustomerUnitSample.xlsx --output db
    python migrate_customer_unit.py --file data/CustomerUnitSample.xlsx --output db --dry-run
    python migrate_customer_unit.py --file data/CustomerUnitSample.xlsx --output db --update
    python migrate_customer_unit.py --file data/CustomerUnitSample.xlsx --output excel

Usage (via unified entry point — preferred):
    python migrate.py --type customer_unit --file data/CustomerUnitSample.xlsx [--output db|excel] [--dry-run] [--update]

Arguments:
    --file      Path to the source Excel file (required)
    --output    Destination: 'db' (default) or 'excel'
    --out-file  [excel mode only] Output Excel filename
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    LOCATION_ID     — required; scopes the unit_map lookup and is not written to the row
    CREATED_BY      — user id recorded in created_by / updated_by
    DRY_RUN         — alternative to --dry-run flag
    UPDATE_EXISTING — alternative to --update flag

Dedup key  : customer_id + unit_id (composite — one active link per customer-unit pair)
Dependency : storentic.customer and storentic.units must be loaded before running this script.

Field mapping summary:
    customer_id             ← TenantId     (lookup → storentic.customer.external_id)
    unit_id                 ← sUnitName    (lookup → storentic.units.unit_number + LOCATION_ID)
    lease_date              ← dLease       (fallback: dMovedIn)
    move_out_date           ← dMovedOut    (NULL for all 377 active rows)
    paid_through_date       ← dPaidThru
    scheduled_date          ← dSchedOut    (NULL for all 377 active rows)
    rental_rate             ← dcRent       (DECIMAL dollars — NOT cents)
    rate_variance           ← hardcoded 0.00
    scheduled_rate          ← dcSchedRent  (copy rental_rate if 0 or null)
    charge_day              ← dMovedIn     (day-of-month, e.g. July 27 → 27)
    access_code             ← sAccessCode  (truncated to 10 chars)
    rental_contract_signed  ← hardcoded False
"""

import argparse
import os
import sys

import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, write_summary, close as close_logger
from scripts.validator import validate
from scripts import customer_unit_transformer as T

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Column short-key constants ─────────────────────────────────────────────────
# Must match the keys in REQUIRED_CUSTOMER_UNIT_COLUMN_KEYS in validator.py.

COL_TENANT_ID   = "TenantId"
COL_UNIT_NAME   = "sUnitName"
COL_MOVED_IN    = "dMovedIn"
COL_LEASE       = "dLease"
COL_PAID_THRU   = "dPaidThru"
COL_MOVED_OUT   = "dMovedOut"
COL_SCHED_OUT   = "dSchedOut"
COL_RENT        = "dcRent"
COL_SCHED_RENT  = "dcSchedRent"
COL_ACCESS_CODE = "sAccessCode"


# ── Lookup caches ──────────────────────────────────────────────────────────────

def load_customer_map(conn, location_id: int) -> dict:
    """
    Pre-load all customers with a known external_id for this location into memory.
    Returns {external_id (str): customer.id (int)}.
    Eliminates per-row SELECT queries against storentic.customer.
    """
    from sqlalchemy import text as sa_text
    rows = conn.execute(sa_text(
        "SELECT id, external_id FROM storentic.customer "
        "WHERE external_id IS NOT NULL AND location_id = :loc"
    ), {"loc": location_id}).fetchall()
    result = {str(row.external_id).strip(): row.id for row in rows}
    logger.info(f"📋  customer_map loaded: {len(result)} entries")
    return result


def load_unit_map(conn, location_id: int) -> dict:
    """
    Pre-load all units for this location into memory.
    Returns {unit_number (str): units.id (int)}.
    Eliminates per-row SELECT queries against storentic.units.
    """
    from sqlalchemy import text as sa_text
    rows = conn.execute(sa_text(
        "SELECT id, unit_number FROM storentic.units WHERE location_id = :loc"
    ), {"loc": location_id}).fetchall()
    result = {str(row.unit_number).strip(): row.id for row in rows}
    logger.info(f"📋  unit_map loaded: {len(result)} entries")
    return result


# ── Row transformer ────────────────────────────────────────────────────────────

def transform_row(
    row_idx: int,
    row: pd.Series,
    col: dict,
    customer_map: dict,
    unit_map: dict,
    created_by: int,
) -> dict | None:
    """
    Transform one Excel row into a dict ready for storentic.customer_unit insertion.
    Returns None if the row must be skipped (critical field missing or FK not found).

    Skip conditions (row rejected, error logged):
        - TenantId not found in customer_map
        - sUnitName not found in unit_map
        - dMovedIn is null or unparseable  (charge_day cannot be derived)
        - dcRent is null or non-numeric    (rental_rate is required)

    Non-skip conditions (safe defaults applied):
        - dLease null         → falls back to dMovedIn
        - dcSchedRent 0/null  → copied from rental_rate
        - dMovedOut null      → stored as NULL  (expected for all active rows)
        - dSchedOut null      → stored as NULL  (expected for all active rows)
        - sAccessCode > 10    → silently truncated to 10 characters
    """
    excel_row = row_idx + 2   # +1 for 1-indexing, +1 for header row

    def get(key):
        col_name = col.get(key)
        return row.get(col_name) if col_name else None

    # ── FK: customer_id ───────────────────────────────────────────────────────
    tenant_id = str(get(COL_TENANT_ID) or "").strip()
    customer_id = customer_map.get(tenant_id)
    if not customer_id:
        log_skipped(excel_row, tenant_id, COL_TENANT_ID,
                    f"customer_id not found for TenantId={tenant_id!r}", tenant_id)
        return None

    # ── FK: unit_id ───────────────────────────────────────────────────────────
    unit_name = str(get(COL_UNIT_NAME) or "").strip()
    unit_id = unit_map.get(unit_name)
    if not unit_id:
        log_skipped(excel_row, unit_name, COL_UNIT_NAME,
                    f"unit_id not found for sUnitName={unit_name!r}", unit_name)
        return None

    # ── dMovedIn (required — drives charge_day and lease_date fallback) ───────
    move_in_date = T.parse_date(get(COL_MOVED_IN))
    if move_in_date is None:
        log_skipped(excel_row, unit_name, COL_MOVED_IN,
                    "dMovedIn is null or unparseable — charge_day cannot be derived",
                    get(COL_MOVED_IN))
        return None

    # ── dcRent (required) ─────────────────────────────────────────────────────
    rent_raw = get(COL_RENT)
    if rent_raw is None or str(rent_raw).strip().lower() in ("nan", "none", ""):
        log_skipped(excel_row, unit_name, COL_RENT,
                    "dcRent is null or non-numeric — rental_rate is required", rent_raw)
        return None
    rental_rate = T.safe_decimal(rent_raw)

    # ── lease_date: dLease → fallback to dMovedIn ─────────────────────────────
    lease_date = T.parse_date(get(COL_LEASE)) or move_in_date

    # ── scheduled_rate: dcSchedRent → copy rental_rate if 0 or null ──────────
    scheduled_rate = T.safe_decimal(get(COL_SCHED_RENT))
    if scheduled_rate == 0.0:
        scheduled_rate = rental_rate

    return {
        # Foreign keys
        "customer_id":                  customer_id,
        "unit_id":                      unit_id,
        # Dates
        "lease_date":                   lease_date,
        "move_out_date":                T.parse_date(get(COL_MOVED_OUT)),   # NULL for all active rows
        "paid_through_date":            T.parse_date(get(COL_PAID_THRU)),
        "scheduled_date":               T.parse_date(get(COL_SCHED_OUT)),   # NULL for all active rows
        # Rates — stored as DECIMAL dollars (NOT cents, unlike rental_agreements)
        "rental_rate":                  round(rental_rate, 2),
        "rate_variance":                0.00,                               # hardcoded — no source
        "scheduled_rate":               round(scheduled_rate, 2),
        # Derived
        "charge_day":                   move_in_date.day,
        # Access
        "access_code":                  T.trim_access_code(get(COL_ACCESS_CODE)),
        # Hardcoded — no source data in legacy system
        "rental_contract_signed":       False,
        # Audit
        "created_by":                   created_by,
        "updated_by":                   created_by,
        "created_datetime":             datetime.utcnow(),
        "updated_datetime":             datetime.utcnow(),
    }


# ── Duplicate check ────────────────────────────────────────────────────────────

def customer_unit_exists(conn, customer_id: int, unit_id: int, move_out_date) -> bool:
    """
    Check if a customer_unit record already exists for this customer-unit pair.
    Dedup key: customer_id + unit_id (composite).
    """
    from sqlalchemy import text as sa_text
    if move_out_date is None :
        result = conn.execute(sa_text(
            "SELECT id FROM storentic.customer_unit "
            "WHERE customer_id = :cid AND unit_id = :uid "
            "LIMIT 1"
        ), {"cid": customer_id, "uid": unit_id})
    else :
        result = conn.execute(sa_text(
            "SELECT id FROM storentic.customer_unit "
            "WHERE customer_id = :cid AND unit_id = :uid  AND move_out_date = :move_out_date "
            "LIMIT 1"
        ), {"cid": customer_id, "uid": unit_id})
    return result.fetchone() is not None


# ── SQL statements ─────────────────────────────────────────────────────────────

_INSERT_SQL = """
    INSERT INTO storentic.customer_unit (
        customer_id,
        unit_id,
        lease_date,
        move_out_date,
        paid_through_date,
        scheduled_date,
        rental_rate,
        rate_variance,
        scheduled_rate,
        charge_day,
        access_code,
        rental_contract_signed,
        created_by,
        updated_by,
        created_datetime,
        updated_datetime
    ) VALUES (
        :customer_id,
        :unit_id,
        :lease_date,
        :move_out_date,
        :paid_through_date,
        :scheduled_date,
        :rental_rate,
        :rate_variance,
        :scheduled_rate,
        :charge_day,
        :access_code,
        :rental_contract_signed,
        :created_by,
        :updated_by,
        :created_datetime,
        :updated_datetime
    )
"""

_UPDATE_SQL = """
    UPDATE storentic.customer_unit SET
        lease_date              = :lease_date,
        paid_through_date       = :paid_through_date,
        rental_rate             = :rental_rate,
        scheduled_rate          = :scheduled_rate,
        charge_day              = :charge_day,
        access_code             = :access_code,
        updated_by              = :updated_by,
        updated_datetime        = :updated_datetime
    WHERE customer_id = :customer_id AND unit_id = :unit_id
"""


# ── Excel output ───────────────────────────────────────────────────────────────

EXCEL_OUTPUT_COLUMNS = [
    "customer_id",
    "unit_id",
    "lease_date",
    "move_out_date",
    "paid_through_date",
    "scheduled_date",
    "rental_rate",
    "rate_variance",
    "scheduled_rate",
    "charge_day",
    "access_code",
    "rental_contract_signed",
    "created_by",
    "updated_by",
]


def write_to_excel(records: list[dict], out_path: str):
    """Write transformed records to an Excel file for QA review before DB insertion."""
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Transformed CustomerUnit")

        ws = writer.sheets["Transformed CustomerUnit"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)

    logger.info(f"📊  Excel output written: {out_path}  ({len(records)} rows)")


# ── Argument parsing (shared with migrate.py) ──────────────────────────────────

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Storentic Customer-Unit ETL Migration")
    parser.add_argument("--file",     required=True,  help="Path to source Excel file")
    parser.add_argument("--output",   default="db",   choices=["db", "excel"],
                        help="Output destination: 'db' (default) or 'excel'")
    parser.add_argument("--out-file", default=None,   help="[excel mode] Output file path")
    parser.add_argument("--dry-run",  action="store_true", help="[db mode] Preview without writing to DB")
    parser.add_argument("--update",   action="store_true", help="[db mode] Update existing records instead of skipping")
    return parser.parse_args(args)


# ── Main ───────────────────────────────────────────────────────────────────────

def main(args=None):
    if args is None:
        args = parse_args()

    output_mode     = args.output
    dry_run         = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    update_existing = args.update  or os.getenv("UPDATE_EXISTING", "false").lower() == "true"
    loc_id          = int(os.getenv("LOCATION_ID", 1))
    created_by      = int(os.getenv("CREATED_BY", 0))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"customer_unit_transformed_{ts}.xlsx")

    logger.info("=" * 60)
    logger.info("  STORENTIC CUSTOMER-UNIT ETL MIGRATION STARTING")
    logger.info(f"  File          : {args.file}")
    logger.info(f"  Output mode   : {output_mode.upper()}")
    if output_mode == "db":
        logger.info(f"  Dry run       : {dry_run}")
        logger.info(f"  Update mode   : {update_existing}")
    else:
        logger.info(f"  Excel output  : {out_file}")
    logger.info(f"  Location      : {loc_id}")
    logger.info(f"  Created by    : {created_by}")
    logger.info("=" * 60)

    # ── Step 1: Validate ──────────────────────────────────────────────────────
    df, col_map = validate(args.file, mode="customer_unit")

    # ── Step 2: Load lookup caches ────────────────────────────────────────────
    engine = None
    customer_map = {}
    unit_map = {}

    if output_mode == "db" and not dry_run:
        try:
            from scripts.db import get_engine
            engine = get_engine()
            logger.info("✅  Database connection established.")
            with engine.connect() as conn:
                customer_map = load_customer_map(conn, loc_id)
                unit_map     = load_unit_map(conn, loc_id)
                
        except ImportError:
            logger.error("❌  sqlalchemy / psycopg2 not installed. Run: pip install sqlalchemy psycopg2-binary")
            sys.exit(1)
    else:
        logger.info("ℹ️   Dry-run / Excel mode: skipping DB connection and lookup cache load.")

    # ── Step 3: Process rows ──────────────────────────────────────────────────
    stats = {
        "total": 0, "inserted": 0, "updated": 0,
        "skipped": 0, "errors": 0, "dry_run": dry_run,
        "output_mode": output_mode,
    }
    excel_records = []

    for row_idx, row in df.iterrows():
        stats["total"] += 1

        record = transform_row(row_idx, row, col_map, customer_map, unit_map, created_by)

        if record is None:
            stats["errors"] += 1
            continue

        customer_id = record["customer_id"]
        unit_id     = record["unit_id"]
        move_out_date = record["move_out_date"]
        display_key = f"customer_id={customer_id}, unit_id={unit_id}"

        # ── Excel output mode ─────────────────────────────────────────────────
        if output_mode == "excel":
            excel_records.append(record)
            logger.info(f"📋  Queued for Excel: {display_key}")
            stats["inserted"] += 1
            continue

        # ── DB dry run ────────────────────────────────────────────────────────
        if dry_run:
            logger.info(f"[DRY RUN] Would upsert customer_unit: {display_key}")
            stats["inserted"] += 1
            continue

        # ── Live DB write ─────────────────────────────────────────────────────
        try:
            from sqlalchemy import text as sa_text
            with engine.begin() as conn:
                exists = customer_unit_exists(conn, customer_id, unit_id, move_out_date)

                if exists and not update_existing:
                    logger.info(f"⏭️   Skipped duplicate: {display_key}")
                    stats["skipped"] += 1

                elif exists:
                    conn.execute(sa_text(_UPDATE_SQL), record)
                    logger.info(f"🔄  Updated: {display_key}")
                    stats["updated"] += 1

                else:
                    conn.execute(sa_text(_INSERT_SQL), record)
                    logger.info(f"✅  Inserted: {display_key}")
                    stats["inserted"] += 1

        except Exception as e:
            log_error(row_idx + 2, display_key, "DB_UPSERT", str(e), display_key)
            stats["errors"] += 1

    # ── Step 4: Flush Excel file ──────────────────────────────────────────────
    if output_mode == "excel":
        if excel_records:
            write_to_excel(excel_records, out_file)
            stats["excel_output"] = out_file
        else:
            logger.warning("⚠️   No records to write — Excel file not created.")

    # ── Step 5: Summary ───────────────────────────────────────────────────────
    write_summary(stats)
    close_logger()


if __name__ == "__main__":
    main()
