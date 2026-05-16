"""
migrate_rental_agreements.py — ETL script for loading legacy rental_agreements data.

Usage (standalone) :
    python migrate_rental_agreements.py --file data/CustomerUnitSample.csv --output db
    python migrate_rental_agreements.py --file data/CustomerUnitSample.csv --output db --dry-run
    python migrate_rental_agreements.py --file data/CustomerUnitSample.csv --output db --update
    python migrate_rental_agreements.py --file data/CustomerUnitSample.csv --output excel

Usage (via unified entry point — preferred):
    python migrate.py --type rental_agreement --file data/CustomerUnitSample.csv [--output db|excel] [--dry-run] [--update]

Arguments:
    --file      Path to the source CSV file (required)
    --output    Destination: 'db' (default) or 'excel'
    --out-file  [excel mode only] Output Excel filename
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    LOCATION_ID     — required; used as facility_id and scopes the unit_map lookup
    CREATED_BY      — user id recorded in created_by / updated_by
    DRY_RUN         — alternative to --dry-run flag
    UPDATE_EXISTING — alternative to --update flag

Dedup key  : customer_id + unit_id (composite — one active agreement per customer-unit pair)
Dependency : storentic.customer and storentic.units must be loaded before running this script.

Field mapping summary:
    customer_id               ← TenantId       (lookup → storentic.customer.external_id)
    unit_id                   ← sUnitName      (lookup → storentic.units.unit_number + LOCATION_ID)
    facility_id               ← LOCATION_ID    (env var)
    move_in_date              ← dMovedIn       (required — also drives charge_day)
    lease_date                ← dLease         (fallback: dMovedIn)
    paid_through_date         ← dPaidThru      (required)
    schedule_date             ← dSchedOut      (fallback: dMovedIn)
    move_out_date             ← dMovedOut      (NULL for all 377 active rows)
    rental_rate_in_cents      ← dcRent         (dollars × 100 → cents)
    schedule_rate_in_cents    ← dcSchedRent    (copy rental_rate_in_cents if 0 or null)
    rate_variance_in_cents    ← hardcoded 0
    security_deposit_in_cents ← dcSecDepPaid   (dollars × 100 → cents; 0 if null)
    insurance_rate_in_cents   ← dcInsurPremium (dollars × 100 → cents; 0 if null)
    insurance_option          ← derived        (BASIC if dcInsurPremium > 0, else NONE)
    charge_day                ← dMovedIn       (day-of-month, e.g. July 27 → 27; clamped to 30)
    status                    ← hardcoded ACTIVE
    promo_code                ← hardcoded NULL
    promo_discount_in_cents   ← hardcoded 0
    notes                     ← hardcoded NULL
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
from scripts import rental_agreement_transformer as T

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Column short-key constants ─────────────────────────────────────────────────
# Must match the keys in REQUIRED_RENTAL_AGREEMENT_COLUMN_KEYS in validator.py.

COL_TENANT_ID  = "TenantId"
COL_UNIT_NAME  = "sUnitName"
COL_MOVED_IN   = "dMovedIn"
COL_LEASE      = "dLease"
COL_PAID_THRU  = "dPaidThru"
COL_MOVED_OUT  = "dMovedOut"
COL_SCHED_OUT  = "dSchedOut"
COL_RENT       = "dcRent"
COL_SCHED_RENT = "dcSchedRent"
COL_SEC_DEP    = "dcSecDepPaid"
COL_INSUR_PREM = "dcInsurPremium"


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
    facility_id: int,
    created_by: int,
) -> dict | None:
    """
    Transform one Excel row into a dict ready for storentic.rental_agreements insertion.
    Returns None if the row must be skipped (critical field missing or FK not found).

    Skip conditions (row rejected, error logged):
        - TenantId not found in customer_map
        - sUnitName not found in unit_map
        - dMovedIn is null or unparseable  (move_in_date and charge_day required)
        - dPaidThru is null or unparseable (paid_through_date required)
        - dcRent is null or non-numeric    (rental_rate_in_cents required)

    Non-skip conditions (safe defaults applied):
        - dLease null           → falls back to dMovedIn
        - dSchedOut null        → falls back to dMovedIn
        - dcSchedRent 0/null    → copied from rental_rate_in_cents
        - dMovedOut null        → stored as NULL (expected for all active rows)
        - dcSecDepPaid null     → 0 cents
        - dcInsurPremium null/0 → 0 cents; insurance_option = NONE
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

    # ── dMovedIn (required — drives move_in_date and charge_day) ─────────────
    move_in_date = T.parse_date(get(COL_MOVED_IN))
    if move_in_date is None:
        log_skipped(excel_row, unit_name, COL_MOVED_IN,
                    "dMovedIn is null or unparseable — move_in_date is required",
                    get(COL_MOVED_IN))
        return None

    # ── dPaidThru (required) ──────────────────────────────────────────────────
    paid_through_date = T.parse_date(get(COL_PAID_THRU))
    if paid_through_date is None:
        log_skipped(excel_row, unit_name, COL_PAID_THRU,
                    "dPaidThru is null or unparseable — paid_through_date is required",
                    get(COL_PAID_THRU))
        return None

    # ── dcRent (required) ─────────────────────────────────────────────────────
    rent_raw = get(COL_RENT)
    if rent_raw is None or str(rent_raw).strip().lower() in ("nan", "none", ""):
        log_skipped(excel_row, unit_name, COL_RENT,
                    "dcRent is null or non-numeric — rental_rate_in_cents is required",
                    rent_raw)
        return None
    rental_rate_in_cents = T.to_cents(rent_raw)

    # ── lease_date: dLease → fallback to dMovedIn ─────────────────────────────
    lease_date = T.parse_date(get(COL_LEASE)) or move_in_date

    # ── schedule_date: dSchedOut → fallback to dMovedIn ──────────────────────
    schedule_date = T.parse_date(get(COL_SCHED_OUT)) or move_in_date

    # ── schedule_rate_in_cents: dcSchedRent → copy rental_rate if 0 or null ──
    schedule_rate_in_cents = T.to_cents(get(COL_SCHED_RENT))
    if schedule_rate_in_cents == 0:
        schedule_rate_in_cents = rental_rate_in_cents

    # ── insurance ─────────────────────────────────────────────────────────────
    insurance_rate_in_cents = T.to_cents(get(COL_INSUR_PREM))
    insurance_option = T.derive_insurance_option(insurance_rate_in_cents)

    now = datetime.utcnow()

    return {
        # Foreign keys
        "customer_id":                  customer_id,
        "unit_id":                      unit_id,
        "facility_id":                  facility_id,
        # Dates
        "move_in_date":                 move_in_date,
        "lease_date":                   lease_date,
        "paid_through_date":            paid_through_date,
        "schedule_date":                schedule_date,
        "move_out_date":                T.parse_date(get(COL_MOVED_OUT)),   # NULL for all active rows
        # Rates — stored as integer cents
        "rental_rate_in_cents":         rental_rate_in_cents,
        "schedule_rate_in_cents":       schedule_rate_in_cents,
        "rate_variance_in_cents":       0,                                  # hardcoded — no source
        "security_deposit_in_cents":    T.to_cents(get(COL_SEC_DEP)),
        # Insurance
        "insurance_rate_in_cents":      insurance_rate_in_cents,
        "insurance_option":             insurance_option,
        # Billing
        "charge_day":                   T.derive_charge_day(move_in_date),
        # Status — all source rows have bRented = True
        "status":                       "ACTIVE",
        # Promos — no source data
        "promo_code":                   None,
        "promo_discount_in_cents":      0,
        # Metadata
        "notes":                        None,
        # Audit
        "created_by":                   created_by,
        "updated_by":                   created_by,
        "created_datetime":             now,
        "updated_datetime":             now,
        "version":                      1,
    }


# ── Duplicate check ────────────────────────────────────────────────────────────

def rental_agreement_exists(conn, customer_id: int, unit_id: int) -> bool:
    """
    Check if a rental_agreement record already exists for this customer-unit pair.
    Dedup key: customer_id + unit_id (composite).
    """
    from sqlalchemy import text as sa_text
    result = conn.execute(sa_text(
        "SELECT id FROM storentic.rental_agreements "
        "WHERE customer_id = :cid AND unit_id = :uid "
        "LIMIT 1"
    ), {"cid": customer_id, "uid": unit_id})
    return result.fetchone() is not None


# ── SQL statements ─────────────────────────────────────────────────────────────

_INSERT_SQL = """
    INSERT INTO storentic.rental_agreements (
        customer_id,
        unit_id,
        facility_id,
        move_in_date,
        lease_date,
        paid_through_date,
        schedule_date,
        move_out_date,
        rental_rate_in_cents,
        schedule_rate_in_cents,
        rate_variance_in_cents,
        security_deposit_in_cents,
        insurance_option,
        insurance_rate_in_cents,
        charge_day,
        status,
        promo_code,
        promo_discount_in_cents,
        notes,
        created_by,
        updated_by,
        created_datetime,
        updated_datetime,
        version
    ) VALUES (
        :customer_id,
        :unit_id,
        :facility_id,
        :move_in_date,
        :lease_date,
        :paid_through_date,
        :schedule_date,
        :move_out_date,
        :rental_rate_in_cents,
        :schedule_rate_in_cents,
        :rate_variance_in_cents,
        :security_deposit_in_cents,
        :insurance_option,
        :insurance_rate_in_cents,
        :charge_day,
        :status,
        :promo_code,
        :promo_discount_in_cents,
        :notes,
        :created_by,
        :updated_by,
        :created_datetime,
        :updated_datetime,
        :version
    )
"""

_UPDATE_SQL = """
    UPDATE storentic.rental_agreements SET
        move_in_date                = :move_in_date,
        lease_date                  = :lease_date,
        paid_through_date           = :paid_through_date,
        schedule_date               = :schedule_date,
        move_out_date               = :move_out_date,
        rental_rate_in_cents        = :rental_rate_in_cents,
        schedule_rate_in_cents      = :schedule_rate_in_cents,
        security_deposit_in_cents   = :security_deposit_in_cents,
        insurance_option            = :insurance_option,
        insurance_rate_in_cents     = :insurance_rate_in_cents,
        charge_day                  = :charge_day,
        updated_by                  = :updated_by,
        updated_datetime            = :updated_datetime
    WHERE customer_id = :customer_id AND unit_id = :unit_id
"""


# ── Excel output ───────────────────────────────────────────────────────────────

EXCEL_OUTPUT_COLUMNS = [
    "customer_id",
    "unit_id",
    "facility_id",
    "move_in_date",
    "lease_date",
    "paid_through_date",
    "schedule_date",
    "move_out_date",
    "rental_rate_in_cents",
    "schedule_rate_in_cents",
    "rate_variance_in_cents",
    "security_deposit_in_cents",
    "insurance_option",
    "insurance_rate_in_cents",
    "charge_day",
    "status",
    "promo_code",
    "promo_discount_in_cents",
    "notes",
    "created_by",
    "updated_by",
]


def write_to_excel(records: list[dict], out_path: str):
    """Write transformed records to an Excel file for QA review before DB insertion."""
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Transformed RentalAgreements")

        ws = writer.sheets["Transformed RentalAgreements"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)

    logger.info(f"📊  Excel output written: {out_path}  ({len(records)} rows)")


# ── Argument parsing (shared with migrate.py) ──────────────────────────────────

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Storentic Rental Agreements ETL Migration")
    parser.add_argument("--file",     required=True,  help="Path to source CSV file")
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
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"rental_agreements_transformed_{ts}.xlsx")

    logger.info("=" * 60)
    logger.info("  STORENTIC RENTAL AGREEMENTS ETL MIGRATION STARTING")
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
    df, col_map = validate(args.file, mode="rental_agreement")

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

        record = transform_row(row_idx, row, col_map, customer_map, unit_map, loc_id, created_by)

        if record is None:
            stats["errors"] += 1
            continue

        customer_id = record["customer_id"]
        unit_id     = record["unit_id"]
        display_key = f"customer_id={customer_id}, unit_id={unit_id}"

        # ── Excel output mode ─────────────────────────────────────────────────
        if output_mode == "excel":
            excel_records.append(record)
            logger.info(f"📋  Queued for Excel: {display_key}")
            stats["inserted"] += 1
            continue

        # ── DB dry run ────────────────────────────────────────────────────────
        if dry_run:
            logger.info(f"[DRY RUN] Would upsert rental_agreement: {display_key}")
            stats["inserted"] += 1
            continue

        # ── Live DB write ─────────────────────────────────────────────────────
        try:
            from sqlalchemy import text as sa_text
            with engine.begin() as conn:
                exists = rental_agreement_exists(conn, customer_id, unit_id)

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
