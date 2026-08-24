"""
migrate_historical_rental_agreements.py — ETL: historical rental_agreements from SiteLink.

Source : Tenants+Units+Ledgers+Access CSV file (SiteLink export)
Filter : Only rows where dMovedOut IS NOT NULL (completed tenancies)
Target : storentic.rental_agreements  (status = TERMINATED)

Usage :
    python migrate_historical_rental_agreements.py --file "data/Tenants_Units_Ledgers_Access_20260502.csv" --output db
    python migrate_historical_rental_agreements.py --file "data/Tenants_Units_Ledgers_Access_20260502.csv" --output db --dry-run
    python migrate_historical_rental_agreements.py --file "data/Tenants_Units_Ledgers_Access_20260502.csv" --output excel

Arguments:
    --file      Path to Tenants+Units+Ledgers+Access CSV file (required)
    --output    Destination: 'db' (default) or 'excel'
    --out-file  [excel mode only] Output Excel filename
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    LOCATION_ID     — scopes unit_map and customer_map lookups; written as facility_id
    CREATED_BY      — user id recorded in created_by / updated_by
    DRY_RUN         — alternative to --dry-run flag
    UPDATE_EXISTING — alternative to --update flag

Dedup key  : customer_id + unit_id + move_in_date
             (a tenant may have rented the same unit more than once at different times)
Dependency : storentic.customer (active + former) and storentic.units must be loaded first.

Field mapping:
    customer_id               ← TenantID        (lookup → storentic.customer.external_id)
    unit_id                   ← sUnitName        (lookup → storentic.units.unit_number)
    facility_id               ← LOCATION_ID      (env var)
    move_in_date              ← dMovedIn         (required — drives charge_day)
    move_out_date             ← dMovedOut        (always populated — filter guarantees this)
    lease_date                ← dLease           (fallback: dMovedIn)
    paid_through_date         ← dPaidThru        (required)
    schedule_date             ← dSchedOut        (fallback: dMovedIn)
    rental_rate_in_cents      ← dcRent           (dollars × 100)
    schedule_rate_in_cents    ← dcSchedRent      (copy rental_rate if 0 or null)
    rate_variance_in_cents    ← hardcoded 0
    security_deposit_in_cents ← dcSecDepPaid     (dollars × 100)
    insurance_rate_in_cents   ← dcInsurPremium   (dollars × 100)
    insurance_option          ← derived           (BASIC if dcInsurPremium > 0, else NONE)
    charge_day                ← dMovedIn          (day-of-month, clamped to 30)
    status                    ← hardcoded TERMINATED
    promo_code                ← hardcoded NULL
    promo_discount_in_cents   ← hardcoded 0
    notes                     ← hardcoded NULL
"""

import argparse
import os
import sys

import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger
from scripts import rental_agreement_transformer as T
from scripts import to_bigint

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def _clean_str(value) -> str | None:
    """Strip and return None for blank/nan values."""
    if value is None:
        return None
    s = str(value).strip()
    return None if s.lower() in ("nan", "none", "") else s


# ── Required source columns ────────────────────────────────────────────────────
REQUIRED_COLS = {
    "TENANTID", "SUNITNAME", "DMOVEDIN", "DMOVEDOUT",
    "DLEASE", "DPAIDTHRU", "DSCHEDOUT",
    "DCRENT", "DCSCHEDRENT", "DCSECDEPPAID", "DCINSURPREMIUM",
}


# ── DB lookup maps ─────────────────────────────────────────────────────────────

def load_customer_map(conn, location_id: int) -> dict:
    from sqlalchemy import text as sa_text
    rows = conn.execute(sa_text(
        "SELECT id, external_id FROM storentic.customer "
        "WHERE external_id IS NOT NULL AND location_id = :loc"
    ), {"loc": location_id}).fetchall()
    result = {str(row.external_id).strip(): row.id for row in rows}
    logger.info(f"📋  customer_map loaded: {len(result)} entries")
    return result


def load_unit_map(conn, location_id: int) -> dict:
    from sqlalchemy import text as sa_text
    rows = conn.execute(sa_text(
        "SELECT id, unit_number FROM storentic.units WHERE location_id = :loc"
    ), {"loc": location_id}).fetchall()
    result = {str(row.unit_number).strip(): row.id for row in rows}
    logger.info(f"📋  unit_map loaded: {len(result)} entries")
    return result


def load_existing_triples(engine) -> tuple[set, set]:
    """
    Returns:
      existing_quads  — set of (customer_id, unit_id, move_in_date, move_out_date)
                        for all TERMINATED rows (dedup key)
      active_triples  — set of (customer_id, unit_id, move_in_date)
                        for all ACTIVE rows (used to skip conflicting historical inserts)
    """
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        hist_rows = conn.execute(sa_text(
            "SELECT customer_id, unit_id, move_in_date, move_out_date "
            "FROM storentic.rental_agreements WHERE move_out_date IS NOT NULL"
        )).fetchall()
        active_rows = conn.execute(sa_text(
            "SELECT customer_id, unit_id, move_in_date "
            "FROM storentic.rental_agreements WHERE move_out_date IS NULL"
        )).fetchall()
    existing_quads  = {(r.customer_id, r.unit_id, r.move_in_date, r.move_out_date) for r in hist_rows}
    active_triples  = {(r.customer_id, r.unit_id, r.move_in_date) for r in active_rows}
    logger.info(f"📋  existing historical records : {len(existing_quads):,}")
    logger.info(f"📋  existing active records     : {len(active_triples):,}")
    return existing_quads, active_triples


# ── SQL ────────────────────────────────────────────────────────────────────────

_INSERT_COLS = [
    "customer_id", "unit_id", "facility_id",
    "move_in_date", "move_out_date", "lease_date",
    "paid_through_date", "schedule_date",
    "rental_rate_in_cents", "schedule_rate_in_cents", "rate_variance_in_cents",
    "security_deposit_in_cents", "insurance_option", "insurance_rate_in_cents",
    "charge_day", "status", "promo_code", "promo_discount_in_cents",
    "notes", "external_employee_id", "external_system",
    "created_by", "updated_by", "created_datetime", "updated_datetime",
    "version",
]

_INSERT_SQL = """
    INSERT INTO storentic.rental_agreements (
        customer_id,
        unit_id,
        facility_id,
        move_in_date,
        move_out_date,
        lease_date,
        paid_through_date,
        schedule_date,
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
        external_employee_id,
        external_system,
        created_by,
        updated_by,
        created_datetime,
        updated_datetime,
        version
    ) VALUES %s
    ON CONFLICT DO NOTHING
"""

_UPDATE_SQL = """
    UPDATE storentic.rental_agreements SET
        move_out_date               = :move_out_date,
        paid_through_date           = :paid_through_date,
        schedule_date               = :schedule_date,
        rental_rate_in_cents        = :rental_rate_in_cents,
        schedule_rate_in_cents      = :schedule_rate_in_cents,
        security_deposit_in_cents   = :security_deposit_in_cents,
        insurance_option            = :insurance_option,
        insurance_rate_in_cents     = :insurance_rate_in_cents,
        updated_by                  = :updated_by,
        updated_datetime            = :updated_datetime
    WHERE customer_id = :customer_id AND unit_id = :unit_id AND move_in_date = :move_in_date
"""

EXCEL_OUTPUT_COLUMNS = [
    "customer_id", "unit_id", "facility_id",
    "move_in_date", "move_out_date", "lease_date",
    "paid_through_date", "schedule_date",
    "rental_rate_in_cents", "schedule_rate_in_cents", "rate_variance_in_cents",
    "security_deposit_in_cents", "insurance_option", "insurance_rate_in_cents",
    "charge_day", "status", "promo_code", "promo_discount_in_cents",
    "notes", "created_by", "updated_by",
]


# ── Row transformer ────────────────────────────────────────────────────────────

def transform_row(excel_row: int, row: pd.Series,
                  customer_map: dict, unit_map: dict,
                  facility_id: int, created_by: int) -> dict | None:

    # ── customer_id ───────────────────────────────────────────────────────────
    tenant_id   = str(row.get("TENANTID") or "").strip().split(".")[0]
    customer_id = customer_map.get(tenant_id)
    if not customer_id:
        log_skipped(excel_row, tenant_id, "TENANTID",
                    f"customer not found for TenantID={tenant_id!r}", tenant_id)
        return None

    # ── unit_id ───────────────────────────────────────────────────────────────
    unit_name = str(row.get("SUNITNAME") or "").strip()
    unit_id   = unit_map.get(unit_name)
    if not unit_id:
        log_skipped(excel_row, unit_name, "SUNITNAME",
                    f"unit not found for sUnitName={unit_name!r}", unit_name)
        return None

    # ── dMovedIn (required) ───────────────────────────────────────────────────
    move_in_date = T.parse_date(row.get("DMOVEDIN"))
    if move_in_date is None:
        log_skipped(excel_row, unit_name, "DMOVEDIN",
                    "dMovedIn is null — move_in_date and charge_day are required",
                    row.get("DMOVEDIN"))
        return None

    # ── dMovedOut (required — filter guarantees this) ─────────────────────────
    move_out_date = T.parse_date(row.get("DMOVEDOUT"))
    if move_out_date is None:
        log_skipped(excel_row, unit_name, "DMOVEDOUT",
                    "dMovedOut is null — row should have been filtered out",
                    row.get("DMOVEDOUT"))
        return None

    # ── dPaidThru (required) ─────────────────────────────────────────────────
    paid_through_date = T.parse_date(row.get("DPAIDTHRU"))
    if paid_through_date is None:
        log_skipped(excel_row, unit_name, "DPAIDTHRU",
                    "dPaidThru is null — paid_through_date is required",
                    row.get("DPAIDTHRU"))
        return None

    # ── dcRent (required) ─────────────────────────────────────────────────────
    rent_raw = row.get("DCRENT")
    if rent_raw is None or str(rent_raw).strip().lower() in ("nan", "none", ""):
        log_skipped(excel_row, unit_name, "DCRENT",
                    "dcRent is null — rental_rate_in_cents is required", rent_raw)
        return None
    rental_rate_in_cents = T.to_cents(rent_raw)

    # ── optional fields ───────────────────────────────────────────────────────
    lease_date    = T.parse_date(row.get("DLEASE")) or move_in_date
    schedule_date = T.parse_date(row.get("DSCHEDOUT")) or move_in_date
    # Clamp schedule_date — must not be before move_in_date (DB constraint valid_schedule_date)
    if schedule_date < move_in_date:
        schedule_date = move_in_date

    schedule_rate_in_cents = T.to_cents(row.get("DCSCHEDRENT"))
    if schedule_rate_in_cents == 0:
        schedule_rate_in_cents = rental_rate_in_cents

    insurance_rate_in_cents = T.to_cents(row.get("DCINSURPREMIUM"))
    insurance_option        = T.derive_insurance_option(insurance_rate_in_cents)

    now = datetime.now(timezone.utc)

    return {
        "customer_id":                customer_id,
        "unit_id":                    unit_id,
        "facility_id":                facility_id,
        "move_in_date":               move_in_date,
        "move_out_date":              move_out_date,
        "lease_date":                 lease_date,
        "paid_through_date":          paid_through_date,
        "schedule_date":              schedule_date,
        "rental_rate_in_cents":       rental_rate_in_cents,
        "schedule_rate_in_cents":     schedule_rate_in_cents,
        "rate_variance_in_cents":     0,
        "security_deposit_in_cents":  T.to_cents(row.get("DCSECDEPPAID")),
        "insurance_option":           insurance_option,
        "insurance_rate_in_cents":    insurance_rate_in_cents,
        "charge_day":                 T.derive_charge_day(move_in_date),
        "status":                     "TERMINATED",
        "promo_code":                 None,
        "promo_discount_in_cents":    0,
        "notes":                      None,
        "external_employee_id":       to_bigint(row.get("EMPLOYEEID")),
        "external_system":            "sitelink",
        "created_by":                 created_by,
        "updated_by":                 created_by,
        "created_datetime":           now,
        "updated_datetime":           now,
        "version":                    1,
    }


# ── Excel writer ───────────────────────────────────────────────────────────────

def write_to_excel(records: list[dict], out_path: str):
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Historical RentalAgreements")
        ws = writer.sheets["Historical RentalAgreements"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(c.value)) if c.value is not None else 0 for c in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 40)
    logger.info(f"📊  Excel output written: {out_path}  ({len(records):,} rows)")


# ── Summary ────────────────────────────────────────────────────────────────────

def _print_summary(stats: dict):
    from scripts.logger import LOG_FILE, ERROR_CSV, RUN_TS
    mode  = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 60,
        "  STORENTIC HISTORICAL RENTAL AGREEMENTS MIGRATION — SUMMARY",
        f"  Run timestamp : {RUN_TS}",
        f"  Output mode   : {mode}",
        f"  Dry run       : {stats.get('dry_run', False)}",
        "=" * 60,
        f"  Source rows (dMovedOut not null) : {stats.get('source_rows', 0):,}",
        f"  Total processed                  : {stats.get('total', 0):,}",
        f"  Successfully inserted            : {stats.get('inserted', 0):,}",
        f"  Updated (--update)               : {stats.get('updated', 0):,}",
        f"  Skipped (duplicates)             : {stats.get('skipped', 0):,}",
        f"  Errors / rejected                : {stats.get('errors', 0):,}",
        "=" * 60,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output : {stats['excel_output']}")
    lines += [f"  Log file     : {LOG_FILE}", f"  Error CSV    : {ERROR_CSV}", "=" * 60]
    text = "\n".join(lines)
    print("\n" + text)
    logger.info(text)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Historical Rental Agreements ETL — SiteLink Tenants file → storentic.rental_agreements"
    )
    parser.add_argument("--file",     required=True,  help="Path to Tenants+Units+Ledgers Excel file")
    parser.add_argument("--output",   default="db",   choices=["db", "excel"])
    parser.add_argument("--out-file", default=None,   help="[excel mode] Output file path")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--update",   action="store_true",
                        help="Update existing records instead of skipping")
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    output_mode     = args.output
    dry_run         = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    update_existing = args.update  or os.getenv("UPDATE_EXISTING", "false").lower() == "true"
    loc_id          = int(os.getenv("LOCATION_ID", 1))
    created_by      = int(os.getenv("CREATED_BY", 0))

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(
        OUTPUT_DIR, f"historical_rental_agreements_transformed_{ts}.xlsx"
    )

    logger.info("=" * 60)
    logger.info("  HISTORICAL RENTAL AGREEMENTS ETL MIGRATION STARTING")
    logger.info(f"  File        : {args.file}")
    logger.info(f"  Output mode : {output_mode.upper()}")
    logger.info(f"  Dry run     : {dry_run}")
    logger.info(f"  Update mode : {update_existing}")
    logger.info(f"  Location    : {loc_id}  |  Created by: {created_by}")
    logger.info("=" * 60)

    # ── Load and filter source file ───────────────────────────────────────────
    logger.info(f"📂  Loading source file: {args.file}")
    df_raw = pd.read_csv(args.file, dtype=str, encoding='latin-1')
    df_raw = df_raw.drop(columns=["Totals & Averages"], errors="ignore")
    df_raw.columns = df_raw.columns.str.strip().str.upper()

    missing = REQUIRED_COLS - set(df_raw.columns)
    if missing:
        print(f"\n❌  Missing required columns: {missing}\n", flush=True)
        sys.exit(1)

    df = df_raw[df_raw["DMOVEDOUT"].notna()].copy()
    df = df[df["DMOVEDOUT"].astype(str).str.strip().str.lower() != "nat"].copy()

    print(
        f"\n  Source rows total                : {len(df_raw):,}"
        f"\n  Rows with dMovedOut (historical) : {len(df):,}\n",
        flush=True,
    )
    logger.info(f"    Source rows: {len(df_raw):,} | Historical: {len(df):,}")

    # ── DB connection and lookup maps ─────────────────────────────────────────
    engine       = None
    customer_map = {}
    unit_map     = {}

    if output_mode == "db" and not dry_run:
        try:
            from scripts.db import get_engine
            engine = get_engine()
            logger.info("✅  Database connection established.")
            with engine.connect() as conn:
                customer_map = load_customer_map(conn, loc_id)
                unit_map     = load_unit_map(conn, loc_id)
        except ImportError:
            logger.error("❌  sqlalchemy / psycopg2 not installed.")
            sys.exit(1)
    else:
        logger.info("ℹ️   Dry-run / Excel mode: skipping DB connection.")

    # Pre-load existing records for O(1) dedup
    existing_quads: set[tuple] = set()
    active_triples: set[tuple] = set()
    if output_mode == "db" and not dry_run:
        existing_quads, active_triples = load_existing_triples(engine)

    batch_size = int(os.getenv("BATCH_SIZE", 500))

    # ── Process rows ──────────────────────────────────────────────────────────
    stats = {
        "source_rows": len(df),
        "total": 0, "inserted": 0, "updated": 0,
        "skipped": 0, "errors": 0,
        "dry_run": dry_run, "output_mode": output_mode,
    }
    excel_records  = []
    new_records    = []
    update_records = []

    for row_idx, row in enumerate(df.itertuples(index=False)):
        stats["total"] += 1
        excel_row = row_idx + 2

        record = transform_row(excel_row, pd.Series(row._asdict()), customer_map, unit_map, loc_id, created_by)
        if record is None:
            stats["errors"] += 1
            continue

        customer_id   = record["customer_id"]
        unit_id       = record["unit_id"]
        move_in_date  = record["move_in_date"]
        move_out_date = record["move_out_date"]
        quad          = (customer_id, unit_id, move_in_date, move_out_date)
        active_triple = (customer_id, unit_id, move_in_date)

        if output_mode == "excel":
            excel_records.append(record)
            stats["inserted"] += 1
            continue

        if dry_run:
            logger.info(f"[DRY RUN] Would upsert: customer_id={customer_id}, unit_id={unit_id}, move_in={move_in_date}")
            stats["inserted"] += 1
            continue

        # Skip if an ACTIVE record exists for same customer+unit+move_in_date
        if active_triple in active_triples:
            log_skipped(0, customer_id, "move_in_date",
                        f"Active rental exists for customer_id={customer_id}, unit_id={unit_id}, "
                        f"move_in_date={move_in_date} — historical insert skipped", None)
            stats["skipped"] += 1
            continue

        if quad in existing_quads:
            if update_existing:
                update_records.append(record)
            else:
                stats["skipped"] += 1
        else:
            new_records.append(record)
            existing_quads.add(quad)

    # ── Bulk INSERT new records ───────────────────────────────────────────────
    if new_records and not dry_run and output_mode == "db":
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                execute_values(
                    cur,
                    _INSERT_SQL,
                    [tuple(r[c] for c in _INSERT_COLS) for r in new_records],
                    page_size=batch_size,
                )
            raw_conn.commit()
        except Exception as exc:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
        stats["inserted"] = len(new_records)
        logger.info(f"✅  Bulk inserted {stats['inserted']:,} historical rental_agreement rows.")

    # ── Batch UPDATE existing records ─────────────────────────────────────────
    if update_records and not dry_run and output_mode == "db":
        from sqlalchemy import text as sa_text
        with engine.begin() as conn:
            for record in update_records:
                conn.execute(sa_text(_UPDATE_SQL), record)
        stats["updated"] = len(update_records)
        logger.info(f"🔄  Updated {stats['updated']:,} historical rental_agreement rows.")

    # ── Excel output ──────────────────────────────────────────────────────────
    if output_mode == "excel":
        if excel_records:
            write_to_excel(excel_records, out_file)
            stats["excel_output"] = out_file
        else:
            logger.warning("⚠️   No records to write.")

    _print_summary(stats)
    close_logger()


if __name__ == "__main__":
    main()
