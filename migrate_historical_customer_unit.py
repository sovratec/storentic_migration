"""
migrate_historical_customer_unit.py — ETL script for loading historical customer_unit records.

Source : Tenants+Units+Ledgers+Access CSV file (SiteLink export)
Filter : Only rows where dMovedOut IS NOT NULL (former tenants with a completed tenancy)
Target : storentic.customer_unit

Usage :
    python migrate_historical_customer_unit.py --file "data/Tenants_Units_Ledgers_Access_20260502.csv" --output db
    python migrate_historical_customer_unit.py --file "data/Tenants_Units_Ledgers_Access_20260502.csv" --output db --dry-run
    python migrate_historical_customer_unit.py --file "data/Tenants_Units_Ledgers_Access_20260502.csv" --output excel

Arguments:
    --file      Path to Tenants+Units+Ledgers+Access CSV file (required)
    --output    Destination: 'db' (default) or 'excel'
    --out-file  [excel mode only] Output Excel filename
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    LOCATION_ID     — scopes the unit_map and customer_map lookups
    CREATED_BY      — user id recorded in created_by / updated_by
    DRY_RUN         — alternative to --dry-run flag
    UPDATE_EXISTING — alternative to --update flag

Dedup key  : customer_id + unit_id + move_in_date
             (a tenant may have rented the same unit more than once at different times)
Dependency : storentic.customer (active + former) and storentic.units must be loaded first.

Field mapping:
    customer_id            ← TenantID      (lookup → storentic.customer.external_id)
    unit_id                ← sUnitName     (lookup → storentic.units.unit_number + LOCATION_ID)
    lease_date             ← dLease        (fallback: dMovedIn)
    move_out_date          ← dMovedOut     (always populated — filter guarantees this)
    paid_through_date      ← dPaidThru
    scheduled_date         ← dSchedOut
    rental_rate            ← dcRent        (DECIMAL dollars)
    rate_variance          ← hardcoded 0.00
    scheduled_rate         ← dcSchedRent   (copy rental_rate if 0 or null)
    charge_day             ← dMovedIn      (day-of-month, clamped to 28)
    access_code            ← sAccessCode   (truncated to 10 chars)
    rental_contract_signed ← hardcoded False
"""

import argparse
import os
import sys

import pandas as pd
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Required source columns ────────────────────────────────────────────────────
REQUIRED_COLS = {
    "TENANTID", "SUNITNAME", "DMOVEDIN", "DMOVEDOUT",
    "DLEASE", "DPAIDTHRU", "DSCHEDOUT", "DCRENT",
    "DCSCHEDRENT", "SACCESSCODE",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_date(val):
    if val is None:
        return None
    if isinstance(val, pd.Timestamp):
        return val.date()
    try:
        ts = pd.to_datetime(val)
        return None if pd.isna(ts) else ts.date()
    except Exception:
        return None


def _safe_decimal(val, default=Decimal("0.00")) -> Decimal:
    try:
        f = float(val)
        return Decimal(str(round(f, 2)))
    except (TypeError, ValueError, InvalidOperation):
        return default


def _charge_day(move_in_date) -> int:
    day = move_in_date.day
    return min(day, 28)


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


def load_existing_triples(engine) -> set[tuple]:
    """Load all (customer_id, unit_id, lease_date) triples for historical rows."""
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT customer_id, unit_id, lease_date FROM storentic.customer_unit "
            "WHERE move_out_date IS NOT NULL"
        )).fetchall()
    triples = {(r.customer_id, r.unit_id, r.lease_date) for r in rows}
    logger.info(f"📋  existing historical customer_unit triples: {len(triples):,}")
    return triples


# ── SQL ────────────────────────────────────────────────────────────────────────

_INSERT_COLS = [
    "customer_id", "unit_id", "lease_date", "move_out_date",
    "paid_through_date", "scheduled_date", "rental_rate", "rate_variance",
    "scheduled_rate", "charge_day", "access_code", "rental_contract_signed",
    "created_by", "updated_by", "created_datetime", "updated_datetime",
]

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
    ) VALUES %s
    ON CONFLICT DO NOTHING
"""

_UPDATE_SQL = """
    UPDATE storentic.customer_unit SET
        move_out_date       = :move_out_date,
        paid_through_date   = :paid_through_date,
        rental_rate         = :rental_rate,
        scheduled_rate      = :scheduled_rate,
        updated_by          = :updated_by,
        updated_datetime    = :updated_datetime
    WHERE customer_id = :customer_id AND unit_id = :unit_id AND lease_date = :lease_date
"""

EXCEL_OUTPUT_COLUMNS = [
    "customer_id", "unit_id", "lease_date", "move_out_date",
    "paid_through_date", "scheduled_date", "rental_rate", "rate_variance",
    "scheduled_rate", "charge_day", "access_code", "rental_contract_signed",
    "created_by", "updated_by",
]


# ── Row transformer ────────────────────────────────────────────────────────────

def transform_row(excel_row: int, row: pd.Series,
                  customer_map: dict, unit_map: dict,
                  created_by: int) -> dict | None:

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

    # ── dMovedIn (required — drives lease_date fallback and charge_day) ───────
    move_in_date = _parse_date(row.get("DMOVEDIN"))
    if move_in_date is None:
        log_skipped(excel_row, unit_name, "DMOVEDIN",
                    "dMovedIn is null — charge_day cannot be derived", row.get("DMOVEDIN"))
        return None

    # ── dMovedOut (required for historical rows — filter guarantees it) ───────
    move_out_date = _parse_date(row.get("DMOVEDOUT"))
    if move_out_date is None:
        log_skipped(excel_row, unit_name, "DMOVEDOUT",
                    "dMovedOut is null — row should have been filtered out", row.get("DMOVEDOUT"))
        return None

    # ── dcRent (required) ─────────────────────────────────────────────────────
    rent_raw = row.get("DCRENT")
    if rent_raw is None or str(rent_raw).strip().lower() in ("nan", "none", ""):
        log_skipped(excel_row, unit_name, "DCRENT",
                    "dcRent is null — rental_rate is required", rent_raw)
        return None
    rental_rate = _safe_decimal(rent_raw)

    # ── optional fields with safe defaults ───────────────────────────────────
    lease_date      = _parse_date(row.get("DLEASE")) or move_in_date
    paid_thru       = _parse_date(row.get("DPAIDTHRU"))
    sched_date      = _parse_date(row.get("DSCHEDOUT"))
    scheduled_rate  = _safe_decimal(row.get("DCSCHEDRENT"))
    if scheduled_rate == Decimal("0.00"):
        scheduled_rate = rental_rate

    raw_access = str(row.get("SACCESSCODE") or "").strip()
    access_code = raw_access[:10] if raw_access else None

    now = datetime.utcnow()

    return {
        "customer_id":           customer_id,
        "unit_id":               unit_id,
        "lease_date":            lease_date,
        "move_out_date":         move_out_date,
        "paid_through_date":     paid_thru,
        "scheduled_date":        sched_date,
        "rental_rate":           rental_rate,
        "rate_variance":         Decimal("0.00"),
        "scheduled_rate":        scheduled_rate,
        "charge_day":            _charge_day(move_in_date),
        "access_code":           access_code,
        "rental_contract_signed": False,
        "created_by":            created_by,
        "updated_by":            created_by,
        "created_datetime":      now,
        "updated_datetime":      now,
    }


# ── Excel writer ───────────────────────────────────────────────────────────────

def write_to_excel(records: list[dict], out_path: str):
    df_out = pd.DataFrame(records, columns=EXCEL_OUTPUT_COLUMNS)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Historical CustomerUnit")
        ws = writer.sheets["Historical CustomerUnit"]
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
        "  STORENTIC HISTORICAL CUSTOMER-UNIT MIGRATION — SUMMARY",
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
        description="Historical Customer-Unit ETL — SiteLink Tenants file → storentic.customer_unit"
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
        OUTPUT_DIR, f"historical_customer_unit_transformed_{ts}.xlsx"
    )

    logger.info("=" * 60)
    logger.info("  HISTORICAL CUSTOMER-UNIT ETL MIGRATION STARTING")
    logger.info(f"  File        : {args.file}")
    logger.info(f"  Output mode : {output_mode.upper()}")
    logger.info(f"  Dry run     : {dry_run}")
    logger.info(f"  Update mode : {update_existing}")
    logger.info(f"  Location    : {loc_id}  |  Created by: {created_by}")
    logger.info("=" * 60)

    # ── Load and filter source file ───────────────────────────────────────────
    logger.info(f"📂  Loading source file: {args.file}")
    df_raw = pd.read_csv(args.file, dtype=str, encoding='utf-8')
    df_raw = df_raw.drop(columns=["Totals & Averages"], errors="ignore")
    df_raw.columns = df_raw.columns.str.strip().str.upper()

    missing = REQUIRED_COLS - set(df_raw.columns)
    if missing:
        print(f"\n❌  Missing required columns: {missing}\n", flush=True)
        sys.exit(1)

    df = df_raw[df_raw["DMOVEDOUT"].notna()].copy()
    df = df[df["DMOVEDOUT"].astype(str).str.strip().str.lower() != "nat"].copy()

    print(
        f"\n  Source rows total            : {len(df_raw):,}"
        f"\n  Rows with dMovedOut (historical) : {len(df):,}\n",
        flush=True,
    )
    logger.info(f"    Source rows: {len(df_raw):,} | Historical (dMovedOut not null): {len(df):,}")

    # ── DB connection and lookup maps ─────────────────────────────────────────
    engine       = None
    customer_map = {}
    unit_map     = {}

    if output_mode == "db" and not dry_run:
        try:
            from scripts.db import get_engine
            from sqlalchemy import text as sa_text
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

    # Pre-load existing triples for O(1) dedup
    existing_triples: set[tuple] = set()
    if output_mode == "db" and not dry_run:
        existing_triples = load_existing_triples(engine)

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

        record = transform_row(excel_row, pd.Series(row._asdict()), customer_map, unit_map, created_by)
        if record is None:
            stats["errors"] += 1
            continue

        customer_id  = record["customer_id"]
        unit_id      = record["unit_id"]
        lease_date   = record["lease_date"]
        triple       = (customer_id, unit_id, lease_date)

        if output_mode == "excel":
            excel_records.append(record)
            stats["inserted"] += 1
            continue

        if dry_run:
            logger.info(f"[DRY RUN] Would upsert: customer_id={customer_id}, unit_id={unit_id}, lease={lease_date}")
            stats["inserted"] += 1
            continue

        if triple in existing_triples:
            if update_existing:
                update_records.append(record)
            else:
                stats["skipped"] += 1
        else:
            new_records.append(record)
            existing_triples.add(triple)

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
        logger.info(f"✅  Bulk inserted {stats['inserted']:,} historical customer_unit rows.")

    # ── Batch UPDATE existing records ─────────────────────────────────────────
    if update_records and not dry_run and output_mode == "db":
        from sqlalchemy import text as sa_text
        with engine.begin() as conn:
            for record in update_records:
                conn.execute(sa_text(_UPDATE_SQL), record)
        stats["updated"] = len(update_records)
        logger.info(f"🔄  Updated {stats['updated']:,} historical customer_unit rows.")

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
