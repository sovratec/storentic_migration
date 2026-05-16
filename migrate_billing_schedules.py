"""
migrate_billing_schedules.py — ETL: SiteLink Tenants → storentic.billing_schedules

Source
------
  Tenants_Units_Ledgers_Access.csv : all columns needed (BillingFreqID, dLease,
                                     dMovedOut, TenantID, sUnitName, iProcessDayOfMonth)
  RentRoll.csv                     : Unit → Charge Day (primary source for charge_day)

DB lookups
----------
  storentic.customer      : external_id = TenantID → customer_id
  storentic.units         : unit_number = sUnitName → unit_id
  storentic.customer_unit : customer_id + unit_id, move_out_date IS NULL → customer_unit_id

BillingFreqID mapping
---------------------
  1 → DAILY          (not in entity enum — skipped)
  2 → WEEKLY         (not in entity enum — skipped)
  3 → MONTHLY
  4 → QUARTERLY
  5 → SEMI_ANNUALLY  (not in entity enum — skipped)
  6 → ANNUAL
  7 → 28_DAYS        (not in entity enum — skipped)

Usage
-----
    python migrate_billing_schedules.py \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260508.csv" \\
        --file-rentroll "data/RentRoll_Sample.csv" \\
        --output db --dry-run

    python migrate_billing_schedules.py \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260508.csv" \\
        --file-rentroll "data/RentRoll_Sample.csv" \\
        --output db

Environment (.env)
------------------
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — written to every row
    LOCATION_ID       — written to every row
    CREATED_BY        — user ID (default: 9)
"""

import argparse
import os
import sys
from calendar import monthrange
from datetime import date, datetime
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_skipped, close as close_logger

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── BillingFreqID → billing_frequency ─────────────────────────────────────────
_FREQ_MAP = {
    '1': 'DAILY',
    '2': 'WEEKLY',
    '3': 'MONTHLY',
    '4': 'QUARTERLY',
    '5': 'SEMI_ANNUALLY',
    '6': 'ANNUAL',
    '7': 'TWENTY_EIGHT_DAYS',
}
_SUPPORTED_FREQS = {'MONTHLY', 'QUARTERLY', 'ANNUAL'}

# ── SQL ────────────────────────────────────────────────────────────────────────
_INSERT_SQL = """
    INSERT INTO storentic.billing_schedules (
        customer_unit_id, customer_id, unit_id,
        location_id, organization_id,
        billing_frequency, charge_day,
        start_date, next_billing_date,
        is_active, created_by, created_at, updated_at
    ) VALUES %s
    ON CONFLICT DO NOTHING
"""

_INSERT_COLS = [
    "customer_unit_id", "customer_id", "unit_id",
    "location_id", "organization_id",
    "billing_frequency", "charge_day",
    "start_date", "next_billing_date",
    "is_active", "created_by", "created_at", "updated_at",
]


# =============================================================================
# Helpers
# =============================================================================

def calc_next_billing_date(charge_day: int) -> date:
    """Next occurrence of charge_day — this month if still upcoming, else next month."""
    today = date.today()
    days_this = monthrange(today.year, today.month)[1]
    candidate = today.replace(day=min(charge_day, days_this))
    if candidate >= today:
        return candidate
    year, month = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)
    return date(year, month, min(charge_day, monthrange(year, month)[1]))


def parse_date(val) -> date | None:
    if not val or str(val).strip() in ('', 'nan', 'NaT', 'None', '1/1/1900', '1900-01-01'):
        return None
    s = str(val).strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y',
                '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# =============================================================================
# Source file loaders
# =============================================================================

def load_tenants(filepath: str) -> pd.DataFrame:
    """Load active tenant rows from the combined Tenants+Units+Ledgers file."""
    logger.info(f"Loading tenants file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, encoding='utf-8')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip()

    required = {"TenantID", "sUnitName", "LedgerID", "BillingFreqID", "dLease", "dMovedOut"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Tenants file missing columns: {missing}")

    total = len(df)
    # Active = dMovedOut blank/null AND ledger dDeleted2 blank/null
    active = df[df["dMovedOut"].isna() | (df["dMovedOut"].str.strip() == "")]
    if "dDeleted2" in df.columns:
        active = active[active["dDeleted2"].isna() | (active["dDeleted2"].str.strip() == "")]
    active = active.reset_index(drop=True)

    logger.info(f"    Total rows  : {total:,}")
    logger.info(f"    Active rows : {len(active):,}")
    return active


def load_rentroll_charge_days(filepath: str) -> dict:
    """Load RentRoll CSV → {unit_number (str): charge_day (int)}."""
    logger.info(f"Loading RentRoll file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, encoding='utf-8')
    df.columns = df.columns.str.strip()
    mapping = {}
    for _, row in df.iterrows():
        unit = str(row.get("Unit", "") or "").strip()
        day  = str(row.get("Charge Day", "") or "").strip()
        if unit and day:
            try:
                mapping[unit] = int(float(day))
            except (ValueError, TypeError):
                pass
    logger.info(f"    Unit→ChargeDay entries: {len(mapping):,}")
    return mapping


# =============================================================================
# DB loaders
# =============================================================================

def load_customer_map(engine, org_id: int) -> dict:
    """Return {TenantID (str): customer_id (int)}."""
    logger.info("Loading customer map from DB...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL AND organization_id = :org"
        ), {"org": org_id}).fetchall()
    mapping = {str(r.external_id).strip(): r.id for r in rows}
    logger.info(f"    Customers loaded: {len(mapping):,}")
    return mapping


def load_unit_map(engine, loc_id: int) -> dict:
    """Return {unit_number (str): unit_id (int)}."""
    logger.info("Loading unit map from DB...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, unit_number FROM storentic.units WHERE location_id = :loc"
        ), {"loc": loc_id}).fetchall()
    mapping = {str(r.unit_number).strip(): r.id for r in rows}
    logger.info(f"    Units loaded: {len(mapping):,}")
    return mapping


def load_active_customer_units(engine) -> dict:
    """Return {(customer_id, unit_id): customer_unit_id} for active rentals."""
    logger.info("Loading active customer_units from DB...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, customer_id, unit_id FROM storentic.customer_unit "
            "WHERE move_out_date IS NULL"
        )).fetchall()
    mapping = {(r.customer_id, r.unit_id): r.id for r in rows}
    logger.info(f"    Active customer_units loaded: {len(mapping):,}")
    return mapping


# =============================================================================
# Main
# =============================================================================

def run(args):
    loc_id     = int(os.getenv("LOCATION_ID", "4"))
    org_id     = int(os.getenv("ORGANIZATION_ID", "1"))
    created_by = int(os.getenv("CREATED_BY", "9"))
    dry_run    = args.dry_run or os.getenv("DRY_RUN", "").lower() == "true"
    now        = datetime.utcnow()

    logger.info("=" * 60)
    logger.info("  BILLING SCHEDULES MIGRATION STARTING")
    logger.info(f"  Tenants  : {args.file_tenants}")
    logger.info(f"  RentRoll : {args.file_rentroll}")
    logger.info(f"  Dry run  : {dry_run}")
    logger.info(f"  Location : {loc_id}  Org: {org_id}")
    logger.info("=" * 60)

    # ── Load sources ──────────────────────────────────────────────────────────
    tenants = load_tenants(args.file_tenants)
    rr_map  = load_rentroll_charge_days(args.file_rentroll)

    # ── DB lookups ────────────────────────────────────────────────────────────
    db_url = (
        f"postgresql+psycopg2://{quote_plus(os.getenv('DB_USER', ''))}:{quote_plus(os.getenv('DB_PASSWORD', ''))}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME')}"
    )
    engine = create_engine(db_url)

    customer_map  = load_customer_map(engine, org_id)
    unit_map      = load_unit_map(engine, loc_id)
    active_cu_map = load_active_customer_units(engine)

    # ── Build insert tuples ───────────────────────────────────────────────────
    stats  = {"total": 0, "inserted": 0, "skipped": 0}
    tuples = []

    for _, row in tenants.iterrows():
        stats["total"] += 1
        tenant_id = str(row.get("TenantID", "") or "").strip()
        unit_name = str(row.get("sUnitName", "") or "").strip()
        freq_id   = str(row.get("BillingFreqID", "") or "").strip()
        lease_raw = row.get("dLease")

        # ── BillingFrequency ──────────────────────────────────────────────────
        freq = _FREQ_MAP.get(freq_id)
        if not freq:
            log_skipped(0, unit_name, "BillingFreqID", f"Unknown BillingFreqID={freq_id!r}", freq_id)
            stats["skipped"] += 1
            continue
        if freq not in _SUPPORTED_FREQS:
            log_skipped(0, unit_name, "BillingFreqID", f"Unsupported frequency {freq} (FreqID={freq_id})", freq_id)
            stats["skipped"] += 1
            continue

        # ── customer_id ───────────────────────────────────────────────────────
        customer_id = customer_map.get(tenant_id)
        if not customer_id:
            log_skipped(0, unit_name, "TenantID", f"No customer for TenantID={tenant_id}", tenant_id)
            stats["skipped"] += 1
            continue

        # ── unit_id ───────────────────────────────────────────────────────────
        unit_id = unit_map.get(unit_name)
        if not unit_id:
            log_skipped(0, unit_name, "sUnitName", f"No unit for unit_number={unit_name!r}", unit_name)
            stats["skipped"] += 1
            continue

        # ── customer_unit_id ──────────────────────────────────────────────────
        cu_id = active_cu_map.get((customer_id, unit_id))
        if not cu_id:
            log_skipped(0, unit_name, "customer_unit",
                        f"No active customer_unit for customer={customer_id} unit={unit_id}", unit_name)
            stats["skipped"] += 1
            continue

        # ── charge_day: RentRoll → iProcessDayOfMonth → day of dLease ────────
        charge_day = rr_map.get(unit_name)
        if not charge_day:
            proc_day = str(row.get("iProcessDayOfMonth", "") or "").strip()
            if proc_day and proc_day not in ('0', ''):
                try:
                    charge_day = int(float(proc_day))
                except (ValueError, TypeError):
                    pass
        if not charge_day:
            lease_date = parse_date(lease_raw)
            charge_day = lease_date.day if lease_date else 1
        charge_day = min(charge_day, 28)  # DB chk_charge_day constraint: max 28

        # ── start_date = dLease ───────────────────────────────────────────────
        start_date = parse_date(lease_raw)
        if not start_date:
            log_skipped(0, unit_name, "dLease", f"Cannot parse dLease={lease_raw!r}", lease_raw)
            stats["skipped"] += 1
            continue

        nbd = calc_next_billing_date(charge_day)

        tuples.append((
            cu_id, customer_id, unit_id,
            loc_id, org_id,
            freq, charge_day,
            start_date, nbd,
            True, created_by, now, now,
        ))

    logger.info(f"    Rows to insert : {len(tuples):,}")
    logger.info(f"    Skipped        : {stats['skipped']:,}")

    if dry_run:
        logger.info("DRY RUN — no DB writes.")
        for t in tuples[:5]:
            logger.info(str(dict(zip(_INSERT_COLS, t))))
        return

    # ── Bulk insert ───────────────────────────────────────────────────────────
    raw_conn   = engine.raw_connection()
    raw_cursor = raw_conn.cursor()
    try:
        execute_values(raw_cursor, _INSERT_SQL, tuples, page_size=500)
        raw_conn.commit()
        stats["inserted"] = len(tuples)
        logger.info(f"✅  Inserted {stats['inserted']:,} billing_schedule rows.")
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"❌  Insert failed: {e}")
        raise
    finally:
        raw_cursor.close()
        raw_conn.close()

    logger.info("=" * 60)
    logger.info(f"  Total     : {stats['total']:,}")
    logger.info(f"  Inserted  : {stats['inserted']:,}")
    logger.info(f"  Skipped   : {stats['skipped']:,}")
    logger.info("=" * 60)


def parse_args():
    p = argparse.ArgumentParser(description="Migrate billing schedules from SiteLink Tenants file")
    p.add_argument("--file-tenants",  required=True,
                   help="Path to Tenants_Units_Ledgers_Access.csv")
    p.add_argument("--file-rentroll", required=True,
                   help="Path to RentRoll.csv (for Charge Day)")
    p.add_argument("--output",  default="db", choices=["db"])
    p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        run(args)
    finally:
        close_logger()
