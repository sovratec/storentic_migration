"""
migrate_ledger_charges.py — ETL script: SiteLink Select Charges → storentic.ledger_charges

Usage :
    # Dry run (preview row counts, no DB writes)
    python migrate_ledger_charges.py \\
        --file-charges      "data/Charges.csv" \\
        --file-tenants      "data/Tenants_Units_Ledgers_Access_20260502.csv" \\
        --file-charge-types data/ChargeType.csv \\
        --output db --dry-run

    # Export to Excel for review before DB import
    python migrate_ledger_charges.py \\
        --file-charges      "data/Charges.csv" \\
        --file-tenants      "data/Tenants_Units_Ledgers_Access_20260502.csv" \\
        --file-charge-types data/ChargeType.csv \\
        --output excel

    # Production import
    python migrate_ledger_charges.py \\
        --file-charges      "data/Charges.csv" \\
        --file-tenants      "data/Tenants_Units_Ledgers_Access_20260502.csv" \\
        --file-charge-types data/ChargeType.csv \\
        --output db

Arguments:
    --file-charges      Path to SiteLink Select Charges CSV file (required)
    --file-tenants      Path to Tenants+Units+Ledgers+Access CSV file (required)
    --file-charge-types Path to ChargeType.csv mapping file (required)
    --output            'db' (default) or 'excel'
    --out-file          [excel mode] Output Excel file path
    --dry-run           [db mode] Preview without writing to DB

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — written to every ledger_charge row
    LOCATION_ID       — scopes unit/customer lookups and written to every row
    CREATED_BY        — user ID written to created_by column
    DRY_RUN           — alternative to --dry-run flag
    BATCH_SIZE        — rows per DB commit batch (default: 500)

Pre-requisite:
    Run sql/V7__ledger_charges_external_charge_id.sql against the target DB
    before running this script.

Join chain (how SiteLink charges map to Storentic records):
    Select Charges     LedgerID
           |
           v  (via Tenants file)
       TenantID  ──────────────────► storentic.customer (external_id) → customer_id
       sUnitName ──────────────────► storentic.units (unit_number)    → unit_id
           |
    ChargeDescID ──────────────────► ChargeType.csv                   → charge_type_id

Status logic:
    dDeleted is null  →  POSTED
    (rows with dDeleted not null are skipped entirely — not imported)

Skipped rows (written to output/ledger_charges_skipped_<ts>.xlsx):
    - dDeleted not null (voided/deleted in SiteLink, incl. abandoned same-day units)
    - LedgerID not found in Tenants file / no matching customer
    - ChargeDescID not in ChargeType.csv mapping
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text as sa_text
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger
from scripts import to_bigint

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── SQL ────────────────────────────────────────────────────────────────────────

# Used by execute_values — %s is the multi-row placeholder
_INSERT_SQL = """
    INSERT INTO storentic.ledger_charges (
        customer_id, unit_id, location_id, organization_id,
        charge_type_id, amount_in_cents, effective_date,
        memo, internal_note, status, invoice_id,
        source_screen, reversed_by_id, reversed_at,
        reversal_reason, created_by, created_at, updated_at,
        external_charge_id, external_employee_id, external_system
    ) VALUES %s
    ON CONFLICT (external_charge_id) WHERE external_charge_id IS NOT NULL DO NOTHING
"""

# Column order must match _INSERT_SQL above
_INSERT_COLS = [
    "customer_id", "unit_id", "location_id", "organization_id",
    "charge_type_id", "amount_in_cents", "effective_date",
    "memo", "internal_note", "status", "invoice_id",
    "source_screen", "reversed_by_id", "reversed_at",
    "reversal_reason", "created_by", "created_at", "updated_at",
    "external_charge_id", "external_employee_id", "external_system",
]

EXCEL_OUTPUT_COLUMNS = [
    "external_charge_id", "customer_id", "unit_id", "location_id",
    "organization_id", "charge_type_id", "amount_in_cents",
    "effective_date", "memo", "internal_note", "status",
    "reversed_at", "reversal_reason", "source_screen",
    "created_by", "created_at", "updated_at",
]

SKIPPED_COLUMNS = [
    "CHARGEID", "CHARGEDESCID", "LEDGERID", "DCAMT",
    "DCHGSTRT", "DCREATED", "skip_reason",
]


# =============================================================================
# STEP 1 — Build lookup maps from source files
# =============================================================================

def load_charge_type_map(charge_types_file: str) -> dict:
    """
    Read ChargeType.csv and return {ChargeDescID (int): charge_type_id (int)}.
    Rows with no id or no ChargeDescID are ignored (summary/total rows).
    """
    logger.info(f"📂  Loading charge type mapping: {charge_types_file}")
    df = pd.read_csv(charge_types_file, dtype=str, encoding='latin-1')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()

    mapping = {}
    for _, row in df.iterrows():
        if pd.isna(row.get("ID")) or pd.isna(row.get("CHARGEDESCID")):
            continue
        try:
            desc_id = int(float(row["CHARGEDESCID"]))
            ct_id   = int(float(row["ID"]))
            mapping[desc_id] = ct_id
        except (ValueError, TypeError):
            continue

    logger.info(f"    ChargeDescID mappings loaded: {len(mapping)}")
    return mapping



def load_tenants_maps(tenants_file: str) -> tuple[dict, dict]:
    """
    Read Tenants file and return two dicts:
        ledger_to_tenant   : {LedgerID (int): TenantID (int)}
        ledger_to_unitname : {LedgerID (int): sUnitName (str)}
    """
    logger.info(f"📂  Loading tenants file: {tenants_file}")
    df = pd.read_csv(tenants_file, dtype=str, encoding='latin-1')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()

    required = {"LEDGERID", "TENANTID", "SUNITNAME"}
    missing  = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Tenants file missing columns: {missing}")

    ledger_to_tenant   = {}
    ledger_to_unitname = {}
    skipped = 0

    for _, row in df.iterrows():
        try:
            ledger_id = int(float(row["LEDGERID"]))
            tenant_id = int(float(row["TENANTID"]))
            unit_name = str(row["SUNITNAME"]).strip() if pd.notna(row["SUNITNAME"]) else None
            ledger_to_tenant[ledger_id]   = tenant_id
            ledger_to_unitname[ledger_id] = unit_name
        except (ValueError, TypeError):
            skipped += 1

    logger.info(
        f"    Rows read        : {len(df)}\n"
        f"    LedgerID entries : {len(ledger_to_tenant)}\n"
        f"    Skipped bad rows : {skipped}"
    )
    return ledger_to_tenant, ledger_to_unitname


# =============================================================================
# STEP 2 — DB lookup maps
# =============================================================================

def check_external_charge_id_column(engine):
    """Verify V7 SQL migration has been fully applied (column + unique index)."""
    with engine.connect() as conn:
        col_exists = conn.execute(sa_text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'storentic' "
            "  AND table_name   = 'ledger_charges' "
            "  AND column_name  = 'external_charge_id'"
        )).fetchone()

        idx_exists = conn.execute(sa_text(
            "SELECT 1 FROM pg_indexes "
            "WHERE schemaname = 'storentic' "
            "  AND tablename  = 'ledger_charges' "
            "  AND indexname  = 'idx_ledger_charges_external_charge_id'"
        )).fetchone()

    remediation = (
        "\n    Run in DBeaver / psql:\n\n"
        "      ALTER TABLE storentic.ledger_charges\n"
        "          ADD COLUMN IF NOT EXISTS external_charge_id VARCHAR(100);\n\n"
        "      CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_charges_external_charge_id\n"
        "          ON storentic.ledger_charges(external_charge_id)\n"
        "          WHERE external_charge_id IS NOT NULL;\n"
    )

    if not col_exists:
        raise SystemExit(
            "\n❌  Column 'external_charge_id' missing on storentic.ledger_charges."
            + remediation
        )

    if not idx_exists:
        raise SystemExit(
            "\n❌  Unique index 'idx_ledger_charges_external_charge_id' is missing.\n"
            "    ON CONFLICT requires this index to exist." + remediation
        )


def load_customer_map(engine, org_id: int) -> dict:
    """{TenantID (int): customer.id (int)} for all migrated customers."""
    logger.info(f"🔍  Loading customer map for org_id={org_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL AND organization_id = :org_id"
        ), {"org_id": org_id}).fetchall()

    mapping = {}
    for row in rows:
        try:
            mapping[int(float(row.external_id))] = row.id
        except (ValueError, TypeError):
            pass

    logger.info(f"    Customers loaded: {len(mapping)}")
    return mapping


def load_unit_map(engine, loc_id: int) -> dict:
    """{unit_number (str): units.id (int)} for the given location."""
    logger.info(f"🔍  Loading unit map for location_id={loc_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, unit_number FROM storentic.units WHERE location_id = :loc"
        ), {"loc": loc_id}).fetchall()

    mapping = {str(row.unit_number).strip(): row.id for row in rows}
    logger.info(f"    Units loaded: {len(mapping)}")
    return mapping


def load_existing_external_ids(engine) -> set:
    """All external_charge_id values already in ledger_charges (for idempotency)."""
    logger.info("🔍  Loading existing external_charge_ids ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_charge_id FROM storentic.ledger_charges "
            "WHERE external_charge_id IS NOT NULL"
        )).fetchall()
    ids = {row.external_charge_id for row in rows}
    logger.info(f"    Already imported: {len(ids)} charges")
    return ids


# =============================================================================
# STEP 3 — Row transformation
# =============================================================================

def _parse_dt(val):
    """Parse a datetime value from pandas, return None if null/NaT/unparseable."""
    if val is None:
        return None
    # pd.isna handles NaN, NaT, and None; guard with try for non-scalar types
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    try:
        ts = pd.to_datetime(val)
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _to_cents(val) -> int:
    """Convert a dollar amount to integer cents. Returns 0 for null/invalid."""
    try:
        return int(round(float(val) * 100))
    except (TypeError, ValueError):
        return 0


def transform_row(row: pd.Series, customer_id: int, unit_id, charge_type_id: int,
                  loc_id: int, org_id: int, created_by: int) -> dict:
    """Transform one Select Charges row into a ledger_charges insert dict."""

    charge_id  = str(int(float(row["CHARGEID"])))
    dc_amt     = row.get("DCAMT")
    d_chg_strt = _parse_dt(row.get("DCHGSTRT"))
    d_created  = _parse_dt(row.get("DCREATED"))
    d_updated  = _parse_dt(row.get("DUPDATED"))
    now        = datetime.utcnow()

    # memo — read directly from SCHGDESC column in Charges.csv
    raw_memo = row.get("SCHGDESC")
    memo = str(raw_memo).strip() if pd.notna(raw_memo) and str(raw_memo).strip() not in ("", "nan") else None

    # Deleted charges are filtered out before transform — all rows here are POSTED
    emp_id = to_bigint(row.get("EMPLOYEEID"))

    return {
        "external_charge_id": charge_id,
        "customer_id":        customer_id,
        "unit_id":            unit_id,
        "location_id":        loc_id,
        "organization_id":    org_id,
        "charge_type_id":     charge_type_id,
        "amount_in_cents":    _to_cents(dc_amt),
        "effective_date":     d_chg_strt or d_created or now,
        "memo":               memo,
        "internal_note":      None,
        "status":             "POSTED",
        "invoice_id":         None,
        "source_screen":      "MIGRATION",
        "reversed_by_id":     None,
        "reversed_at":        None,
        "reversal_reason":    None,
        "created_by":         created_by,
        "created_at":         d_created or now,
        "updated_at":         d_updated or now,
        "external_employee_id": emp_id,
        "external_system":      "sitelink",
    }


# =============================================================================
# STEP 4 — Main processing loop
# =============================================================================

def _record_to_tuple(record: dict) -> tuple:
    """Convert a record dict to an ordered tuple matching _INSERT_COLS."""
    return tuple(record[col] for col in _INSERT_COLS)


def process_charges(
    charges_file: str,
    charge_type_map: dict,
    ledger_to_tenant: dict,
    ledger_to_unitname: dict,
    customer_map: dict,
    unit_map: dict,
    existing_ids: set,
    loc_id: int,
    org_id: int,
    created_by: int,
    output_mode: str,
    dry_run: bool,
    engine,
    out_file: str,
    batch_size: int,
) -> dict:

    # ── Read source CSV ────────────────────────────────────────────────────────
    logger.info(f"📂  Loading charges file: {charges_file}")
    df = pd.read_csv(charges_file, dtype=str, encoding='latin-1')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()
    df = df[df["CHARGEID"].notna() & (df["CHARGEID"].str.strip() != "")].copy()
    total_raw = len(df)
    logger.info(f"    Total charge rows: {total_raw:,}")

    stats = {
        "total": total_raw, "inserted": 0, "skipped_dup": 0,
        "skipped_deleted": 0, "skipped_no_cust": 0, "skipped_no_ct": 0,
        "errors": 0, "dry_run": dry_run, "output_mode": output_mode,
    }

    # ── Step 1: Vectorized pre-filter ─────────────────────────────────────────

    # Drop deleted/voided charges (DDELETED not null).
    # This includes all charges belonging to same-day-cancelled (abandoned) units
    # whose entire charge history was deleted in SiteLink.
    if "DDELETED" in df.columns:
        deleted_mask = df["DDELETED"].notna() & (df["DDELETED"].str.strip() != "")
        stats["skipped_deleted"] = int(deleted_mask.sum())
        df = df[~deleted_mask].copy()
    logger.info(f"    Skipped deleted charges  : {stats['skipped_deleted']:,}")

    # Parse ChargeDescID and LedgerID as integers; drop unparseable rows
    df["CHARGEDESCID_INT"] = pd.to_numeric(df["CHARGEDESCID"], errors="coerce")
    df["LEDGERID_INT"]     = pd.to_numeric(df["LEDGERID"],     errors="coerce")
    bad_parse = df["CHARGEDESCID_INT"].isna() | df["LEDGERID_INT"].isna()
    if bad_parse.any():
        stats["errors"] += int(bad_parse.sum())
        logger.warning(f"    Dropped {bad_parse.sum()} rows with unparseable CHARGEDESCID/LEDGERID")
    df = df[~bad_parse].copy()
    df["CHARGEDESCID_INT"] = df["CHARGEDESCID_INT"].astype(int)
    df["LEDGERID_INT"]     = df["LEDGERID_INT"].astype(int)

    # Resolve charge_type_id (vectorized)
    df["charge_type_id"] = df["CHARGEDESCID_INT"].map(charge_type_map)
    no_ct_df = df[df["charge_type_id"].isna()].copy()
    stats["skipped_no_ct"] = len(no_ct_df)
    df = df[df["charge_type_id"].notna()].copy()
    df["charge_type_id"] = df["charge_type_id"].astype(int)

    # Resolve tenant_id → customer_id (vectorized)
    df["tenant_id"]   = df["LEDGERID_INT"].map(ledger_to_tenant)
    df["customer_id"] = df["tenant_id"].map(
        lambda t: customer_map.get(int(t)) if pd.notna(t) else None
    )
    no_cust_df = df[df["customer_id"].isna()].copy()
    stats["skipped_no_cust"] = len(no_cust_df)
    df = df[df["customer_id"].notna()].copy()
    df["customer_id"] = df["customer_id"].astype(int)

    # Resolve unit_id (nullable — missing unit is allowed)
    df["_unit_name"] = df["LEDGERID_INT"].map(ledger_to_unitname)
    df["unit_id"]    = df["_unit_name"].map(
        lambda u: unit_map.get(str(u).strip()) if pd.notna(u) else None
    )

    # Build external_charge_id and dedup
    df["ext_charge_id"] = df["CHARGEID"].apply(lambda x: str(int(float(x))))
    dup_mask = df["ext_charge_id"].isin(existing_ids)
    stats["skipped_dup"] = int(dup_mask.sum())
    df = df[~dup_mask].copy().reset_index(drop=True)

    logger.info(f"    After pre-filter      : {len(df):,} rows to process")
    logger.info(f"    Skipped deleted       : {stats['skipped_deleted']:,}")
    logger.info(f"    Skipped no-customer   : {stats['skipped_no_cust']:,}")
    logger.info(f"    Skipped no-chargetype : {stats['skipped_no_ct']:,}")
    logger.info(f"    Skipped duplicates    : {stats['skipped_dup']:,}")
    print(f"  {len(df):,} rows to insert after filtering\n", flush=True)

    # ── Build skipped-rows Excel data (vectorized) ────────────────────────────
    skipped_rows = []
    for r in no_ct_df.itertuples(index=False):
        rd = r._asdict()
        skipped_rows.append({
            "CHARGEID": rd.get("CHARGEID"), "CHARGEDESCID": rd.get("CHARGEDESCID"),
            "LEDGERID": rd.get("LEDGERID"), "DCAMT": rd.get("DCAMT"),
            "DCHGSTRT": rd.get("DCHGSTRT"), "DCREATED": rd.get("DCREATED"),
            "skip_reason": f"ChargeDescID {rd.get('CHARGEDESCID')} not in charge_type mapping",
        })
    for r in no_cust_df.itertuples(index=False):
        rd = r._asdict()
        ledger_id = rd.get("LEDGERID_INT")
        tenant_id = ledger_to_tenant.get(int(ledger_id)) if ledger_id else None
        skipped_rows.append({
            "CHARGEID": rd.get("CHARGEID"), "CHARGEDESCID": rd.get("CHARGEDESCID"),
            "LEDGERID": rd.get("LEDGERID"), "DCAMT": rd.get("DCAMT"),
            "DCHGSTRT": rd.get("DCHGSTRT"), "DCREATED": rd.get("DCREATED"),
            "skip_reason": f"LedgerID {ledger_id} → TenantID {tenant_id} not found in storentic.customer",
        })

    if dry_run:
        stats["inserted"] = len(df)
        logger.info(f"DRY RUN — {len(df):,} rows would be inserted.")
        _flush_skipped(skipped_rows, stats)
        return stats

    # ── Step 2: Excel export mode ──────────────────────────────────────────────
    if output_mode == "excel":
        excel_records = []
        for row in df.itertuples(index=False):
            rd = row._asdict()
            try:
                record = transform_row(
                    pd.Series(rd),
                    customer_id    = rd["customer_id"],
                    unit_id        = rd["unit_id"] if pd.notna(rd.get("unit_id")) else None,
                    charge_type_id = rd["charge_type_id"],
                    loc_id=loc_id, org_id=org_id, created_by=created_by,
                )
                excel_records.append({k: record[k] for k in EXCEL_OUTPUT_COLUMNS if k in record})
                stats["inserted"] += 1
            except Exception as exc:
                log_error(0, rd.get("CHARGEID"), "TRANSFORM", str(exc), rd.get("CHARGEID"))
                stats["errors"] += 1
        if excel_records:
            _write_excel(excel_records, out_file, EXCEL_OUTPUT_COLUMNS, "LedgerCharges")
            stats["excel_output"] = out_file
        _flush_skipped(skipped_rows, stats)
        return stats

    # ── Step 3: Transform rows using itertuples (fast) ────────────────────────
    batch_tuples: list[tuple] = []
    batch_ids:    list[str]   = []
    raw_conn   = engine.raw_connection()
    raw_cursor = raw_conn.cursor()

    def flush_batch():
        if not batch_tuples:
            return
        try:
            execute_values(raw_cursor, _INSERT_SQL, batch_tuples, page_size=batch_size)
            raw_conn.commit()
            stats["inserted"] += len(batch_tuples)
            logger.info(f"    Flushed batch: {len(batch_tuples):,} rows committed.")
        except Exception as exc:
            raw_conn.rollback()
            exc_str = str(exc)
            is_conn_err = any(kw in exc_str for kw in (
                "could not translate host name", "could not connect to server",
                "connection refused", "OperationalError", "timeout",
            ))
            if is_conn_err:
                logger.error(f"DB connection lost: {exc_str[:200]}")
                print(f"\n  ERROR: DB connection lost — re-run to resume.\n", flush=True)
                raise SystemExit(1)
            # Data error — fall back to row-by-row for this batch only
            logger.warning(f"Batch insert failed ({exc_str[:120]}); retrying row-by-row ...")
            for tup, cid in zip(batch_tuples, batch_ids):
                try:
                    execute_values(raw_cursor, _INSERT_SQL, [tup])
                    raw_conn.commit()
                    stats["inserted"] += 1
                except Exception as row_exc:
                    raw_conn.rollback()
                    log_error(0, cid, "DB_INSERT", str(row_exc), cid)
                    stats["errors"] += 1
        finally:
            batch_tuples.clear()
            batch_ids.clear()

    total_to_insert = len(df)
    for row in df.itertuples(index=False):
        rd         = row._asdict()
        charge_id  = rd["ext_charge_id"]
        unit_id    = rd["unit_id"] if pd.notna(rd.get("unit_id")) else None
        try:
            record = transform_row(
                pd.Series(rd),
                customer_id    = rd["customer_id"],
                unit_id        = unit_id,
                charge_type_id = rd["charge_type_id"],
                loc_id=loc_id, org_id=org_id, created_by=created_by,
            )
            batch_tuples.append(_record_to_tuple(record))
            batch_ids.append(charge_id)
            existing_ids.add(charge_id)
            if len(batch_tuples) >= batch_size:
                flush_batch()
                pct = min(100, stats["inserted"] / total_to_insert * 100)
                print(
                    f"  {pct:5.1f}%  |  inserted {stats['inserted']:,} / {total_to_insert:,}"
                    f"  err={stats['errors']:,}",
                    flush=True,
                )
        except Exception as exc:
            log_error(0, charge_id, "TRANSFORM", str(exc), charge_id)
            stats["errors"] += 1

    # ── Flush remainder ───────────────────────────────────────────────────────
    flush_batch()
    raw_cursor.close()
    raw_conn.close()

    # ── Write skipped-rows Excel ──────────────────────────────────────────────
    _flush_skipped(skipped_rows, stats)
    return stats


def _flush_skipped(skipped_rows: list, stats: dict):
    """Write skipped rows to Excel and update stats."""
    if not skipped_rows:
        return
    ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
    skipped_path = os.path.join(OUTPUT_DIR, f"ledger_charges_skipped_{ts}.xlsx")
    _write_excel(skipped_rows, skipped_path, SKIPPED_COLUMNS, "Skipped")
    stats["skipped_output"] = skipped_path
    print(f"\n  ⚠️   {len(skipped_rows):,} skipped rows → {skipped_path}", flush=True)


def _write_excel(records: list, out_path: str, columns: list, sheet_name: str):
    df_out = pd.DataFrame(records, columns=[c for c in columns if c in (records[0] if records else {})])
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(
                len(str(c.value)) if c.value is not None else 0
                for c in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    logger.info(f"📊  Excel written: {out_path}  ({len(records):,} rows)")


# =============================================================================
# STEP 5 — Summary
# =============================================================================

def _print_summary(stats: dict, run_ts: str):
    from scripts.logger import LOG_FILE, ERROR_CSV
    mode  = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 65,
        "  STORENTIC LEDGER CHARGES MIGRATION — SUMMARY",
        f"  Run timestamp     : {run_ts}",
        f"  Output mode       : {mode}",
        f"  Dry run           : {stats.get('dry_run', False)}",
        "=" * 65,
        f"  Total rows read             : {stats.get('total', 0):,}",
        f"  Successfully written        : {stats.get('inserted', 0):,}",
        f"  Skipped (deleted/voided)    : {stats.get('skipped_deleted', 0):,}",
        f"  Skipped (already imported)  : {stats.get('skipped_dup', 0):,}",
        f"  Skipped (no customer match) : {stats.get('skipped_no_cust', 0):,}",
        f"  Skipped (no charge type)    : {stats.get('skipped_no_ct', 0):,}",
        f"  Errors / rejected           : {stats.get('errors', 0):,}",
        "=" * 65,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output   : {stats['excel_output']}")
    if stats.get("skipped_output"):
        lines.append(f"  Skipped rows   : {stats['skipped_output']}")
    lines += [
        f"  Log file       : {LOG_FILE}",
        f"  Error CSV      : {ERROR_CSV}",
        "=" * 65,
    ]
    text = "\n".join(lines)
    print("\n" + text)
    logger.info(text)


# =============================================================================
# CLI
# =============================================================================

def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="SiteLink Select Charges → Storentic Ledger Charges Migration"
    )
    parser.add_argument("--file-charges",      required=True,
                        help="Path to SiteLink Select Charges CSV file")
    parser.add_argument("--file-tenants",      required=True,
                        help="Path to Tenants+Units+Ledgers+Access CSV file")
    parser.add_argument("--file-charge-types", required=True,
                        help="Path to ChargeType.csv mapping file")
    parser.add_argument("--output",   default="db", choices=["db", "excel"],
                        help="Output destination: 'db' (default) or 'excel'")
    parser.add_argument("--out-file", default=None,
                        help="[excel mode] Output Excel file path")
    parser.add_argument("--dry-run",  action="store_true",
                        help="[db mode] Preview without writing to DB")
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    run_ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_mode = args.output
    dry_run     = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    org_id      = int(os.getenv("ORGANIZATION_ID", 1))
    loc_id      = int(os.getenv("LOCATION_ID", 1))
    created_by  = int(os.getenv("CREATED_BY", 0))
    batch_size  = int(os.getenv("BATCH_SIZE", 5000))

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"ledger_charges_transformed_{ts}.xlsx")

    print(
        f"\n  Storentic Ledger Charges Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}  |"
        f"  org={org_id}  loc={loc_id}  created_by={created_by}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("STORENTIC LEDGER CHARGES ETL MIGRATION STARTING")
    logger.info(f"Charges file      : {args.file_charges}")
    logger.info(f"Tenants file      : {args.file_tenants}")
    logger.info(f"Charge types file : {args.file_charge_types}")
    logger.info(f"Memo source       : SCHGDESC column in Charges.csv")
    logger.info(f"Output mode       : {output_mode.upper()}")
    logger.info(f"Dry run           : {dry_run}")
    logger.info(f"Org / Loc / CreatedBy : {org_id} / {loc_id} / {created_by}")
    logger.info(f"Batch size        : {batch_size}")
    logger.info("=" * 65)

    # ── DB connection ─────────────────────────────────────────────────────────
    try:
        from scripts.db import get_engine
        engine = get_engine()
        logger.info("✅  Database connection established.")
    except Exception as exc:
        print(f"\n  ERROR: Cannot connect to database: {exc}\n", flush=True)
        sys.exit(1)

    # ── Pre-flight: check column exists ───────────────────────────────────────
    check_external_charge_id_column(engine)

    # ── Load source file maps ─────────────────────────────────────────────────
    charge_type_map                  = load_charge_type_map(args.file_charge_types)
    ledger_to_tenant, ledger_to_unit = load_tenants_maps(args.file_tenants)

    # ── Load DB lookup maps ───────────────────────────────────────────────────
    customer_map = load_customer_map(engine, org_id)
    unit_map     = load_unit_map(engine, loc_id)
    existing_ids = load_existing_external_ids(engine)

    # ── Process ───────────────────────────────────────────────────────────────
    stats = process_charges(
        charges_file        = args.file_charges,
        charge_type_map     = charge_type_map,
        ledger_to_tenant    = ledger_to_tenant,
        ledger_to_unitname  = ledger_to_unit,
        customer_map        = customer_map,
        unit_map            = unit_map,
        existing_ids        = existing_ids,
        loc_id              = loc_id,
        org_id              = org_id,
        created_by          = created_by,
        output_mode         = output_mode,
        dry_run             = dry_run,
        engine              = engine,
        out_file            = out_file,
        batch_size          = batch_size,
    )

    _print_summary(stats, run_ts)
    close_logger()


if __name__ == "__main__":
    main()
