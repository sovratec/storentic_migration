"""
migrate_credits.py — ETL script: SiteLink Credits CSV → storentic.ledger_charges

Credits are stored as negative-amount ledger charges with a Waived Off charge type.

Column mapping
---------------
external_charge_id  = CreditID
customer_id         = TenantID  (lookup → storentic.customer.external_id)
unit_id             = sUnitName (lookup → storentic.units.unit_number + LOCATION_ID, nullable)
charge_type_id      = derived from SMEMO:
                          'Waived current late fee 1.' → 10000
                          'Waived current late fee 2.' → 10001
                          'Reservation Fee'            → 10002
amount_in_cents     = dcCreditAmt * 100 * -1   (negative — credit reduces balance)
effective_date      = dCredit
memo                = SMEMO
status              = POSTED
source_screen       = MIGRATION
created_at          = dCreated  (falls back to dCredit if dCreated is sentinel 1900-01-01)

Filters applied before import
------------------------------
- Keep only rows where dcCreditAmt > 0  (drop negative double-entry counterpart)
- Drop rows where bNonPosting = True
- Drop rows where dDeleted is not null/blank
- Drop rows where LedgerID is blank
- Drop rows where SMEMO has no charge_type mapping (logged as skipped)

Pre-requisites
--------------
Run sql/V11__credits_charge_types.sql before executing this script.

Usage
-----
    python migrate_credits.py \\
        --file-credits  "data/Credits.csv" \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260502.csv" \\
        --output db

    python migrate_credits.py \\
        --file-credits  "data/Credits.csv" \\
        --file-tenants  "data/Tenants_Units_Ledgers_Access_20260502.csv" \\
        --output db --dry-run

Environment (.env)
------------------
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID   — written to every row
    LOCATION_ID       — scopes unit lookup and written to every row
    CREATED_BY        — user ID written to created_by column
    BATCH_SIZE        — rows per DB commit (default: 500)
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from sqlalchemy import text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

from scripts.logger import logger, log_error, log_skipped, close as close_logger
from scripts import to_bigint

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── SMEMO → charge_type_id mapping ────────────────────────────────────────────
_SMEMO_TO_CHARGE_TYPE: dict[str, int] = {
    "waived current late fee 1.": 10000,
    "waived current late fee 2.": 10001,
    "reservation fee":            10002,
}

def _clean_str(value) -> str | None:
    """Strip and return None for blank/nan values."""
    if value is None:
        return None
    s = str(value).strip()
    return None if s.lower() in ("nan", "none", "") else s


# ── SQL ────────────────────────────────────────────────────────────────────────
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
    "effective_date", "memo", "status", "created_at",
]

SKIPPED_COLUMNS = [
    "CREDITID", "LEDGERID", "DCCREDITAMT", "SMEMO",
    "DCREDIT", "BNONPOSTING", "DDELETED", "skip_reason",
]

# Sentinel date SiteLink uses for "no date" — treat as null
_SITELINK_EPOCH = pd.Timestamp("1900-01-01")


# =============================================================================
# Step 1 — Source file loaders
# =============================================================================

def load_credits_file(filepath: str) -> pd.DataFrame:
    """
    Read Credits.csv. Applies all pre-import filters:
      - dcCreditAmt > 0  (keep positive side of double-entry pair only)
      - bNonPosting != True
      - dDeleted is null/blank
      - LedgerID not blank
    """
    logger.info(f"Loading credits file: {filepath}")
    df = pd.read_csv(filepath, dtype=str, low_memory=False, encoding="latin-1")
    df.columns = df.columns.str.strip().str.upper()
    df = df.drop(columns=["Totals & Averages"], errors="ignore")

    required = {"CREDITID", "LEDGERID", "DCCREDITAMT", "DCREDIT", "SMEMO"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Credits file missing columns: {missing}")

    total_raw = len(df)

    # Numeric amount
    df["DCCREDITAMT"] = pd.to_numeric(df["DCCREDITAMT"], errors="coerce")

    # Filter: keep positive amounts only (drop double-entry negative rows + zeros)
    df = df[df["DCCREDITAMT"] > 0].copy()
    logger.info(f"    After dcCreditAmt > 0 filter : {len(df):,}  (dropped {total_raw - len(df):,} negative/zero rows)")

    # Filter: drop bNonPosting = True (column may not exist in all exports)
    if "BNONPOSTING" in df.columns:
        before = len(df)
        df = df[df["BNONPOSTING"].str.strip().str.lower() != "true"].copy()
        logger.info(f"    After bNonPosting filter     : {len(df):,}  (dropped {before - len(df):,} non-posting rows)")

    # Filter: drop deleted credits (column may not exist in all exports)
    if "DDELETED" in df.columns:
        before = len(df)
        df = df[df["DDELETED"].isna() | (df["DDELETED"].str.strip() == "")].copy()
        logger.info(f"    After dDeleted filter        : {len(df):,}  (dropped {before - len(df):,} deleted rows)")

    # Filter: drop blank LedgerID
    before = len(df)
    df = df[df["LEDGERID"].notna() & (df["LEDGERID"].str.strip() != "")].copy()
    logger.info(f"    After LedgerID filter        : {len(df):,}  (dropped {before - len(df):,} blank-LedgerID rows)")

    df = df.reset_index(drop=True)
    logger.info(f"    Rows to process: {len(df):,}")
    return df


def load_tenants_maps(tenants_file: str) -> tuple[dict, dict]:
    """
    Read Tenants CSV → {LedgerID: TenantID}, {LedgerID: sUnitName}
    """
    logger.info(f"Loading tenants file: {tenants_file}")
    df = pd.read_csv(tenants_file, dtype=str, encoding='latin-1')
    df = df.drop(columns=["Totals & Averages"], errors="ignore")
    df.columns = df.columns.str.strip().str.upper()

    required = {"LEDGERID", "TENANTID", "SUNITNAME"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"❌  Tenants file missing columns: {missing}")

    ledger_to_tenant: dict[int, int] = {}
    ledger_to_unit:   dict[int, str] = {}

    for _, row in df.iterrows():
        try:
            lid = int(float(row["LEDGERID"]))
            tid = int(float(row["TENANTID"]))
            unit = str(row["SUNITNAME"]).strip() if pd.notna(row["SUNITNAME"]) else None
            ledger_to_tenant[lid] = tid
            ledger_to_unit[lid]   = unit
        except (ValueError, TypeError):
            pass

    logger.info(f"    LedgerID entries: {len(ledger_to_tenant):,}")
    return ledger_to_tenant, ledger_to_unit


# =============================================================================
# Step 2 — DB lookups
# =============================================================================

def load_customer_map(engine, org_id: int) -> dict[int, int]:
    """{TenantID: customer.id} for all migrated customers."""
    logger.info(f"Loading customer map for org_id={org_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, external_id FROM storentic.customer "
            "WHERE external_id IS NOT NULL AND organization_id = :org"
        ), {"org": org_id}).fetchall()
    mapping = {}
    for row in rows:
        try:
            mapping[int(float(row.external_id))] = row.id
        except (ValueError, TypeError):
            pass
    logger.info(f"    Customers loaded: {len(mapping):,}")
    return mapping


def load_unit_map(engine, loc_id: int) -> dict[str, int]:
    """{unit_number: units.id} for the given location."""
    logger.info(f"Loading unit map for location_id={loc_id} ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT id, unit_number FROM storentic.units WHERE location_id = :loc"
        ), {"loc": loc_id}).fetchall()
    mapping = {str(r.unit_number).strip(): r.id for r in rows}
    logger.info(f"    Units loaded: {len(mapping):,}")
    return mapping


def load_existing_external_ids(engine) -> set[str]:
    """All external_charge_id values already in ledger_charges (idempotency)."""
    logger.info("Loading existing external_charge_ids ...")
    with engine.connect() as conn:
        rows = conn.execute(sa_text(
            "SELECT external_charge_id FROM storentic.ledger_charges "
            "WHERE external_charge_id IS NOT NULL"
        )).fetchall()
    ids = {r.external_charge_id for r in rows}
    logger.info(f"    Already imported: {len(ids):,} charges")
    return ids


# =============================================================================
# Step 3 — Helpers
# =============================================================================

def _parse_dt(val) -> datetime | None:
    """Parse date string to datetime; returns None for null, blank, or 1900 sentinel."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "nat", "none"):
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        if ts.date() <= _SITELINK_EPOCH.date():
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _derive_charge_type(smemo: str) -> int | None:
    """Map SMEMO string to charge_type_id. Returns None if not mapped."""
    if not smemo:
        return None
    return _SMEMO_TO_CHARGE_TYPE.get(smemo.strip().lower())


def _to_negative_cents(val) -> int:
    """Convert positive dollar amount to negative integer cents."""
    try:
        return -abs(int(round(float(val) * 100)))
    except (TypeError, ValueError):
        return 0


# =============================================================================
# Step 4 — Main processing loop
# =============================================================================

def process_credits(
    df: pd.DataFrame,
    ledger_to_tenant: dict,
    ledger_to_unit: dict,
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

    total_raw = len(df)
    stats = {
        "total": total_raw, "inserted": 0, "skipped_dup": 0,
        "skipped_no_cust": 0, "skipped_no_memo_map": 0,
        "errors": 0, "dry_run": dry_run, "output_mode": output_mode,
    }

    now = datetime.utcnow()

    # ── Step 1: Vectorized pre-filter ─────────────────────────────────────────

    # Build external_credit_id and dedup
    def _to_id_str(x):
        s = str(x).strip()
        if s.lower() in ("", "nan", "none"):
            return ""
        try:
            return str(int(float(s)))
        except (ValueError, TypeError):
            return s
    df["ext_credit_id"] = df["CREDITID"].apply(_to_id_str)
    dup_mask = df["ext_credit_id"].isin(existing_ids)
    stats["skipped_dup"] = int(dup_mask.sum())
    df = df[~dup_mask].copy()

    # Normalise SMEMO and map to charge_type_id
    df["_smemo_norm"] = df["SMEMO"].fillna("").str.strip().str.lower()
    df["charge_type_id"] = df["_smemo_norm"].map(_SMEMO_TO_CHARGE_TYPE)
    no_map_df = df[df["charge_type_id"].isna()].copy()
    stats["skipped_no_memo_map"] = len(no_map_df)
    df = df[df["charge_type_id"].notna()].copy()
    df["charge_type_id"] = df["charge_type_id"].astype(int)

    # Parse LedgerID and resolve tenant_id → customer_id
    df["LEDGERID_INT"] = pd.to_numeric(df["LEDGERID"], errors="coerce")
    bad_lid = df["LEDGERID_INT"].isna()
    if bad_lid.any():
        stats["errors"] += int(bad_lid.sum())
        logger.warning(f"    Dropped {bad_lid.sum()} rows with unparseable LEDGERID")
    df = df[~bad_lid].copy()
    df["LEDGERID_INT"] = df["LEDGERID_INT"].astype(int)

    df["tenant_id"]   = df["LEDGERID_INT"].map(ledger_to_tenant)
    df["customer_id"] = df["tenant_id"].map(
        lambda t: customer_map.get(int(t)) if pd.notna(t) else None
    )
    no_cust_df = df[df["customer_id"].isna()].copy()
    stats["skipped_no_cust"] = len(no_cust_df)
    df = df[df["customer_id"].notna()].copy()
    df["customer_id"] = df["customer_id"].astype(int)

    # Resolve unit_id (nullable)
    df["_unit_name"] = df["LEDGERID_INT"].map(ledger_to_unit)
    df["unit_id"]    = df["_unit_name"].map(
        lambda u: unit_map.get(str(u).strip()) if pd.notna(u) else None
    )

    df = df.reset_index(drop=True)

    logger.info(f"    After pre-filter      : {len(df):,} rows to process")
    logger.info(f"    Skipped duplicates    : {stats['skipped_dup']:,}")
    logger.info(f"    Skipped no-SMEMO-map  : {stats['skipped_no_memo_map']:,}")
    logger.info(f"    Skipped no-customer   : {stats['skipped_no_cust']:,}")
    print(f"  {len(df):,} rows to insert after filtering\n", flush=True)

    # ── Build skipped-rows list ───────────────────────────────────────────────
    skipped_rows: list[dict] = []
    for r in no_map_df.itertuples(index=False):
        rd = r._asdict()
        smemo = rd.get("SMEMO", "") or ""
        skipped_rows.append({
            "CREDITID": rd.get("CREDITID"), "LEDGERID": rd.get("LEDGERID"),
            "DCCREDITAMT": rd.get("DCCREDITAMT"), "SMEMO": smemo,
            "DCREDIT": rd.get("DCREDIT"),
            "BNONPOSTING": rd.get("BNONPOSTING") if "BNONPOSTING" in rd else None,
            "DDELETED": rd.get("DDELETED") if "DDELETED" in rd else None,
            "skip_reason": f"SMEMO '{smemo.strip()}' has no charge_type mapping",
        })
    for r in no_cust_df.itertuples(index=False):
        rd = r._asdict()
        ledger_id = rd.get("LEDGERID_INT")
        tenant_id = ledger_to_tenant.get(int(ledger_id)) if ledger_id else None
        skipped_rows.append({
            "CREDITID": rd.get("CREDITID"), "LEDGERID": rd.get("LEDGERID"),
            "DCCREDITAMT": rd.get("DCCREDITAMT"), "SMEMO": rd.get("SMEMO"),
            "DCREDIT": rd.get("DCREDIT"),
            "BNONPOSTING": rd.get("BNONPOSTING") if "BNONPOSTING" in rd else None,
            "DDELETED": rd.get("DDELETED") if "DDELETED" in rd else None,
            "skip_reason": f"LedgerID {ledger_id} → TenantID {tenant_id} not found in storentic.customer",
        })

    if dry_run:
        stats["inserted"] = len(df)
        logger.info(f"DRY RUN — {len(df):,} rows would be inserted.")
        _write_skipped(skipped_rows, stats)
        return stats

    # ── Step 2: Excel export mode ──────────────────────────────────────────────
    if output_mode == "excel":
        excel_records = []
        for row in df.itertuples(index=False):
            rd = row._asdict()
            credit_id      = rd["ext_credit_id"]
            smemo          = str(rd.get("SMEMO", "") or "").strip()
            effective_date = _parse_dt(rd.get("DCREDIT")) or now
            created_dt     = _parse_dt(rd.get("DCREATED")) or effective_date
            record = _build_record(
                credit_id, rd["customer_id"],
                rd["unit_id"] if pd.notna(rd.get("unit_id")) else None,
                rd["charge_type_id"], smemo, rd.get("DCCREDITAMT"),
                effective_date, created_dt, loc_id, org_id, created_by,
                employee_id=to_bigint(rd.get("EMPLOYEEID")),
            )
            excel_records.append({k: record[k] for k in EXCEL_OUTPUT_COLUMNS if k in record})
            stats["inserted"] += 1
        if excel_records:
            _write_excel(excel_records, out_file, EXCEL_OUTPUT_COLUMNS, "Credits")
            stats["excel_output"] = out_file
        _write_skipped(skipped_rows, stats)
        return stats

    # ── Step 3: Transform with itertuples + bulk insert ────────────────────────
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
            logger.info(f"    Flushed {len(batch_tuples):,} rows (total: {stats['inserted']:,})")
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
            logger.warning(f"Batch failed ({exc_str[:120]}); retrying row-by-row ...")
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
        credit_id  = rd["ext_credit_id"]
        smemo      = str(rd.get("SMEMO", "") or "").strip()
        unit_id    = rd["unit_id"] if pd.notna(rd.get("unit_id")) else None
        try:
            effective_date = _parse_dt(rd.get("DCREDIT")) or now
            created_dt     = _parse_dt(rd.get("DCREATED")) or effective_date
            record = _build_record(
                credit_id, rd["customer_id"], unit_id,
                rd["charge_type_id"], smemo, rd.get("DCCREDITAMT"),
                effective_date, created_dt, loc_id, org_id, created_by,
                employee_id=to_bigint(rd.get("EMPLOYEEID")),
            )
            batch_tuples.append(tuple(record[col] for col in _INSERT_COLS))
            batch_ids.append(credit_id)
            existing_ids.add(credit_id)
            if len(batch_tuples) >= batch_size:
                flush_batch()
                pct = min(100, stats["inserted"] / total_to_insert * 100)
                print(
                    f"  {pct:5.1f}%  |  inserted {stats['inserted']:,} / {total_to_insert:,}"
                    f"  err={stats['errors']:,}",
                    flush=True,
                )
        except Exception as exc:
            log_error(0, credit_id, "TRANSFORM", str(exc), credit_id)
            stats["errors"] += 1

    flush_batch()
    raw_cursor.close()
    raw_conn.close()

    _write_skipped(skipped_rows, stats)
    return stats


def _build_record(
    credit_id: str, customer_id: int, unit_id,
    charge_type_id: int, smemo: str, dc_credit_amt,
    effective_date: datetime, created_dt: datetime,
    loc_id: int, org_id: int, created_by: int,
    employee_id: int | None = None,
) -> dict:
    """Assemble the ledger_charges insert dict for one credit row."""
    return {
        "external_charge_id": credit_id,
        "customer_id":        customer_id,
        "unit_id":            unit_id,
        "location_id":        loc_id,
        "organization_id":    org_id,
        "charge_type_id":     charge_type_id,
        "amount_in_cents":    _to_negative_cents(dc_credit_amt),
        "effective_date":     effective_date,
        "memo":               smemo or None,
        "internal_note":      None,
        "status":             "POSTED",
        "invoice_id":         None,
        "source_screen":      "MIGRATION",
        "reversed_by_id":     None,
        "reversed_at":        None,
        "reversal_reason":    None,
        "created_by":         created_by,
        "created_at":         created_dt,
        "updated_at":         created_dt,
        "external_employee_id": employee_id,
        "external_system":      "sitelink",
    }


def _write_skipped(skipped_rows: list, stats: dict):
    """Write skipped rows to Excel and update stats."""
    if not skipped_rows:
        return
    ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
    skipped_path = os.path.join(OUTPUT_DIR, f"credits_skipped_{ts}.xlsx")
    _write_excel(skipped_rows, skipped_path, SKIPPED_COLUMNS, "Skipped")
    stats["skipped_output"] = skipped_path
    print(f"\n  ⚠️   {len(skipped_rows):,} skipped rows → {skipped_path}", flush=True)


def _write_excel(records: list, out_path: str, columns: list, sheet_name: str):
    cols = [c for c in columns if c in (records[0] if records else {})]
    df_out = pd.DataFrame(records, columns=cols)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 50)
    logger.info(f"Excel written: {out_path}  ({len(records):,} rows)")


# =============================================================================
# Step 5 — Summary
# =============================================================================

def _print_summary(stats: dict, run_ts: str):
    from scripts.logger import LOG_FILE, ERROR_CSV, SKIPPED_CSV
    mode  = stats.get("output_mode", "db").upper()
    lines = [
        "=" * 65,
        "  STORENTIC CREDITS MIGRATION — SUMMARY",
        f"  Run timestamp     : {run_ts}",
        f"  Output mode       : {mode}",
        f"  Dry run           : {stats.get('dry_run', False)}",
        "=" * 65,
        f"  Total rows read            : {stats.get('total', 0):,}",
        f"  Successfully written       : {stats.get('inserted', 0):,}",
        f"  Skipped (already imported) : {stats.get('skipped_dup', 0):,}",
        f"  Skipped (no customer)      : {stats.get('skipped_no_cust', 0):,}",
        f"  Skipped (no SMEMO mapping) : {stats.get('skipped_no_memo_map', 0):,}",
        f"  Errors / rejected          : {stats.get('errors', 0):,}",
        "=" * 65,
    ]
    if stats.get("excel_output"):
        lines.append(f"  Excel output   : {stats['excel_output']}")
    if stats.get("skipped_output"):
        lines.append(f"  Skipped rows   : {stats['skipped_output']}")
    lines += [
        f"  Log file       : {LOG_FILE}",
        f"  Error CSV      : {ERROR_CSV}",
        f"  Skipped CSV    : {SKIPPED_CSV}",
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
        description="SiteLink Credits CSV → storentic.ledger_charges migration"
    )
    parser.add_argument("--file-credits", required=True,
                        help="Path to Credits.csv (SiteLink export)")
    parser.add_argument("--file-tenants", required=True,
                        help="Path to Tenants+Units+Ledgers+Access Excel file")
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
    batch_size  = int(os.getenv("BATCH_SIZE", 500))

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = args.out_file or os.path.join(OUTPUT_DIR, f"credits_transformed_{ts}.xlsx")

    print(
        f"\n  Storentic Credits Migration"
        f"\n  Mode: {output_mode.upper()}{'  [DRY RUN]' if dry_run else ''}  |"
        f"  org={org_id}  loc={loc_id}  created_by={created_by}\n",
        flush=True,
    )
    logger.info("=" * 65)
    logger.info("STORENTIC CREDITS ETL MIGRATION STARTING")
    logger.info(f"Credits file  : {args.file_credits}")
    logger.info(f"Tenants file  : {args.file_tenants}")
    logger.info(f"Output mode   : {output_mode.upper()}")
    logger.info(f"Dry run       : {dry_run}")
    logger.info(f"Org/Loc/By    : {org_id}/{loc_id}/{created_by}")
    logger.info(f"Batch size    : {batch_size}")
    logger.info("=" * 65)

    # ── DB connection ─────────────────────────────────────────────────────────
    try:
        from scripts.db import get_engine
        engine = get_engine()
        logger.info("Database connection established.")
    except Exception as exc:
        print(f"\n  ERROR: Cannot connect to database: {exc}\n", flush=True)
        sys.exit(1)

    # ── Load sources ──────────────────────────────────────────────────────────
    df_credits                         = load_credits_file(args.file_credits)
    ledger_to_tenant, ledger_to_unit   = load_tenants_maps(args.file_tenants)

    # ── DB lookups ────────────────────────────────────────────────────────────
    customer_map = load_customer_map(engine, org_id)
    unit_map     = load_unit_map(engine, loc_id)
    existing_ids = load_existing_external_ids(engine)

    # ── Process ───────────────────────────────────────────────────────────────
    stats = process_credits(
        df               = df_credits,
        ledger_to_tenant = ledger_to_tenant,
        ledger_to_unit   = ledger_to_unit,
        customer_map     = customer_map,
        unit_map         = unit_map,
        existing_ids     = existing_ids,
        loc_id           = loc_id,
        org_id           = org_id,
        created_by       = created_by,
        output_mode      = output_mode,
        dry_run          = dry_run,
        engine           = engine,
        out_file         = out_file,
        batch_size       = batch_size,
    )

    _print_summary(stats, run_ts)
    close_logger()


if __name__ == "__main__":
    main()
