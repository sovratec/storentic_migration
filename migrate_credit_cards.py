"""
migrate_credit_cards.py
=======================
Migrates credit card records from CreditCards.xlsx into storentic.payment_profiles.

Customer lookup
---------------
Last name is extracted from the Name column (text before the comma, e.g. "Waldrop, J" → "Waldrop").
If zero or multiple customers share that last name the row is skipped and written to the errors report.

Deduplication
-------------
Match key: (customer_id, card_last4, card_exp_month, card_exp_year).
Existing record found → UPDATE.  No record found → INSERT.

Usage
-----
    python migrate_credit_cards.py --input data/CreditCards.xlsx
    python migrate_credit_cards.py --input data/CreditCards.xlsx --dry-run

Environment (.env)
------------------
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import os
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from sqlalchemy import text

from scripts.logger import logger
from scripts.db import get_engine
from scripts.customer_transformer import clean_str

ORG_ID                 = 1
LOC_ID                 = 4
PAYMENT_METHOD_TYPE_ID = 1

_BRAND_MAP = {
    "master card":      "mastercard",
    "mastercard":       "mastercard",
    "visa":             "visa",
    "american express": "amex",
    "amex":             "amex",
    "discover":         "discover",
}


def _normalize_brand(raw: str) -> str | None:
    if not raw:
        return None
    return _BRAND_MAP.get(raw.strip().lower(), raw.strip().lower())


def _extract_last4(card_number: str) -> str | None:
    if not card_number:
        return None
    s = str(card_number).strip().replace("*", "").replace(" ", "")
    return s[-4:] if len(s) >= 4 else (s or None)


def _parse_expiry(raw) -> tuple:
    """Returns (month, year) from ExpirationDate. Both None on failure."""
    if raw is None or str(raw).lower() in ("", "nan", "nat", "none"):
        return None, None
    try:
        if hasattr(raw, "month"):
            return int(raw.month), int(raw.year)
        dt = pd.to_datetime(str(raw))
        return int(dt.month), int(dt.year)
    except Exception:
        return None, None


# ── SQL ────────────────────────────────────────────────────────────────────────

_INSERT_SQL = text("""
    INSERT INTO storentic.payment_profiles
        (customer_id, payment_method_type_id, card_brand, card_last4,
         card_exp_month, card_exp_year, billing_name, billing_address_line1,
         billing_postal_code, billing_country, is_default, is_active,
         location_id, organization_id, created_at, updated_at)
    VALUES
        (:customer_id, :payment_method_type_id, :card_brand, :card_last4,
         :card_exp_month, :card_exp_year, :billing_name, :billing_address_line1,
         :billing_postal_code, 'US', false, true,
         :location_id, :organization_id, NOW(), NOW())
""")

_UPDATE_SQL = text("""
    UPDATE storentic.payment_profiles
    SET card_brand            = :card_brand,
        card_exp_month        = :card_exp_month,
        card_exp_year         = :card_exp_year,
        billing_name          = :billing_name,
        billing_address_line1 = :billing_address_line1,
        billing_postal_code   = :billing_postal_code,
        updated_at            = NOW()
    WHERE id = :id
""")


# ── Helpers ────────────────────────────────────────────────────────────────────

def load_customer_map(engine) -> dict:
    """Returns {lower(last_name): [(customer_id, address1), ...]} for this org."""
    logger.info("🔍 Loading customer map ...")
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, last_name, address1 FROM storentic.customer "
            "WHERE last_name IS NOT NULL AND organization_id = :org"
        ), {"org": ORG_ID}).fetchall()
    result: dict = {}
    for row in rows:
        key = row.last_name.strip().lower()
        result.setdefault(key, []).append((row.id, row.address1 or ""))
    logger.info(f"    Customers loaded: {len(rows)}")
    return result


def load_existing_profiles(engine) -> dict:
    """Returns {(customer_id, card_last4, exp_month, exp_year): profile_id}."""
    logger.info("🔍 Loading existing payment profiles ...")
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, customer_id, card_last4, card_exp_month, card_exp_year "
            "FROM storentic.payment_profiles WHERE organization_id = :org"
        ), {"org": ORG_ID}).fetchall()
    result = {
        (r.customer_id, r.card_last4, r.card_exp_month, r.card_exp_year): r.id
        for r in rows
    }
    logger.info(f"    Existing profiles: {len(result)}")
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(args=None):
    p = argparse.ArgumentParser(description="Migrate credit cards → payment_profiles")
    p.add_argument("--input",   required=True, help="Path to CreditCards.xlsx")
    p.add_argument("--output",  default="db", choices=["db"])
    p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    return p.parse_args(args)


# ── Main ───────────────────────────────────────────────────────────────────────

def main(args=None):
    if args is None:
        args = parse_args()

    dry_run = args.dry_run
    now_ts  = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info("Credit Card Migration")
    logger.info(f"Input    : {args.input}")
    logger.info(f"Dry run  : {dry_run}")
    logger.info(f"Org / Loc: {ORG_ID} / {LOC_ID}")
    logger.info("=" * 60)

    # ── Load file ─────────────────────────────────────────────────
    logger.info(f"📂 Loading file: {args.input}")
    df = pd.read_excel(args.input, dtype=str)
    df.columns = df.columns.str.strip()
    df = df.fillna("").apply(lambda c: c.str.strip() if c.dtype == "object" else c)
    logger.info(f"    Rows read: {len(df)}")

    # ── DB ────────────────────────────────────────────────────────
    engine = get_engine()
    logger.info("✅ Database connection established.")

    customer_map      = load_customer_map(engine)
    existing_profiles = load_existing_profiles(engine)

    skipped_rows: list = []
    error_rows:   list = []
    stats = {"total": 0, "inserted": 0, "updated": 0, "skipped": 0, "errors": 0}

    # ── Row loop ──────────────────────────────────────────────────
    for idx, row in df.iterrows():
        stats["total"] += 1
        row_num  = idx + 2
        row_dict = row.to_dict()

        # Extract last name
        raw_name = row.get("Name", "")
        if not raw_name or "," not in raw_name:
            msg = f"Name missing or no comma: '{raw_name}'"
            logger.warning(f"  Row {row_num}: SKIP — {msg}")
            skipped_rows.append({**row_dict, "skip_reason": msg})
            stats["skipped"] += 1
            continue

        last_name = raw_name.split(",")[0].strip()
        matches   = customer_map.get(last_name.lower(), [])

        if len(matches) == 0:
            msg = f"No customer found with last_name='{last_name}'"
            logger.warning(f"  Row {row_num}: SKIP — {msg}")
            error_rows.append({**row_dict, "error": msg})
            stats["skipped"] += 1
            continue

        if len(matches) == 1:
            customer_id = matches[0][0]
        else:
            # Multiple customers — try to narrow down by address
            excel_addr = str(row.get("Address", "")).strip().lower()
            addr_matches = [
                cid for cid, db_addr in matches
                if db_addr.strip().lower() == excel_addr
            ]
            if len(addr_matches) == 1:
                customer_id = addr_matches[0]
                logger.info(f"  Row {row_num}: Resolved ambiguous last_name='{last_name}' via address match")
            elif len(addr_matches) == 0:
                msg = f"Ambiguous: {len(matches)} customers with last_name='{last_name}', no address match for '{row.get('Address', '')}'"
                logger.warning(f"  Row {row_num}: SKIP — {msg}")
                error_rows.append({**row_dict, "error": msg})
                stats["skipped"] += 1
                continue
            else:
                msg = f"Ambiguous: {len(addr_matches)} customers with last_name='{last_name}' and same address"
                logger.warning(f"  Row {row_num}: SKIP — {msg}")
                error_rows.append({**row_dict, "error": msg})
                stats["skipped"] += 1
                continue

        card_last4      = _extract_last4(row.get("CardNumber", ""))
        exp_month, exp_year = _parse_expiry(row.get("ExpirationDate", ""))
        card_brand      = _normalize_brand(row.get("CardType", ""))
        billing_name    = clean_str(row.get("NameOnCard", ""))
        billing_address = clean_str(row.get("Address", ""))
        billing_postal  = clean_str(str(row.get("ZipCode", ""))) or None

        record = {
            "customer_id":            customer_id,
            "payment_method_type_id": PAYMENT_METHOD_TYPE_ID,
            "card_brand":             card_brand,
            "card_last4":             card_last4,
            "card_exp_month":         exp_month,
            "card_exp_year":          exp_year,
            "billing_name":           billing_name,
            "billing_address_line1":  billing_address,
            "billing_postal_code":    billing_postal,
            "location_id":            LOC_ID,
            "organization_id":        ORG_ID,
        }

        profile_key = (customer_id, card_last4, exp_month, exp_year)
        existing_id = existing_profiles.get(profile_key)

        try:
            if dry_run:
                action = "UPDATE" if existing_id else "INSERT"
                logger.info(f"  Row {row_num}: [DRY RUN] {action} — {last_name} / *{card_last4}")
                if existing_id:
                    stats["updated"] += 1
                else:
                    stats["inserted"] += 1
            elif existing_id:
                with engine.begin() as conn:
                    conn.execute(_UPDATE_SQL, {**record, "id": existing_id})
                stats["updated"] += 1
                logger.info(f"  Row {row_num}: UPDATED  — {last_name} / *{card_last4}")
            else:
                with engine.begin() as conn:
                    conn.execute(_INSERT_SQL, record)
                stats["inserted"] += 1
                logger.info(f"  Row {row_num}: INSERTED — {last_name} / *{card_last4}")

        except Exception as exc:
            msg = str(exc)
            logger.error(f"  Row {row_num}: ERROR — {last_name}: {msg}")
            error_rows.append({**row_dict, "error": msg})
            stats["errors"] += 1

    # ── Output reports ────────────────────────────────────────────
    out_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(out_dir, exist_ok=True)

    if skipped_rows:
        skip_path = os.path.join(out_dir, f"credit_cards_skipped_{now_ts}.xlsx")
        pd.DataFrame(skipped_rows).to_excel(skip_path, index=False)
        logger.info(f"📄 Skipped report : {skip_path}")

    if error_rows:
        err_path = os.path.join(out_dir, f"credit_cards_errors_{now_ts}.xlsx")
        pd.DataFrame(error_rows).to_excel(err_path, index=False)
        logger.info(f"📄 Errors report  : {err_path}")

    # ── Summary ───────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  Total    : {stats['total']}")
    logger.info(f"  Inserted : {stats['inserted']}")
    logger.info(f"  Updated  : {stats['updated']}")
    logger.info(f"  Skipped  : {stats['skipped']}")
    logger.info(f"  Errors   : {stats['errors']}")
    logger.info("=" * 60)
    logger.info("Done.")


if __name__ == "__main__":
    main()
