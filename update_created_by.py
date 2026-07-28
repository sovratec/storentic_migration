"""
update_created_by.py
====================
Updates created_by on migrated records by resolving external_employee_id
to the matching storentic.users.id.

Tables updated:
    storentic.customer
    storentic.rental_agreements
    storentic.billing_schedules
    storentic.payments
    storentic.ledger_charges
    storentic.invoices

Usage
-----
    python update_created_by.py              # live update
    python update_created_by.py --dry-run    # preview row counts only

Environment
-----------
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


# ── SQL ────────────────────────────────────────────────────────────────────────

_UPDATES = [
    {
        "table": "storentic.customer",
        "sql": """
            UPDATE storentic.customer c
            SET created_by = (
                SELECT u.id FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
            WHERE external_employee_id IS NOT NULL
        """,
        "preview_sql": """
            SELECT COUNT(*) FROM storentic.customer c
            WHERE external_employee_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
        """,
    },
    {
        "table": "storentic.rental_agreements",
        "sql": """
            UPDATE storentic.rental_agreements c
            SET created_by = (
                SELECT u.id FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
            WHERE external_employee_id IS NOT NULL
        """,
        "preview_sql": """
            SELECT COUNT(*) FROM storentic.rental_agreements c
            WHERE external_employee_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
        """,
    },
    {
        "table": "storentic.billing_schedules",
        "sql": """
            UPDATE storentic.billing_schedules c
            SET created_by = (
                SELECT u.id FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
            WHERE external_employee_id IS NOT NULL
        """,
        "preview_sql": """
            SELECT COUNT(*) FROM storentic.billing_schedules c
            WHERE external_employee_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
        """,
    },
    {
        "table": "storentic.payments",
        "sql": """
            UPDATE storentic.payments c
            SET created_by = (
                SELECT u.id FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
            WHERE external_employee_id IS NOT NULL
        """,
        "preview_sql": """
            SELECT COUNT(*) FROM storentic.payments c
            WHERE external_employee_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
        """,
    },
    {
        "table": "storentic.ledger_charges",
        "sql": """
            UPDATE storentic.ledger_charges c
            SET created_by = (
                SELECT u.id FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
            WHERE external_employee_id IS NOT NULL
        """,
        "preview_sql": """
            SELECT COUNT(*) FROM storentic.ledger_charges c
            WHERE external_employee_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
        """,
    },
    {
        "table": "storentic.invoices",
        "sql": """
            UPDATE storentic.invoices c
            SET created_by = (
                SELECT u.id FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
            WHERE external_employee_id IS NOT NULL
        """,
        "preview_sql": """
            SELECT COUNT(*) FROM storentic.invoices c
            WHERE external_employee_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM storentic.users u
                WHERE u.external_id = c.external_employee_id
            )
        """,
    },
]


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(args=None):
    p = argparse.ArgumentParser(
        description="Update created_by from external_employee_id → storentic.users.id"
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Preview row counts without updating the DB")
    return p.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    dry_run = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    run_ts  = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(
        f"\n  Update created_by"
        f"\n  Mode: {'DRY RUN' if dry_run else 'LIVE'}  |  {run_ts}\n",
        flush=True,
    )

    try:
        from scripts.db import get_engine
        from sqlalchemy import text
        engine = get_engine()
        print("  ✅  Database connection established.\n", flush=True)
    except Exception as exc:
        print(f"\n  ❌  Cannot connect to DB: {exc}\n", flush=True)
        sys.exit(1)

    total_updated = 0

    print(f"  {'Table':<35}  {'Rows':>8}", flush=True)
    print(f"  {'-' * 46}", flush=True)

    for entry in _UPDATES:
        table = entry["table"]
        try:
            if dry_run:
                with engine.connect() as conn:
                    count = conn.execute(text(entry["preview_sql"])).scalar()
                print(f"  {table:<35}  {count:>8,}  (would update)", flush=True)
            else:
                with engine.begin() as conn:
                    result = conn.execute(text(entry["sql"]))
                    count  = result.rowcount
                total_updated += count
                print(f"  {table:<35}  {count:>8,}  ✅", flush=True)
        except Exception as exc:
            print(f"  {table:<35}  ❌  {exc}", flush=True)

    print(f"  {'-' * 46}", flush=True)
    if not dry_run:
        print(f"  {'TOTAL':<35}  {total_updated:>8,}", flush=True)
    print(f"\n  Done.\n", flush=True)


if __name__ == "__main__":
    main()
