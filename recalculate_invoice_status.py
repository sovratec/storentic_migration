"""
recalculate_invoice_status.py
==============================
Recalculates and updates status, amount_paid_in_cents, and balance_in_cents
on storentic.invoices based on actual payment totals in storentic.payments.

Status rules
------------
    sum(payments.amount_in_cents) >= invoices.total_in_cents  →  PAID
    0 < sum(payments.amount_in_cents) < invoices.total_in_cents  →  PARTIAL_PAID
    no payments AND due_date > today  →  OVERDUE
    no payments AND due_date <= today  →  UNPAID

Usage
-----
    python recalculate_invoice_status.py              # live update
    python recalculate_invoice_status.py --dry-run    # print counts only

Environment (.env)
------------------
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import os
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
from sqlalchemy import text as sa_text

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()


# ── SQL ────────────────────────────────────────────────────────────────────────

# Single-pass UPDATE: LEFT JOIN payments → invoices so every invoice is covered,
# including those with no payments at all.
_UPDATE_SQL = """
WITH payment_totals AS (
    SELECT
        invoice_id,
        COALESCE(SUM(amount_in_cents), 0) AS total_paid
    FROM storentic.payments
    WHERE invoice_id IS NOT NULL
    GROUP BY invoice_id
)
UPDATE storentic.invoices i
SET
    status = CASE
        WHEN COALESCE(pt.total_paid, 0) >= i.total_in_cents
             AND i.total_in_cents > 0             THEN 'PAID'
        WHEN COALESCE(pt.total_paid, 0) > 0       THEN 'PARTIAL_PAID'
        WHEN i.due_date > CURRENT_DATE            THEN 'UNPAID'
        ELSE                                           'OVERDUE'
    END,
    amount_paid_in_cents = COALESCE(pt.total_paid, 0),
    balance_in_cents     = GREATEST(i.total_in_cents - COALESCE(pt.total_paid, 0), 0),
    updated_at           = NOW()
FROM (
    SELECT
        i2.id,
        COALESCE(pt.total_paid, 0) AS total_paid
    FROM storentic.invoices i2
    LEFT JOIN payment_totals pt ON pt.invoice_id = i2.id
) AS pt
WHERE i.id = pt.id
"""

# Preview query — shows how many invoices fall into each status bucket
_PREVIEW_SQL = """
WITH payment_totals AS (
    SELECT
        invoice_id,
        COALESCE(SUM(amount_in_cents), 0) AS total_paid
    FROM storentic.payments
    WHERE invoice_id IS NOT NULL
    GROUP BY invoice_id
),
classified AS (
    SELECT
        CASE
            WHEN COALESCE(pt.total_paid, 0) >= i.total_in_cents
                 AND i.total_in_cents > 0             THEN 'PAID'
            WHEN COALESCE(pt.total_paid, 0) > 0       THEN 'PARTIAL_PAID'
            WHEN i.due_date > CURRENT_DATE            THEN 'OVERDUE'
            ELSE                                           'UNPAID'
        END AS new_status
    FROM storentic.invoices i
    LEFT JOIN payment_totals pt ON pt.invoice_id = i.id
)
SELECT new_status, COUNT(*) AS count
FROM classified
GROUP BY new_status
ORDER BY new_status
"""


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(args=None):
    p = argparse.ArgumentParser(
        description="Recalculate storentic.invoices status from payments table"
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Preview status counts without updating the DB")
    return p.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    dry_run = args.dry_run or os.getenv("DRY_RUN", "false").lower() == "true"
    run_ts  = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(
        f"\n  Recalculate Invoice Status"
        f"\n  Mode: {'DRY RUN' if dry_run else 'LIVE'}  |  {run_ts}\n",
        flush=True,
    )

    # DB connection
    try:
        from scripts.db import get_engine
        engine = get_engine()
        print("  ✅  Database connection established.\n", flush=True)
    except Exception as exc:
        print(f"\n  ❌  Cannot connect to DB: {exc}\n", flush=True)
        sys.exit(1)

    if dry_run:
        # Show what the status distribution would look like
        print("  [DRY RUN] Projected status breakdown:\n", flush=True)
        with engine.connect() as conn:
            rows = conn.execute(sa_text(_PREVIEW_SQL)).fetchall()
        total = sum(r.count for r in rows)
        print(f"  {'Status':<20}  {'Count':>8}  {'%':>6}", flush=True)
        print(f"  {'-'*38}", flush=True)
        for row in rows:
            pct = row.count / total * 100 if total else 0
            print(f"  {row.new_status:<20}  {row.count:>8,}  {pct:>5.1f}%", flush=True)
        print(f"  {'-'*38}", flush=True)
        print(f"  {'TOTAL':<20}  {total:>8,}", flush=True)
    else:
        print("  Updating invoice statuses ...", flush=True)
        with engine.begin() as conn:
            result = conn.execute(sa_text(_UPDATE_SQL))
            updated = result.rowcount

        print(f"  ✅  Updated {updated:,} invoices.\n", flush=True)

        # Print breakdown after update
        with engine.connect() as conn:
            rows = conn.execute(sa_text(
                "SELECT status, COUNT(*) AS count "
                "FROM storentic.invoices "
                "GROUP BY status ORDER BY status"
            )).fetchall()

        total = sum(r.count for r in rows)
        print(f"  {'Status':<20}  {'Count':>8}  {'%':>6}", flush=True)
        print(f"  {'-'*38}", flush=True)
        for row in rows:
            pct = row.count / total * 100 if total else 0
            print(f"  {row.new_status if hasattr(row, 'new_status') else row.status:<20}  {row.count:>8,}  {pct:>5.1f}%", flush=True)
        print(f"  {'-'*38}", flush=True)
        print(f"  {'TOTAL':<20}  {total:>8,}\n", flush=True)

    print("  Done.\n", flush=True)


if __name__ == "__main__":
    main()
