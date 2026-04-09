"""
migrate.py — Unified entry point for Storentic ETL migrations.

Selects the migration type at runtime using the --type flag, then delegates
to the appropriate module (migrate_units or migrate_customers).

Usage:
    # Unit migration
    python migrate.py --type unit --file data/Units.xlsx
    python migrate.py --type unit --file data/Units.xlsx --output excel
    python migrate.py --type unit --file data/Units.xlsx --output db --dry-run
    python migrate.py --type unit --file data/Units.xlsx --output db --update

    # Customer migration
    python migrate.py --type customer --file data/CustomerUnitSample.xlsx
    python migrate.py --type customer --file data/CustomerUnitSample.xlsx --output excel
    python migrate.py --type customer --file data/CustomerUnitSample.xlsx --output db --dry-run
    python migrate.py --type customer --file data/CustomerUnitSample.xlsx --output db --update

    # Customer-Unit migration  (run after 'unit' and 'customer' are complete)
    python migrate.py --type customer_unit --file data/CustomerUnitSample.xlsx
    python migrate.py --type customer_unit --file data/CustomerUnitSample.xlsx --output excel
    python migrate.py --type customer_unit --file data/CustomerUnitSample.xlsx --output db --dry-run
    python migrate.py --type customer_unit --file data/CustomerUnitSample.xlsx --output db --update

Arguments:
    --type      Migration target: 'unit', 'customer', or 'customer_unit' (required)
    --file      Path to the source Excel file (required)
    --output    Destination: 'db' (default) or 'excel'
    --out-file  [excel mode only] Output Excel filename
    --dry-run   [db mode only] Preview without writing to DB
    --update    [db mode only] Update existing records instead of skipping duplicates

Environment (.env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    ORGANIZATION_ID, LOCATION_ID, CREATED_BY
    DRY_RUN, UPDATE_EXISTING
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Storentic ETL Migration — unified entry point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--type",
        required=True,
        choices=["unit", "customer", "customer_unit"],
        help="Migration target: 'unit', 'customer', or 'customer_unit'",
    )
    parser.add_argument("--file",     required=True,  help="Path to source Excel file")
    parser.add_argument("--output",   default="db",   choices=["db", "excel"],
                        help="Output destination: 'db' (default) or 'excel'")
    parser.add_argument("--out-file", default=None,   dest="out_file",
                        help="[excel mode] Output file path")
    parser.add_argument("--dry-run",  action="store_true", dest="dry_run",
                        help="[db mode] Preview without writing to DB")
    parser.add_argument("--update",   action="store_true",
                        help="[db mode] Update existing records instead of skipping")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.type == "unit":
        import migrate_units as handler
    elif args.type == "customer_unit":
        import migrate_customer_unit as handler
    else:
        import migrate_customers as handler

    handler.main(args)


if __name__ == "__main__":
    main()
