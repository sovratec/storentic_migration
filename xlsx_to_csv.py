"""
xlsx_to_csv.py
==============
Converts every .xlsx file in the input folder to UTF-8 CSV and deletes the
original .xlsx file after a successful conversion.

Multi-sheet workbooks
---------------------
    Single sheet  →  <filename>.csv
    Multiple sheets → <filename>_<SheetName>.csv  (one file per sheet)

Usage
-----
    python xlsx_to_csv.py                          # uses ./input  → ./input
    python xlsx_to_csv.py --input-dir data         # custom input folder
    python xlsx_to_csv.py --output-dir data        # write CSVs to a different folder
    python xlsx_to_csv.py --dry-run                # preview only, no writes/deletes

Arguments
---------
    --input-dir   Folder to scan for .xlsx files  (default: input/)
    --output-dir  Folder to write .csv files into  (default: same as --input-dir)
    --dry-run     Preview what would happen without writing or deleting anything
    --keep-xlsx   Convert but do NOT delete the original .xlsx files
"""

import argparse
import os
import re
import sys
from datetime import datetime

import pandas as pd


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_sheet_name(name: str) -> str:
    """Sanitise a sheet name so it is safe to use in a filename."""
    name = name.strip()
    name = re.sub(r'[\\/:*?"<>|]', "_", name)   # replace illegal filename chars
    name = re.sub(r"\s+", "_", name)              # spaces → underscores
    return name[:50]                               # cap length


def convert_xlsx(xlsx_path: str, output_dir: str, dry_run: bool, keep_xlsx: bool) -> dict:
    """
    Convert one xlsx file to UTF-8 CSV(s).

    Returns a result dict:
        {
          "file":     <xlsx filename>,
          "sheets":   [list of sheet names converted],
          "csvs":     [list of csv paths written],
          "deleted":  True/False,
          "error":    None or exception string,
        }
    """
    result = {
        "file":    os.path.basename(xlsx_path),
        "sheets":  [],
        "csvs":    [],
        "deleted": False,
        "error":   None,
    }

    try:
        xl = pd.ExcelFile(xlsx_path)
    except Exception as exc:
        result["error"] = f"Cannot open: {exc}"
        return result

    sheet_names = xl.sheet_names
    base_name   = os.path.splitext(os.path.basename(xlsx_path))[0]
    multi_sheet = len(sheet_names) > 1

    for sheet in sheet_names:
        try:
            df = xl.parse(sheet, dtype=str)
        except Exception as exc:
            result["error"] = f"Sheet '{sheet}': {exc}"
            continue

        # Build output filename
        if multi_sheet:
            csv_name = f"{base_name}_{_safe_sheet_name(sheet)}.csv"
        else:
            csv_name = f"{base_name}.csv"

        csv_path = os.path.join(output_dir, csv_name)

        if not dry_run:
            df.to_csv(csv_path, index=False, encoding="utf-8")

        result["sheets"].append(sheet)
        result["csvs"].append(csv_path)

    xl.close()

    # Delete xlsx only if all sheets converted without error and not keeping
    if not dry_run and not keep_xlsx and result["error"] is None and result["csvs"]:
        try:
            os.remove(xlsx_path)
            result["deleted"] = True
        except Exception as exc:
            result["error"] = f"Converted OK but could not delete xlsx: {exc}"

    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args(args=None):
    p = argparse.ArgumentParser(
        description="Convert all .xlsx files in a folder to UTF-8 CSV and delete originals"
    )
    p.add_argument(
        "--input-dir", default="input",
        help="Folder containing .xlsx files (default: input/)",
    )
    p.add_argument(
        "--output-dir", default=None,
        help="Folder to write .csv files into (default: same as --input-dir)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would happen — no files written or deleted",
    )
    p.add_argument(
        "--keep-xlsx", action="store_true",
        help="Convert but keep the original .xlsx files (do not delete)",
    )
    return p.parse_args(args)


def main(args=None):
    if args is None:
        args = parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir  = args.input_dir if os.path.isabs(args.input_dir) \
                 else os.path.join(script_dir, args.input_dir)
    output_dir = args.output_dir or input_dir
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(script_dir, output_dir)

    dry_run   = args.dry_run
    keep_xlsx = args.keep_xlsx
    run_ts    = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(
        f"\n  XLSX → CSV Converter"
        f"\n  Mode      : {'DRY RUN' if dry_run else 'LIVE'}"
        f"\n  Input dir : {input_dir}"
        f"\n  Output dir: {output_dir}"
        f"\n  Keep xlsx : {keep_xlsx}"
        f"\n  Run ts    : {run_ts}\n",
        flush=True,
    )

    # Create directories if needed
    if not dry_run:
        os.makedirs(input_dir,  exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
    elif not os.path.isdir(input_dir):
        print(f"  ⚠️   Input folder does not exist yet: {input_dir}\n", flush=True)
        sys.exit(0)

    # Find all xlsx files
    xlsx_files = sorted(
        f for f in os.listdir(input_dir)
        if f.lower().endswith(".xlsx") and not f.startswith("~$")  # skip Excel temp files
    )

    if not xlsx_files:
        print(f"  No .xlsx files found in {input_dir}\n", flush=True)
        sys.exit(0)

    print(f"  Found {len(xlsx_files)} xlsx file(s):\n", flush=True)

    # ── Process each file ─────────────────────────────────────────────────────
    total_converted = 0
    total_deleted   = 0
    total_errors    = 0

    for fname in xlsx_files:
        xlsx_path = os.path.join(input_dir, fname)
        print(f"  📄  {fname}", flush=True)

        result = convert_xlsx(xlsx_path, output_dir, dry_run, keep_xlsx)

        if result["error"] and not result["csvs"]:
            print(f"       ❌  {result['error']}", flush=True)
            total_errors += 1
            continue

        for csv_path in result["csvs"]:
            label = "(would write)" if dry_run else "✅ written"
            print(f"       → {os.path.basename(csv_path)}  [{label}]", flush=True)
            total_converted += 1

        if result["deleted"]:
            print(f"       🗑️   deleted original", flush=True)
            total_deleted += 1
        elif dry_run and not keep_xlsx:
            print(f"       🗑️   (would delete original)", flush=True)
        elif keep_xlsx:
            print(f"       📌  original kept (--keep-xlsx)", flush=True)

        if result["error"]:
            print(f"       ⚠️   {result['error']}", flush=True)
            total_errors += 1

        print(flush=True)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("  " + "=" * 50, flush=True)
    print(f"  xlsx files found   : {len(xlsx_files)}", flush=True)
    print(f"  CSV files written  : {total_converted}", flush=True)
    print(f"  xlsx files deleted : {total_deleted}", flush=True)
    if total_errors:
        print(f"  Errors             : {total_errors}", flush=True)
    print("  " + "=" * 50, flush=True)
    print(f"\n  Done.\n", flush=True)


if __name__ == "__main__":
    main()
