"""
utils.py — Shared helpers for migration scripts.
"""

import pandas as pd


def normalize_columns(df: pd.DataFrame, required: list) -> pd.DataFrame:
    """
    Rename DataFrame columns case-insensitively to match expected names.

    Example: if required has 'TenantID' and the CSV has 'tenantid',
    the column is renamed to 'TenantID' so all downstream code works unchanged.
    """
    lower_to_actual = {c.lower(): c for c in df.columns}
    rename_map = {}
    for req in required:
        match = lower_to_actual.get(req.lower())
        if match and match != req:
            rename_map[match] = req
    return df.rename(columns=rename_map)
