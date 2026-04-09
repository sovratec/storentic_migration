"""
customer_transformer.py — Field-level transformation functions for customer ETL.

All customer fields from the legacy Excel are plain strings.
Only two helper functions are needed beyond a simple strip:
  - clean_str()       : normalize blank / NaN values to None
  - derive_full_name(): combine first name + middle initial with a space
"""


def clean_str(value) -> str | None:
    """
    Strip whitespace from a raw Excel cell value.
    Returns None for empty, NaN, or None inputs.
    """
    if value is None:
        return None
    s = str(value).strip()
    return None if s.lower() in ("nan", "none", "") else s


def derive_full_name(fname, mi) -> str | None:
    """
    Combine a first-name and middle-initial cell into a single string.

    Rules:
      - If fname is blank  → return None (critical field missing)
      - If mi is blank     → return fname only
      - Otherwise          → return "fname MI"

    Examples:
      ("John", "A")  → "John A"
      ("John", "")   → "John"
      ("John", None) → "John"
      ("",     "A")  → None
    """
    f = clean_str(fname)
    m = clean_str(mi)
    if not f:
        return None
    return f"{f} {m}" if m else f
