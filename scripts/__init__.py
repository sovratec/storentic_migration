# scripts package


def to_bigint(val) -> int | None:
    """Convert a pandas cell (may be float like 91.0, int, or str) to Python int.

    Returns None for blank / NaN / non-numeric values so psycopg2 inserts NULL
    rather than raising 'invalid input syntax for type bigint: "91.0"'.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "none", "null", "nat"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None
