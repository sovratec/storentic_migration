"""
db.py — Database connection utility for Storentic migration.
Reads credentials from .env and returns a SQLAlchemy engine.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import urllib.parse

load_dotenv()


def get_engine():
    """Create and return a SQLAlchemy engine from environment variables.

    Uses NullPool so migration scripts never hold idle connections open —
    each engine.connect() opens a fresh connection and closes it on exit.
    This prevents "too many clients" errors on shared dev/staging DB servers.
    """
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "storentic")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    safe_password = urllib.parse.quote_plus(password)
    if not user or not password:
        raise EnvironmentError(
            "DB_USER and DB_PASSWORD must be set in your .env file."
        )

    url = f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{name}"
    engine = create_engine(url, echo=False, poolclass=NullPool)
    return engine


def test_connection():
    """Test the DB connection. Prints success or raises on failure."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        version = result.fetchone()[0]
        print(f"✅  Connection successful!")
        print(f"    PostgreSQL: {version}")
    return engine


if __name__ == "__main__":
    test_connection()
