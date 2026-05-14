import os
import psycopg2
import logging
from rich.logging import RichHandler

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
log = logging.getLogger("mnemosyne-setup")

DATABASE_URL = os.environ.get("DATABASE_URL")

def setup_database():
    if not DATABASE_URL:
        log.error("DATABASE_URL is not set.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()

        log.info("Enabling extensions...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;")
        log.info("Extension vectorscale enabled.")

        # Optional: Setup DiskANN index if collection exists
        # This usually happens after some data is inserted, but we can prepare it
        # Or we can just ensure the extension is there.
        
        cur.close()
        conn.close()
        log.info("Database setup complete.")
    except Exception as e:
        log.error(f"Error setting up database: {e}")

if __name__ == "__main__":
    setup_database()
