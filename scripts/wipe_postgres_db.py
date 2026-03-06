#!/usr/bin/env python3
"""Wipe all data from the destination PostgreSQL database.

This deletes all rows from all discovered tables in reverse dependency order,
and then resets PostgreSQL sequences.

Usage (inside app container):
    python scripts/wipe_postgres_db.py --yes
"""

from __future__ import annotations

import argparse
import os

from sqlalchemy import MetaData, create_engine, func, select, text
from sqlalchemy.orm import Session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wipe destination PostgreSQL database")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Target PostgreSQL DATABASE_URL (defaults to env DATABASE_URL)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm destructive wipe action",
    )
    return parser.parse_args()


def reset_pg_sequences(session: Session, metadata: MetaData) -> None:
    for table in metadata.sorted_tables:
        for column in table.columns:
            if not column.primary_key:
                continue
            sequence_name = session.execute(
                select(func.pg_get_serial_sequence(table.name, column.name))
            ).scalar_one_or_none()
            if not sequence_name:
                continue
            session.execute(
                text("SELECT setval(:seq_name, 1, false)"),
                {"seq_name": sequence_name},
            )


def main() -> int:
    args = parse_args()

    if not args.yes:
        raise SystemExit("Refusing to wipe DB without --yes")

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required (pass --database-url or set env var)")

    if not args.database_url.startswith("postgresql"):
        raise SystemExit("This wipe script only supports PostgreSQL DATABASE_URL values")

    engine = create_engine(args.database_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if not metadata.sorted_tables:
        print("No tables found to wipe.")
        return 0

    with Session(engine) as session:
        with session.begin():
            for table in reversed(metadata.sorted_tables):
                print(f"Clearing table {table.name}...")
                session.execute(table.delete())

            reset_pg_sequences(session, metadata)

            for table in metadata.sorted_tables:
                count = session.execute(select(func.count()).select_from(table)).scalar_one()
                print(f"Post-wipe row count {table.name}: {count}")

    print("PostgreSQL wipe complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
