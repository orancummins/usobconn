#!/usr/bin/env python3
"""Import SQLite data into PostgreSQL for this app.

Usage (inside app container):
    python scripts/import_sqlite_to_postgres.py \
      --sqlite-path /app/instance/monarch.db
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import MetaData, create_engine, func, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import SQLite DB into PostgreSQL")
    parser.add_argument(
        "--sqlite-path",
        default="/app/instance/monarch.db",
        help="Path to source SQLite database file",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Target PostgreSQL DATABASE_URL (defaults to env DATABASE_URL)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per insert batch",
    )
    return parser.parse_args()


def batched(iterable, size: int):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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
            max_id = session.execute(select(func.max(column))).scalar_one_or_none()
            next_value = 1 if max_id is None else int(max_id)
            is_called = max_id is not None
            session.execute(
                text("SELECT setval(:seq_name, :next_value, :is_called)"),
                {
                    "seq_name": sequence_name,
                    "next_value": next_value,
                    "is_called": is_called,
                },
            )


def main() -> int:
    args = parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL is required (pass --database-url or set env var)")

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite file not found: {sqlite_path}")

    src_engine = create_engine(f"sqlite:///{sqlite_path}")
    dst_engine = create_engine(args.database_url)

    src_metadata = MetaData()
    dst_metadata = MetaData()

    src_metadata.reflect(bind=src_engine)
    dst_metadata.reflect(bind=dst_engine)

    src_tables = {t.name: t for t in src_metadata.sorted_tables}
    dst_tables = {t.name: t for t in dst_metadata.sorted_tables}

    table_names = [name for name in src_tables if name in dst_tables]
    if not table_names:
        print("No overlapping tables found between SQLite and PostgreSQL.")
        return 0

    skipped = [name for name in src_tables if name not in dst_tables]
    if skipped:
        print(f"Skipping SQLite-only tables: {', '.join(sorted(skipped))}")

    try:
        with Session(dst_engine) as dst_session, src_engine.connect() as src_conn:
            with dst_session.begin():
                inspector = inspect(dst_engine)
                dst_sorted = [t for t in dst_metadata.sorted_tables if t.name in table_names]

                # Delete child tables first to satisfy foreign keys.
                for table in reversed(dst_sorted):
                    print(f"Clearing table {table.name}...")
                    dst_session.execute(table.delete())

                for name in table_names:
                    src_table = src_tables[name]
                    dst_table = dst_tables[name]
                    src_cols = [c.name for c in src_table.columns]
                    dst_cols = [c.name for c in dst_table.columns]
                    common_cols = [c for c in src_cols if c in dst_cols]

                    print(f"Importing {name} ({len(common_cols)} columns)...")
                    rows = src_conn.execute(select(src_table)).mappings()

                    inserted = 0
                    for batch in batched(rows, args.batch_size):
                        payload = [{col: row[col] for col in common_cols} for row in batch]
                        if payload:
                            dst_session.execute(dst_table.insert(), payload)
                            inserted += len(payload)
                    print(f"Imported {inserted} rows into {name}")

                # Keep serial/identity sequences aligned with imported IDs.
                reset_pg_sequences(dst_session, dst_metadata)

                # Emit simple post-import counts.
                for table in dst_sorted:
                    count = dst_session.execute(
                        select(func.count()).select_from(table)
                    ).scalar_one()
                    print(f"Postgres row count {table.name}: {count}")

                # Basic health check: verify all expected tables exist.
                present = set(inspector.get_table_names())
                missing = [t for t in table_names if t not in present]
                if missing:
                    raise RuntimeError(
                        f"Expected target tables missing after import: {', '.join(missing)}"
                    )

    except SQLAlchemyError as exc:
        raise SystemExit(f"Import failed: {exc}") from exc

    print("SQLite -> PostgreSQL import complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
