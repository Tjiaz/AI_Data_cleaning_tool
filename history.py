from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from cleaner import DataProfile


DB_PATH = Path("cleaning_history.db")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: Path = DB_PATH) -> None:
    with get_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cleaning_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                filename TEXT NOT NULL,
                file_type TEXT NOT NULL,
                original_rows INTEGER NOT NULL,
                cleaned_rows INTEGER NOT NULL,
                original_columns INTEGER NOT NULL,
                cleaned_columns INTEGER NOT NULL,
                missing_cells_fixed INTEGER NOT NULL,
                report_json TEXT NOT NULL
            )
            """
        )


def log_cleaning_run(
    filename: str,
    file_type: str,
    profile: DataProfile,
    report: list[str],
    db_path: Path = DB_PATH,
) -> None:
    init_db(db_path)
    profile_data = asdict(profile)

    with get_connection(db_path) as connection:
        connection.execute(
            """
            INSERT INTO cleaning_runs (
                created_at,
                filename,
                file_type,
                original_rows,
                cleaned_rows,
                original_columns,
                cleaned_columns,
                missing_cells_fixed,
                report_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                filename,
                file_type,
                profile_data["original_rows"],
                profile_data["cleaned_rows"],
                profile_data["original_columns"],
                profile_data["cleaned_columns"],
                profile_data["missing_cells_fixed"],
                json.dumps(report),
            ),
        )


def fetch_recent_runs(limit: int = 10, db_path: Path = DB_PATH) -> list[dict[str, object]]:
    init_db(db_path)

    with get_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                created_at,
                filename,
                file_type,
                original_rows,
                cleaned_rows,
                original_columns,
                cleaned_columns,
                missing_cells_fixed,
                report_json
            FROM cleaning_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    runs: list[dict[str, object]] = []
    for row in rows:
        run = dict(row)
        run["report"] = json.loads(str(run.pop("report_json")))
        runs.append(run)
    return runs
