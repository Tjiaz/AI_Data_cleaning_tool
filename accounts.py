from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
from pathlib import Path
import secrets
import sqlite3

from history import DB_PATH, get_connection


@dataclass(frozen=True)
class Account:
    id: int
    full_name: str
    email: str
    company: str
    plan: str
    created_at: str


def init_accounts(db_path: Path = DB_PATH) -> None:
    with get_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                full_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                company TEXT NOT NULL,
                plan TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL
            )
            """
        )


def create_account(
    full_name: str,
    email: str,
    company: str,
    password: str,
    plan: str = "Free Trial",
    db_path: Path = DB_PATH,
) -> Account:
    validate_account_input(full_name, email, password)
    init_accounts(db_path)
    normalized_email = email.strip().lower()
    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)

    try:
        with get_connection(db_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO accounts (
                    created_at,
                    full_name,
                    email,
                    company,
                    plan,
                    password_salt,
                    password_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    full_name.strip(),
                    normalized_email,
                    company.strip(),
                    plan,
                    salt,
                    password_hash,
                ),
            )
            account_id = int(cursor.lastrowid)
    except sqlite3.IntegrityError as exc:
        raise ValueError("An account already exists with this email address.") from exc

    account = get_account_by_email(normalized_email, db_path)
    if account is None:
        raise RuntimeError(f"Account {account_id} was created but could not be loaded.")
    return account


def authenticate_account(email: str, password: str, db_path: Path = DB_PATH) -> Account | None:
    init_accounts(db_path)
    normalized_email = email.strip().lower()

    with get_connection(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                id,
                created_at,
                full_name,
                email,
                company,
                plan,
                password_salt,
                password_hash
            FROM accounts
            WHERE email = ?
            """,
            (normalized_email,),
        ).fetchone()

    if row is None:
        return None

    expected_hash = hash_password(password, str(row["password_salt"]))
    if not hmac.compare_digest(expected_hash, str(row["password_hash"])):
        return None

    return account_from_row(row)


def update_account_plan(email: str, plan: str, db_path: Path = DB_PATH) -> Account | None:
    init_accounts(db_path)
    normalized_email = email.strip().lower()

    with get_connection(db_path) as connection:
        connection.execute(
            """
            UPDATE accounts
            SET plan = ?
            WHERE email = ?
            """,
            (plan, normalized_email),
        )

    return get_account_by_email(normalized_email, db_path)


def get_account_by_email(email: str, db_path: Path = DB_PATH) -> Account | None:
    init_accounts(db_path)

    with get_connection(db_path) as connection:
        row = connection.execute(
            """
            SELECT id, created_at, full_name, email, company, plan
            FROM accounts
            WHERE email = ?
            """,
            (email.strip().lower(),),
        ).fetchone()

    return None if row is None else account_from_row(row)


def account_from_row(row: sqlite3.Row) -> Account:
    return Account(
        id=int(row["id"]),
        full_name=str(row["full_name"]),
        email=str(row["email"]),
        company=str(row["company"]),
        plan=str(row["plan"]),
        created_at=str(row["created_at"]),
    )


def hash_password(password: str, salt: str) -> str:
    password_bytes = password.encode("utf-8")
    salt_bytes = salt.encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", password_bytes, salt_bytes, 120_000).hex()


def validate_account_input(full_name: str, email: str, password: str) -> None:
    if not full_name.strip():
        raise ValueError("Full name is required.")
    if "@" not in email or "." not in email:
        raise ValueError("Enter a valid email address.")
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters.")
