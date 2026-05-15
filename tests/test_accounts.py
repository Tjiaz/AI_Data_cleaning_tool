from pathlib import Path

import pytest

from accounts import authenticate_account, create_account, update_account_plan


def test_create_and_authenticate_account(tmp_path: Path):
    db_path = tmp_path / "accounts.db"

    account = create_account(
        full_name="Ada Lovelace",
        email="ADA@example.com",
        company="Analytical Engines",
        password="securepass123",
        db_path=db_path,
    )
    authenticated = authenticate_account("ada@example.com", "securepass123", db_path=db_path)

    assert account.email == "ada@example.com"
    assert authenticated is not None
    assert authenticated.full_name == "Ada Lovelace"


def test_create_account_rejects_duplicate_email(tmp_path: Path):
    db_path = tmp_path / "accounts.db"
    create_account("Ada Lovelace", "ada@example.com", "", "securepass123", db_path=db_path)

    with pytest.raises(ValueError, match="already exists"):
        create_account("Ada", "ada@example.com", "", "securepass123", db_path=db_path)


def test_authenticate_account_rejects_wrong_password(tmp_path: Path):
    db_path = tmp_path / "accounts.db"
    create_account("Ada Lovelace", "ada@example.com", "", "securepass123", db_path=db_path)

    assert authenticate_account("ada@example.com", "wrongpass123", db_path=db_path) is None


def test_update_account_plan(tmp_path: Path):
    db_path = tmp_path / "accounts.db"
    create_account("Ada Lovelace", "ada@example.com", "", "securepass123", db_path=db_path)

    updated = update_account_plan("ada@example.com", "Pro", db_path=db_path)

    assert updated is not None
    assert updated.plan == "Pro"
