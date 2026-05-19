from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest

from accounts import authenticate_account, can_use_cleaner, create_account, trial_days_remaining, update_account_plan


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


def test_free_trial_is_active_for_first_two_days(tmp_path: Path):
    db_path = tmp_path / "accounts.db"
    account = create_account("Ada Lovelace", "ada@example.com", "", "securepass123", db_path=db_path)
    now = datetime.fromisoformat(account.trial_started_at) + timedelta(days=1)

    assert can_use_cleaner(account, now=now)
    assert trial_days_remaining(account, now=now) == 1


def test_free_trial_expires_after_two_days(tmp_path: Path):
    db_path = tmp_path / "accounts.db"
    account = create_account("Ada Lovelace", "ada@example.com", "", "securepass123", db_path=db_path)
    now = datetime.fromisoformat(account.trial_started_at) + timedelta(days=2, minutes=1)

    assert not can_use_cleaner(account, now=now)
    assert trial_days_remaining(account, now=now) == 0


def test_paid_plan_keeps_cleaner_access_after_trial_expires(tmp_path: Path):
    db_path = tmp_path / "accounts.db"
    account = create_account("Ada Lovelace", "ada@example.com", "", "securepass123", db_path=db_path)
    updated = update_account_plan(account.email, "Pro", db_path=db_path)
    assert updated is not None
    now = datetime.now(timezone.utc) + timedelta(days=30)

    assert can_use_cleaner(updated, now=now)
