from pathlib import Path

from cleaner import DataProfile
from history import fetch_recent_runs, log_cleaning_run


def test_log_cleaning_run_persists_recent_history(tmp_path: Path):
    db_path = tmp_path / "history.db"
    profile = DataProfile(
        original_rows=5,
        cleaned_rows=4,
        original_columns=3,
        cleaned_columns=2,
        missing_cells_fixed=6,
    )

    log_cleaning_run("sample.csv", "csv", profile, ["Removed duplicates."], db_path=db_path)
    runs = fetch_recent_runs(limit=1, db_path=db_path)

    assert len(runs) == 1
    assert runs[0]["filename"] == "sample.csv"
    assert runs[0]["missing_cells_fixed"] == 6
    assert runs[0]["report"] == ["Removed duplicates."]
