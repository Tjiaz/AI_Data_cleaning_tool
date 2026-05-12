from __future__ import annotations

from dataclasses import dataclass, field
import io
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class TableReadResult:
    dataframe: pd.DataFrame
    notes: list[str] = field(default_factory=list)


def read_table_from_upload(filename: str, file_bytes: bytes, sheet_name: str | None = None) -> TableReadResult:
    suffix = Path(filename).suffix.lower()

    if suffix == ".csv":
        return read_csv_resilient(file_bytes)
    if suffix in {".xlsx", ".xls"}:
        dataframe = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name or 0)
        return TableReadResult(dataframe=dataframe)
    raise ValueError("Unsupported file type. Please upload CSV, XLSX, or XLS.")


def read_csv_resilient(file_bytes: bytes) -> TableReadResult:
    attempts = [
        (
            {"engine": "c"},
            None,
        ),
        (
            {"engine": "python", "sep": None, "encoding": "utf-8-sig"},
            "Used a safer CSV parser with automatic delimiter detection.",
        ),
        (
            {"engine": "python", "sep": None, "encoding": "latin1"},
            "Read the file with latin1 encoding because utf-8 parsing failed.",
        ),
        (
            {"engine": "python", "sep": None, "encoding": "latin1", "on_bad_lines": "skip"},
            "Skipped malformed rows that could not be parsed safely.",
        ),
    ]
    errors: list[str] = []

    for kwargs, note in attempts:
        try:
            dataframe = pd.read_csv(io.BytesIO(file_bytes), **kwargs)
            if len(dataframe.columns) == 1 and likely_has_non_comma_delimiter(file_bytes) and kwargs["engine"] == "c":
                errors.append("Default comma parser produced one column for a likely delimited file.")
                continue
            notes = [note] if note else []
            if errors and not note:
                notes.append("The file was parsed after retrying with a compatible CSV parser.")
            return TableReadResult(dataframe=dataframe, notes=notes)
        except Exception as exc:
            errors.append(str(exc))

    raise ValueError(f"Could not parse this CSV after multiple attempts. Last error: {errors[-1]}")


def likely_has_non_comma_delimiter(file_bytes: bytes) -> bool:
    sample = file_bytes[:4096].decode("utf-8", errors="ignore")
    first_lines = [line for line in sample.splitlines()[:5] if line.strip()]
    if not first_lines:
        return False

    delimiter_counts = {
        ";": sum(line.count(";") for line in first_lines),
        "\t": sum(line.count("\t") for line in first_lines),
        "|": sum(line.count("|") for line in first_lines),
    }
    comma_count = sum(line.count(",") for line in first_lines)
    return max(delimiter_counts.values(), default=0) > comma_count


def get_excel_sheets_from_bytes(file_bytes: bytes) -> list[str]:
    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
    return excel_file.sheet_names
