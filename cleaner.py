from __future__ import annotations

from dataclasses import dataclass
import re

import pandas as pd


@dataclass(frozen=True)
class CleanOptions:
    trim_text: bool = True
    normalize_headers: bool = True
    remove_duplicates: bool = True
    infer_dates: bool = True
    convert_numeric: bool = True
    missing_strategy: str = "Fill with column defaults"


@dataclass(frozen=True)
class DataProfile:
    original_rows: int
    cleaned_rows: int
    original_columns: int
    cleaned_columns: int
    missing_cells_fixed: int


def normalize_column_name(name: object) -> str:
    normalized = str(name).strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "column"


def make_unique_columns(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique_columns = []

    for column in columns:
        count = seen.get(column, 0)
        seen[column] = count + 1
        unique_columns.append(column if count == 0 else f"{column}_{count + 1}")

    return unique_columns


def clean_dataframe(df: pd.DataFrame, options: CleanOptions) -> tuple[pd.DataFrame, list[str]]:
    cleaned = df.copy()
    report: list[str] = []

    if options.normalize_headers:
        original_columns = list(cleaned.columns)
        cleaned.columns = make_unique_columns([normalize_column_name(column) for column in cleaned.columns])
        if original_columns != list(cleaned.columns):
            report.append("Normalized column names to lowercase snake_case.")

    text_columns = cleaned.select_dtypes(include=["object", "string"]).columns

    if options.trim_text and len(text_columns) > 0:
        for column in text_columns:
            cleaned[column] = cleaned[column].map(lambda value: value.strip() if isinstance(value, str) else value)
        report.append("Trimmed leading and trailing whitespace from text cells.")

    if options.convert_numeric:
        converted_columns = []
        for column in cleaned.select_dtypes(include=["object", "string"]).columns:
            converted = pd.to_numeric(cleaned[column], errors="coerce")
            non_empty = cleaned[column].notna()
            conversion_rate = converted[non_empty].notna().mean() if non_empty.any() else 0
            if conversion_rate >= 0.9:
                cleaned[column] = converted
                converted_columns.append(column)
        if converted_columns:
            report.append(f"Converted numeric-looking text columns: {', '.join(converted_columns)}.")

    if options.infer_dates:
        converted_columns = []
        for column in cleaned.select_dtypes(include=["object", "string"]).columns:
            converted = pd.to_datetime(cleaned[column], errors="coerce", format="mixed")
            non_empty = cleaned[column].notna()
            conversion_rate = converted[non_empty].notna().mean() if non_empty.any() else 0
            if conversion_rate >= 0.9:
                cleaned[column] = converted.dt.date
                converted_columns.append(column)
        if converted_columns:
            report.append(f"Inferred date columns: {', '.join(converted_columns)}.")

    if options.remove_duplicates:
        before = len(cleaned)
        cleaned = cleaned.drop_duplicates()
        removed = before - len(cleaned)
        if removed:
            report.append(f"Removed {removed:,} duplicate row(s).")

    missing_before = int(cleaned.isna().sum().sum())
    if options.missing_strategy == "Drop rows with missing values":
        before = len(cleaned)
        cleaned = cleaned.dropna()
        removed = before - len(cleaned)
        if removed:
            report.append(f"Dropped {removed:,} row(s) containing missing values.")
    elif options.missing_strategy == "Fill with column defaults":
        cleaned = fill_missing_values(cleaned)
        missing_after = int(cleaned.isna().sum().sum())
        filled = missing_before - missing_after
        if filled:
            report.append(f"Filled {filled:,} missing value(s) with sensible column defaults.")

    return cleaned, report


def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    filled = df.copy()

    for column in filled.columns:
        series = filled[column]
        if not series.isna().any():
            continue

        if pd.api.types.is_numeric_dtype(series):
            value = series.median()
            filled[column] = series.fillna(0 if pd.isna(value) else value)
        elif pd.api.types.is_bool_dtype(series):
            mode = series.mode(dropna=True)
            filled[column] = series.fillna(False if mode.empty else mode.iloc[0])
        else:
            mode = series.mode(dropna=True)
            filled[column] = series.fillna("Unknown" if mode.empty else mode.iloc[0])

    return filled


def profile_dataframe(original: pd.DataFrame, cleaned: pd.DataFrame) -> DataProfile:
    original_missing = int(original.isna().sum().sum())
    cleaned_missing = int(cleaned.isna().sum().sum())

    return DataProfile(
        original_rows=len(original),
        cleaned_rows=len(cleaned),
        original_columns=len(original.columns),
        cleaned_columns=len(cleaned.columns),
        missing_cells_fixed=max(original_missing - cleaned_missing, 0),
    )
