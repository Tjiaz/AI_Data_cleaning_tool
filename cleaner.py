from __future__ import annotations

from dataclasses import dataclass, field
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
    columns_to_clean: list[str] | None = None
    drop_columns: list[str] = field(default_factory=list)
    column_missing_strategies: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DataProfile:
    original_rows: int
    cleaned_rows: int
    original_columns: int
    cleaned_columns: int
    missing_cells_fixed: int


@dataclass(frozen=True)
class CleaningRecommendation:
    title: str
    detail: str
    priority: str = "Medium"


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

    if options.drop_columns:
        existing_drop_columns = [column for column in options.drop_columns if column in cleaned.columns]
        if existing_drop_columns:
            cleaned = cleaned.drop(columns=existing_drop_columns)
            report.append(f"Dropped column(s): {', '.join(existing_drop_columns)}.")

    if options.normalize_headers:
        original_columns = list(cleaned.columns)
        cleaned.columns = make_unique_columns([normalize_column_name(column) for column in cleaned.columns])
        column_name_map = dict(zip(original_columns, cleaned.columns, strict=False))
        if original_columns != list(cleaned.columns):
            report.append("Normalized column names to lowercase snake_case.")
        columns_to_clean = [column_name_map.get(column, column) for column in options.columns_to_clean or []]
        column_missing_strategies = {
            column_name_map.get(column, column): strategy
            for column, strategy in options.column_missing_strategies.items()
        }
    else:
        columns_to_clean = options.columns_to_clean
        column_missing_strategies = options.column_missing_strategies

    target_columns = resolve_target_columns(cleaned, columns_to_clean)

    text_columns = cleaned.select_dtypes(include=["object", "string"]).columns
    text_columns = [column for column in text_columns if column in target_columns]

    if options.trim_text and len(text_columns) > 0:
        for column in text_columns:
            cleaned[column] = cleaned[column].map(lambda value: value.strip() if isinstance(value, str) else value)
        report.append("Trimmed leading and trailing whitespace from text cells.")

    if options.convert_numeric:
        converted_columns = []
        for column in cleaned.select_dtypes(include=["object", "string"]).columns:
            if column not in target_columns:
                continue
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
            if column not in target_columns:
                continue
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
        cleaned = fill_missing_values(cleaned, column_missing_strategies)
        missing_after = int(cleaned.isna().sum().sum())
        filled = missing_before - missing_after
        if filled:
            report.append(f"Filled {filled:,} missing value(s) with sensible column defaults.")

    return cleaned, report


def resolve_target_columns(df: pd.DataFrame, columns_to_clean: list[str] | None) -> set[str]:
    if not columns_to_clean:
        return set(df.columns)
    return {column for column in columns_to_clean if column in df.columns}


def fill_missing_values(df: pd.DataFrame, column_strategies: dict[str, str] | None = None) -> pd.DataFrame:
    filled = df.copy()
    column_strategies = column_strategies or {}

    for column in filled.columns:
        series = filled[column]
        if not series.isna().any():
            continue

        strategy = column_strategies.get(column, "Auto")
        if strategy == "Leave missing":
            continue
        if strategy == "Fill with zero":
            filled[column] = series.fillna(0)
        elif strategy == "Fill with Unknown":
            filled[column] = series.fillna("Unknown")
        elif pd.api.types.is_numeric_dtype(series):
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


def generate_cleaning_recommendations(df: pd.DataFrame) -> list[CleaningRecommendation]:
    recommendations: list[CleaningRecommendation] = []

    duplicate_rows = int(df.duplicated().sum())
    if duplicate_rows:
        recommendations.append(
            CleaningRecommendation(
                "Remove duplicate records",
                f"{duplicate_rows:,} duplicate row(s) were detected. Removing them can prevent double-counting.",
                "High",
            )
        )

    missing_by_column = df.isna().sum()
    for column, missing_count in missing_by_column[missing_by_column > 0].items():
        missing_rate = missing_count / len(df) if len(df) else 0
        priority = "High" if missing_rate >= 0.3 else "Medium"
        recommendations.append(
            CleaningRecommendation(
                f"Review missing values in {column}",
                f"{missing_count:,} cell(s), or {missing_rate:.0%}, are missing in this column.",
                priority,
            )
        )

    for column in df.select_dtypes(include=["object", "string"]).columns:
        series = df[column].dropna().astype(str)
        if series.empty:
            continue

        whitespace_count = int(series.str.match(r"^\s|\s$").sum())
        if whitespace_count:
            recommendations.append(
                CleaningRecommendation(
                    f"Trim text in {column}",
                    f"{whitespace_count:,} value(s) have leading or trailing spaces.",
                    "Medium",
                )
            )

        numeric_values = pd.to_numeric(series, errors="coerce")
        numeric_rate = numeric_values.notna().mean()
        if numeric_rate >= 0.9:
            recommendations.append(
                CleaningRecommendation(
                    f"Convert {column} to numeric",
                    f"{numeric_rate:.0%} of non-empty values look numeric.",
                    "Medium",
                )
            )
            continue

        date_values = pd.to_datetime(series, errors="coerce", format="mixed")
        date_rate = date_values.notna().mean()
        if date_rate >= 0.9:
            recommendations.append(
                CleaningRecommendation(
                    f"Convert {column} to date",
                    f"{date_rate:.0%} of non-empty values look like dates.",
                    "Medium",
                )
            )

    if not recommendations:
        recommendations.append(
            CleaningRecommendation(
                "Your dataset looks tidy",
                "No major duplicate, missing-value, type-conversion, or whitespace issues were detected.",
                "Low",
            )
        )

    return recommendations
