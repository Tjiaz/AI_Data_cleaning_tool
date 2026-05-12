import pandas as pd

from cleaner import CleanOptions, clean_dataframe, generate_cleaning_recommendations, normalize_column_name


def test_normalize_column_name_creates_snake_case():
    assert normalize_column_name(" Full Name!! ") == "full_name"


def test_clean_dataframe_removes_duplicates_and_fills_missing_values():
    df = pd.DataFrame(
        {
            " Name ": [" Alice ", " Alice ", None],
            "Age": ["30", "30", None],
        }
    )

    cleaned, report = clean_dataframe(df, CleanOptions())

    assert list(cleaned.columns) == ["name", "age"]
    assert len(cleaned) == 2
    assert cleaned["name"].isna().sum() == 0
    assert cleaned["age"].isna().sum() == 0
    assert "Alice" in cleaned["name"].tolist()
    assert any("Removed 1" in item for item in report)


def test_column_level_controls_skip_unselected_columns_and_drop_columns():
    df = pd.DataFrame(
        {
            "Keep Text": ["  001  ", "  002  "],
            "Convert Me": ["10", "20"],
            "Drop Me": ["x", "y"],
        }
    )

    cleaned, _ = clean_dataframe(
        df,
        CleanOptions(
            normalize_headers=False,
            columns_to_clean=["Convert Me"],
            drop_columns=["Drop Me"],
        ),
    )

    assert "Drop Me" not in cleaned.columns
    assert cleaned["Keep Text"].tolist() == ["  001  ", "  002  "]
    assert pd.api.types.is_numeric_dtype(cleaned["Convert Me"])


def test_column_missing_override_can_leave_values_missing():
    df = pd.DataFrame({"Name": ["Alice", None], "Score": [10, None]})

    cleaned, _ = clean_dataframe(
        df,
        CleanOptions(
            normalize_headers=False,
            column_missing_strategies={"Name": "Leave missing", "Score": "Fill with zero"},
        ),
    )

    assert cleaned["Name"].isna().sum() == 1
    assert cleaned["Score"].tolist() == [10.0, 0.0]


def test_generate_cleaning_recommendations_flags_common_issues():
    df = pd.DataFrame(
        {
            "Amount": ["10", "20", "20"],
            "Customer": [" Alice ", None, None],
        }
    )

    recommendations = generate_cleaning_recommendations(df)
    titles = [recommendation.title for recommendation in recommendations]

    assert "Convert Amount to numeric" in titles
    assert any(title.startswith("Review missing values in Customer") for title in titles)
    assert "Trim text in Customer" in titles
