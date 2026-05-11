from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from cleaner import CleanOptions, clean_dataframe, profile_dataframe


st.set_page_config(page_title="AI Data Cleaning Tool", page_icon="CSV", layout="wide")


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


st.title("AI Data Cleaning Tool")
st.caption("Upload a CSV, apply smart cleaning rules, inspect the changes, then download the cleaned file.")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

with st.sidebar:
    st.header("Cleaning Options")
    trim_text = st.checkbox("Trim text whitespace", value=True)
    normalize_headers = st.checkbox("Normalize column names", value=True)
    remove_duplicates = st.checkbox("Remove duplicate rows", value=True)
    infer_dates = st.checkbox("Infer date columns", value=True)
    convert_numeric = st.checkbox("Convert numeric-looking text", value=True)
    missing_strategy = st.selectbox(
        "Missing values",
        ["Leave as-is", "Drop rows with missing values", "Fill with column defaults"],
        index=2,
    )

options = CleanOptions(
    trim_text=trim_text,
    normalize_headers=normalize_headers,
    remove_duplicates=remove_duplicates,
    infer_dates=infer_dates,
    convert_numeric=convert_numeric,
    missing_strategy=missing_strategy,
)

if uploaded_file is None:
    st.info("Choose a CSV file to begin.")
    st.stop()

try:
    original_df = pd.read_csv(uploaded_file)
except Exception as exc:
    st.error(f"Could not read CSV: {exc}")
    st.stop()

cleaned_df, report = clean_dataframe(original_df, options)
profile = profile_dataframe(original_df, cleaned_df)

metric_cols = st.columns(4)
metric_cols[0].metric("Original rows", f"{profile.original_rows:,}")
metric_cols[1].metric("Cleaned rows", f"{profile.cleaned_rows:,}")
metric_cols[2].metric("Original columns", f"{profile.original_columns:,}")
metric_cols[3].metric("Missing cells fixed", f"{profile.missing_cells_fixed:,}")

tab_preview, tab_report, tab_schema = st.tabs(["Preview", "Cleaning Report", "Schema"])

with tab_preview:
    left, right = st.columns(2)
    with left:
        st.subheader("Original")
        st.dataframe(original_df.head(100), use_container_width=True)
    with right:
        st.subheader("Cleaned")
        st.dataframe(cleaned_df.head(100), use_container_width=True)

with tab_report:
    if report:
        for item in report:
            st.write(f"- {item}")
    else:
        st.write("No changes were needed.")

with tab_schema:
    schema_df = pd.DataFrame(
        {
            "column": cleaned_df.columns,
            "dtype": [str(dtype) for dtype in cleaned_df.dtypes],
            "missing_values": cleaned_df.isna().sum().values,
            "unique_values": cleaned_df.nunique(dropna=True).values,
        }
    )
    st.dataframe(schema_df, use_container_width=True)

st.download_button(
    "Download cleaned CSV",
    data=dataframe_to_csv_bytes(cleaned_df),
    file_name="cleaned_data.csv",
    mime="text/csv",
)
