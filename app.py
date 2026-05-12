from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import streamlit as st

from cleaner import CleanOptions, clean_dataframe, generate_cleaning_recommendations, profile_dataframe
from history import fetch_recent_runs, init_db, log_cleaning_run
from io_utils import get_excel_sheets_from_bytes, read_table_from_upload


st.set_page_config(page_title="AI Data Cleaning Tool", page_icon=":material/table:", layout="wide")
init_db()


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Cleaned Data")
    return buffer.getvalue()


def inject_styles() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(circle at 16% 12%, rgba(31, 120, 104, 0.12), transparent 26rem),
                    linear-gradient(180deg, #f8fbfb 0%, #eef5f2 45%, #f8faf9 100%);
            }

            .block-container {
                padding-top: 1.2rem;
                padding-bottom: 3rem;
            }

            .top-nav {
                align-items: center;
                background: rgba(255,255,255,0.92);
                border: 1px solid rgba(25, 78, 70, 0.12);
                border-radius: 8px;
                display: flex;
                gap: 1rem;
                justify-content: space-between;
                margin-bottom: 1rem;
                padding: 0.8rem 1rem;
                position: sticky;
                top: 0.6rem;
                z-index: 20;
                box-shadow: 0 12px 30px rgba(17, 60, 52, 0.08);
            }

            .brand {
                color: #113c34;
                font-size: 1.05rem;
                font-weight: 800;
            }

            .nav-links {
                align-items: center;
                display: flex;
                flex-wrap: wrap;
                gap: 0.4rem;
                justify-content: flex-end;
            }

            .nav-links a,
            .trial-pill {
                border-radius: 8px;
                color: #174a40;
                font-size: 0.92rem;
                font-weight: 700;
                padding: 0.48rem 0.72rem;
                text-decoration: none;
            }

            .trial-pill {
                background: #dff5ea;
                border: 1px solid #8dd8b1;
                color: #14583c;
            }

            .hero {
                padding: 2.25rem;
                border: 1px solid rgba(25, 78, 70, 0.12);
                border-radius: 8px;
                background: linear-gradient(135deg, #113c34 0%, #1f7868 58%, #e6b450 100%);
                color: #ffffff;
                box-shadow: 0 24px 60px rgba(17, 60, 52, 0.16);
                margin-bottom: 1rem;
            }

            .hero h1 {
                margin: 0;
                font-size: clamp(2.2rem, 5vw, 4.4rem);
                line-height: 1.03;
                letter-spacing: 0;
                max-width: 900px;
            }

            .hero p {
                max-width: 760px;
                margin-top: 1rem;
                color: rgba(255,255,255,0.86);
                font-size: 1.05rem;
            }

            .feature-row {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 0.8rem;
                margin: 1rem 0 1.2rem;
            }

            .feature {
                border: 1px solid rgba(25, 78, 70, 0.11);
                border-radius: 8px;
                background: rgba(255,255,255,0.82);
                padding: 1rem;
            }

            .feature strong {
                display: block;
                color: #123d35;
                margin-bottom: 0.25rem;
            }

            .feature span {
                color: #48645e;
                font-size: 0.94rem;
            }

            .section-heading {
                color: #14583c;
                font-size: 1.45rem;
                font-weight: 800;
                margin: 0 0 0.35rem;
            }

            .section-subtitle {
                color: #48645e;
                margin: 0 0 0.85rem;
            }

            [data-testid="stFileUploader"] label,
            [data-testid="stSelectbox"] label,
            [data-testid="stTextInput"] label,
            [data-testid="stMultiSelect"] label {
                color: #14583c;
                font-weight: 700;
            }

            .account-panel {
                background: rgba(255,255,255,0.9);
                border: 1px solid rgba(25, 78, 70, 0.12);
                border-radius: 8px;
                padding: 1rem;
            }

            .plan-card {
                background: rgba(255,255,255,0.9);
                border: 1px solid rgba(25, 78, 70, 0.12);
                border-radius: 8px;
                min-height: 190px;
                padding: 1rem;
            }

            .plan-card strong {
                color: #113c34;
                display: block;
                font-size: 1.08rem;
                margin-bottom: 0.25rem;
            }

            .plan-card .price {
                color: #14583c;
                font-size: 1.7rem;
                font-weight: 900;
                margin: 0.35rem 0;
            }

            .recommendation {
                border-left: 4px solid #1f7868;
                background: rgba(255,255,255,0.9);
                border-radius: 8px;
                padding: 0.9rem 1rem;
                margin-bottom: 0.7rem;
                box-shadow: 0 10px 28px rgba(17, 60, 52, 0.08);
            }

            .recommendation small {
                color: #7b5b12;
                font-weight: 700;
                text-transform: uppercase;
            }

            .recommendation h4 {
                color: #153f37;
                margin: 0.15rem 0 0.25rem;
                font-size: 1rem;
            }

            .recommendation p {
                color: #506761;
                margin: 0;
                font-size: 0.94rem;
            }

            [data-testid="stMetric"] {
                background: rgba(255,255,255,0.88);
                border: 1px solid rgba(25, 78, 70, 0.11);
                border-radius: 8px;
                padding: 1rem;
            }

            @media (max-width: 800px) {
                .feature-row {
                    grid-template-columns: 1fr;
                }

                .top-nav {
                    align-items: flex-start;
                    flex-direction: column;
                    position: static;
                }

                .hero {
                    padding: 1.4rem;
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_styles()

st.markdown(
    """
    <nav class="top-nav">
        <div class="brand">CleanWise Data Studio</div>
        <div class="nav-links">
            <a href="#cleaner">Cleaner</a>
            <a href="#account">Account</a>
            <a href="#pricing">Pricing</a>
            <a href="#billing">Billing</a>
            <span class="trial-pill">Free trial first</span>
        </div>
    </nav>
    <section class="hero">
        <h1>Clean messy data before it slows you down.</h1>
        <p>Upload a CSV or Excel file, get smart cleaning recommendations, adjust each column, preview the result, and download a cleaner dataset in minutes.</p>
    </section>
    <div class="feature-row">
        <div class="feature"><strong>Guided recommendations</strong><span>Spot duplicates, missing values, type issues, and whitespace before cleaning.</span></div>
        <div class="feature"><strong>Column-level control</strong><span>Choose exactly which columns to clean, drop, convert, or leave alone.</span></div>
        <div class="feature"><strong>Download-ready output</strong><span>Export cleaned data as CSV or Excel, with a local history of each run.</span></div>
    </div>
    """,
    unsafe_allow_html=True,
)

cleaner_tab, account_tab, pricing_tab, billing_tab = st.tabs(["Cleaner", "Account", "Pricing", "Billing"])

with account_tab:
    st.markdown('<div id="account" class="section-heading">Create Your Account</div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="section-subtitle">Save your cleaning history, trial status, and billing preferences.</p>',
        unsafe_allow_html=True,
    )
    account_cols = st.columns([1, 1], gap="large")
    with account_cols[0]:
        with st.form("account_form"):
            full_name = st.text_input("Full name")
            email = st.text_input("Email address")
            company = st.text_input("Company or project name")
            submitted = st.form_submit_button("Save Account")
            if submitted:
                st.session_state["account"] = {
                    "full_name": full_name,
                    "email": email,
                    "company": company,
                    "plan": st.session_state.get("selected_plan", "Free Trial"),
                }
                st.success("Account details saved for this session.")
    with account_cols[1]:
        account = st.session_state.get("account")
        if account:
            st.markdown(
                f"""
                <div class="account-panel">
                    <strong>{account.get("full_name") or "Account"}</strong><br>
                    {account.get("email") or "No email added"}<br>
                    {account.get("company") or "No company added"}<br><br>
                    Current plan: <strong>{account.get("plan", "Free Trial")}</strong>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.info("Add account details to personalize the app experience.")

with pricing_tab:
    st.markdown('<div id="pricing" class="section-heading">Plans Built Around Your Data Workflow</div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="section-subtitle">Start with a free trial, then upgrade when you need more history, exports, or team features.</p>',
        unsafe_allow_html=True,
    )
    plan_cols = st.columns(3)
    plans = [
        ("Free Trial", "GBP 0", "Try cleaning, recommendations, and downloads on sample workflows."),
        ("Pro", "GBP 12/mo", "More saved history, larger files, Excel exports, and priority recommendations."),
        ("Team", "GBP 39/mo", "Shared cleaning history, team seats, billing controls, and audit-friendly logs."),
    ]
    for column, (name, price, detail) in zip(plan_cols, plans, strict=False):
        with column:
            st.markdown(
                f"""
                <div class="plan-card">
                    <strong>{name}</strong>
                    <div class="price">{price}</div>
                    <p>{detail}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button(f"Choose {name}", key=f"plan_{name}", use_container_width=True):
                st.session_state["selected_plan"] = name
                if "account" in st.session_state:
                    st.session_state["account"]["plan"] = name
                st.success(f"{name} selected.")

with billing_tab:
    st.markdown('<div id="billing" class="section-heading">Payment Methods</div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="section-subtitle">Payments should be connected to a secure provider such as Stripe before production. This form stores no card details.</p>',
        unsafe_allow_html=True,
    )
    billing_cols = st.columns([1, 1], gap="large")
    with billing_cols[0]:
        selected_plan = st.selectbox(
            "Plan",
            ["Free Trial", "Pro", "Team"],
            index=["Free Trial", "Pro", "Team"].index(st.session_state.get("selected_plan", "Free Trial")),
        )
        provider = st.selectbox("Preferred payment provider", ["Stripe", "PayPal", "Bank transfer"])
        billing_email = st.text_input("Billing email")
        if st.button("Save Billing Preference"):
            st.session_state["selected_plan"] = selected_plan
            st.session_state["billing"] = {"provider": provider, "billing_email": billing_email}
            st.success("Billing preference saved for this session.")
    with billing_cols[1]:
        st.info("Production payment processing should use hosted checkout pages and webhooks so sensitive card data never touches this app.")

with cleaner_tab:
    st.markdown('<div id="cleaner" class="section-heading">Upload Your Dataset</div>', unsafe_allow_html=True)
    st.markdown('<p class="section-subtitle">CSV or Excel file</p>', unsafe_allow_html=True)

left_panel, right_panel = st.columns([1.05, 0.95], gap="large")

with left_panel:
    uploaded_file = st.file_uploader("CSV or Excel file", type=["csv", "xlsx", "xls"])

with right_panel:
    st.markdown('<div class="section-heading">Recent Cleaning History</div>', unsafe_allow_html=True)
    history_runs = fetch_recent_runs(limit=5)
    if history_runs:
        history_df = pd.DataFrame(
            [
                {
                    "file": run["filename"],
                    "rows": f'{run["original_rows"]:,} -> {run["cleaned_rows"]:,}',
                    "missing fixed": run["missing_cells_fixed"],
                    "created": str(run["created_at"]).replace("T", " "),
                }
                for run in history_runs
            ]
        )
        st.dataframe(history_df, use_container_width=True, hide_index=True)
    else:
        st.info("Your cleaning history will appear here after you save a run.")

if uploaded_file is None:
    st.stop()

sheet_name = None
if Path(uploaded_file.name).suffix.lower() in {".xlsx", ".xls"}:
    try:
        sheet_names = get_excel_sheets_from_bytes(uploaded_file.getvalue())
    except Exception as exc:
        st.error(f"Could not inspect Excel workbook: {exc}")
        st.stop()
    sheet_name = st.selectbox("Choose Excel sheet", sheet_names)

try:
    read_result = read_table_from_upload(uploaded_file.name, uploaded_file.getvalue(), sheet_name)
    original_df = read_result.dataframe
    for note in read_result.notes:
        st.warning(note)
except Exception as exc:
    st.error(f"Could not read file: {exc}")
    st.info("Try opening the CSV in Excel or Google Sheets and exporting it again as UTF-8 CSV if the file is heavily malformed.")
    st.stop()

if original_df.empty:
    st.warning("This file loaded successfully, but it does not contain any rows.")
    st.stop()

recommendations = generate_cleaning_recommendations(original_df)

st.divider()

control_panel, recommendation_panel = st.columns([1, 1], gap="large")

with control_panel:
    st.markdown('<div class="section-heading">Cleaning Controls</div>', unsafe_allow_html=True)
    all_columns = list(original_df.columns)
    selected_columns = st.multiselect(
        "Columns to clean",
        all_columns,
        default=all_columns,
        help="Columns left out here will stay available but will not receive type conversion or text trimming.",
    )
    drop_columns = st.multiselect("Columns to drop", all_columns)

    basic_cols = st.columns(2)
    trim_text = basic_cols[0].toggle("Trim text whitespace", value=True)
    normalize_headers = basic_cols[1].toggle("Normalize headers", value=True)
    remove_duplicates = basic_cols[0].toggle("Remove duplicate rows", value=True)
    infer_dates = basic_cols[1].toggle("Infer date columns", value=True)
    convert_numeric = basic_cols[0].toggle("Convert numeric text", value=True)

    missing_strategy = st.selectbox(
        "Default missing-value strategy",
        ["Leave as-is", "Drop rows with missing values", "Fill with column defaults"],
        index=2,
    )

    column_missing_strategies: dict[str, str] = {}
    with st.expander("Column missing-value overrides"):
        missing_columns = [column for column in all_columns if original_df[column].isna().any()]
        if missing_columns:
            for column in missing_columns:
                column_missing_strategies[column] = st.selectbox(
                    str(column),
                    ["Auto", "Leave missing", "Fill with zero", "Fill with Unknown"],
                    key=f"missing_strategy_{column}",
                )
        else:
            st.write("No missing values detected.")

with recommendation_panel:
    st.markdown('<div class="section-heading">AI Cleaning Recommendations</div>', unsafe_allow_html=True)
    st.caption("Generated from a local data quality scan. No data leaves your machine.")
    for recommendation in recommendations[:8]:
        st.markdown(
            f"""
            <div class="recommendation">
                <small>{recommendation.priority} priority</small>
                <h4>{recommendation.title}</h4>
                <p>{recommendation.detail}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

options = CleanOptions(
    trim_text=trim_text,
    normalize_headers=normalize_headers,
    remove_duplicates=remove_duplicates,
    infer_dates=infer_dates,
    convert_numeric=convert_numeric,
    missing_strategy=missing_strategy,
    columns_to_clean=selected_columns,
    drop_columns=drop_columns,
    column_missing_strategies=column_missing_strategies,
)

cleaned_df, report = clean_dataframe(original_df, options)
profile = profile_dataframe(original_df, cleaned_df)

metric_cols = st.columns(5)
metric_cols[0].metric("Original rows", f"{profile.original_rows:,}")
metric_cols[1].metric("Cleaned rows", f"{profile.cleaned_rows:,}")
metric_cols[2].metric("Columns", f"{profile.original_columns:,} -> {profile.cleaned_columns:,}")
metric_cols[3].metric("Missing fixed", f"{profile.missing_cells_fixed:,}")
metric_cols[4].metric("Recommendations", f"{len(recommendations):,}")

tab_preview, tab_report, tab_schema, tab_download = st.tabs(
    ["Preview", "Cleaning Report", "Schema", "Download"]
)

with tab_preview:
    original_tab, cleaned_tab = st.columns(2)
    with original_tab:
        st.markdown('<div class="section-heading">Original</div>', unsafe_allow_html=True)
        st.dataframe(original_df.head(150), use_container_width=True, height=420)
    with cleaned_tab:
        st.markdown('<div class="section-heading">Cleaned</div>', unsafe_allow_html=True)
        st.dataframe(cleaned_df.head(150), use_container_width=True, height=420)

with tab_report:
    if report:
        for item in report:
            st.write(f"- {item}")
    else:
        st.write("No changes were needed with the current settings.")

with tab_schema:
    schema_df = pd.DataFrame(
        {
            "column": cleaned_df.columns,
            "dtype": [str(dtype) for dtype in cleaned_df.dtypes],
            "missing_values": cleaned_df.isna().sum().values,
            "unique_values": cleaned_df.nunique(dropna=True).values,
        }
    )
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

with tab_download:
    st.markdown('<div class="section-heading">Export Cleaned Data</div>', unsafe_allow_html=True)
    download_cols = st.columns(3)
    download_cols[0].download_button(
        "Download CSV",
        data=dataframe_to_csv_bytes(cleaned_df),
        file_name="cleaned_data.csv",
        mime="text/csv",
        use_container_width=True,
    )
    download_cols[1].download_button(
        "Download Excel",
        data=dataframe_to_excel_bytes(cleaned_df),
        file_name="cleaned_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    if download_cols[2].button("Save Run to History", use_container_width=True):
        log_cleaning_run(
            filename=uploaded_file.name,
            file_type=Path(uploaded_file.name).suffix.lower().lstrip("."),
            profile=profile,
            report=report,
        )
        st.success("Saved this cleaning run to history.")
