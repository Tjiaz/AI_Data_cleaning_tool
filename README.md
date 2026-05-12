# AI Data Cleaning Tool

A polished Streamlit app for uploading CSV or Excel files, getting smart cleaning recommendations, adjusting column-level cleaning rules, previewing results, and downloading clean data.

## Why Python?

Python is the best fit for this project because:

- `pandas` is excellent for CSV, Excel, profiling, and data-cleaning workflows.
- `streamlit` makes it fast to build a clean, interactive web interface.
- `sqlite3` is built into Python and is ideal for a lightweight local cleaning history.
- AI-assisted recommendations can start with local data-quality heuristics and later be upgraded to an LLM provider.

## Features

- Upload CSV, XLSX, or XLS files.
- Retry malformed CSV files with safer parser fallbacks.
- Preview original and cleaned datasets side by side.
- Generate AI-style cleaning recommendations from a local data quality scan.
- Normalize column names.
- Trim text whitespace.
- Convert numeric-looking text columns.
- Infer date columns.
- Remove duplicate rows.
- Drop selected columns.
- Choose exactly which columns should be cleaned.
- Override missing-value handling per column.
- Download cleaned data as CSV or Excel.
- Save a local SQLite cleaning history.
- Use account, pricing, and billing screens for a free-trial-first product flow.
- Run automated tests for the cleaning and history logic.

## Database

The app uses SQLite through `cleaning_history.db` to store cleaning run history. This is appropriate for the current version because it is local, simple, and does not require a hosted database service.

If the app later needs user accounts, shared team history, or cloud deployment at larger scale, PostgreSQL would be a better next step.

## Payments

The current billing screen is a product-ready placeholder. It stores no card details and is safe for local development. For production, connect a hosted checkout provider such as Stripe Checkout so sensitive payment data never touches the Streamlit app.

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

```powershell
streamlit run app.py
```

Then open the local URL shown in the terminal.

On Windows, you can also run:

```powershell
.\run_app.bat
```

## Test

```powershell
pytest
```

## Project Structure

```text
.
|-- app.py              # Streamlit UI
|-- cleaner.py          # Data cleaning and recommendation logic
|-- history.py          # SQLite cleaning history
|-- requirements.txt    # Python dependencies
|-- run_app.bat         # Windows launcher
|-- tests/              # Automated tests
`-- README.md           # Project notes
```
