# AI Data Cleaning Tool

A starter Python app for uploading a CSV, applying smart cleaning rules, previewing the results, and downloading a cleaned CSV.

## Why Python?

Python is the strongest choice for this project because:

- `pandas` is excellent for CSV parsing, profiling, and cleaning.
- `streamlit` makes it fast to ship an interactive upload/download UI.
- AI features can be added later with OpenAI or another LLM provider without changing the core data pipeline.

## Features

- Upload CSV files.
- Normalize column names.
- Trim text whitespace.
- Convert numeric-looking text columns.
- Infer date columns.
- Remove duplicate rows.
- Fill or drop missing values.
- Preview original and cleaned data side by side.
- Download the cleaned CSV.

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

## Project Structure

```text
.
├── app.py              # Streamlit UI
├── cleaner.py          # Data cleaning logic
├── requirements.txt    # Python dependencies
└── README.md           # Project notes
```

## Next Ideas

- Add AI-generated cleaning recommendations.
- Add column-level cleaning controls.
- Add Excel upload support.
- Add a cleaning history log.
- Add automated tests for the cleaning functions.
