import pandas as pd

from io_utils import read_csv_resilient


def test_read_csv_resilient_handles_semicolon_delimited_files():
    result = read_csv_resilient(b"name;score\nAlice;10\nBob;20\n")

    assert result.dataframe.equals(pd.DataFrame({"name": ["Alice", "Bob"], "score": [10, 20]}))
    assert result.notes
