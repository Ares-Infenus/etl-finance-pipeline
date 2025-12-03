import pandas as pd

from src.etl.utils.reporting import data_quality_report

idx = pd.date_range("2024-01-01", periods=10, freq="1min", tz="UTC")

df = pd.DataFrame(
    {
        "OPEN": [1, 2, 3, None, 5, 6, 7, 8, 9, 10],
        "HIGH": [2, 3, 4, 5, 6, 7, 8, None, 10, 11],
        "LOW": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "CLOSE": [1.5, 2.5, 3.5, 4.5, 5.5, None, 7.5, 8.5, 9.5, 10.5],
    },
    index=idx,
)

report = data_quality_report(df, compute_indicators=True, sma_windows=(3, 5))

print(report)
