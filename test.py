import pandas as pd

df = pd.read_parquet(
    "data/processed/input_0_20240102_20240102/input_0_20240102_20240102_5m.parquet"
)
print(df.head())
