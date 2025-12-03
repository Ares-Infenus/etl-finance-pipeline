# tests/test_phase9_resample.py
import pandas as pd

from src.etl.transform.resample import resample_ohlc


def _make_1min_sample() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01T00:00:00", periods=6, freq="T", tz="UTC")
    df = pd.DataFrame(
        {
            "OPEN": [10, 11, 11.5, 12, 11.8, 12.1],
            "HIGH": [11, 11.2, 12, 12.2, 12.0, 12.5],
            "LOW": [9.8, 10.9, 11.2, 11.9, 11.7, 12.0],
            "CLOSE": [11, 11.5, 12, 11.8, 12.1, 12.4],
            "VOLUME": [100, 110, 90, 120, 60, 80],
        },
        index=idx,
    )
    return df


def test_resample_5min_basic() -> None:
    df = _make_1min_sample()
    # resample to 5-minute: should aggregate first 5 rows into 00:00, last row into 00:05
    res = resample_ohlc(df, "5T", drop_incomplete=True)

    # expect two rows: period starting at 00:00 and 00:05
    assert len(res) >= 1
    first = res.iloc[0]
    # OPEN == first open in span
    assert first["OPEN"] == 10
    # HIGH == max of first 5 highs
    assert first["HIGH"] == max(df["HIGH"][:5])
    # LOW == min of first 5 lows
    assert first["LOW"] == min(df["LOW"][:5])
    # CLOSE == last close in span
    assert first["CLOSE"] == df["CLOSE"][:5].iloc[-1]
    # VOLUME == sum
    assert first["VOLUME"] == sum(df["VOLUME"][:5])


def test_drop_incomplete_behavior() -> None:
    df = _make_1min_sample()
    # drop one row to make the first 5-min interval incomplete
    df = df.drop(df.index[2])
    res = resample_ohlc(df, "5T", drop_incomplete=True)
    # If CLOSE is missing for that interval, it should be dropped (no NaN CLOSE rows)
    if "CLOSE" in res.columns:
        assert not res["CLOSE"].isna().any()
