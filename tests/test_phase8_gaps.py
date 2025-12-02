from pathlib import Path

import pandas as pd

from src.etl.transform.gaps import detect_gaps, repair_gaps


def _make_sample_with_missing() -> pd.DataFrame:
    # build a 1-minute index but drop some timestamps
    idx = pd.date_range("2024-01-01T00:00:00", periods=10, freq="T", tz="UTC")
    df = pd.DataFrame({"OPEN": range(10), "CLOSE": range(10)}, index=idx)
    # drop minute 3 and 4 (simulate short gap)
    df = df.drop(df.index[3:5])
    return df


def test_detect_gaps_simple() -> None:
    df = _make_sample_with_missing()
    gaps = detect_gaps(df, rule="1T", short_gap_minutes=5)
    assert len(gaps) >= 1
    g = gaps[0]
    assert g.missing_count >= 2
    assert g.classification in ("short_gap", "medium_gap", "overnight_gap", "long_gap")


def test_repair_gaps_ffill_and_interpolate(tmp_path: Path) -> None:
    df = _make_sample_with_missing()
    repaired, report = repair_gaps(
        df, rule="1T", use_ffill_for=["CLOSE"], interpolate_prices=True, short_gap_minutes=5
    )
    # After repair, index length equals expected
    start = df.index.min()
    end = df.index.max()
    expected = pd.date_range(start=start, end=end, freq="1T", tz=start.tz)
    assert len(repaired) == len(expected)
    # CLOSE should have had fills (ffill)
    assert report["filled_counts"].get("CLOSE", 0) >= 1
    # OPEN/CLOSE interpolation should have eliminated NaNs for price columns if possible
    remaining = report["remaining_nans"]
    assert remaining.get("OPEN", 0) == 0 or remaining.get("CLOSE", 0) == 0
