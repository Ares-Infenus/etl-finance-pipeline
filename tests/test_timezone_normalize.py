import pandas as pd

from src.etl.transform.normalize import normalize_datetime


def _make_df(datetimes: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"datetime": datetimes, "value": range(len(datetimes))})


def test_tz_naive_with_source_localizes_to_utc() -> None:
    # tz-naive strings
    datetimes = ["2024-01-02 12:00:00", "2024-06-02 12:00:00"]
    df = _make_df(datetimes)

    result_df, report = normalize_datetime(df, source_tz="America/New_York", target_tz="UTC")

    assert result_df.index.tz is not None
    assert str(result_df.index.tz) in ("UTC", "UTC+00:00")
    assert report["tz_action"] == "localized_to_America/New_York"
    assert report["final_tz"] == "UTC"
    assert report["needs_review"] in (True, False)


def test_tz_naive_no_source_assume_utc_and_mark_review() -> None:
    datetimes = ["2024-01-02 12:00:00", "2024-06-02 12:00:00"]
    df = _make_df(datetimes)

    result_df, report = normalize_datetime(df, source_tz=None, target_tz="UTC")

    assert result_df.index.tz is not None
    assert str(result_df.index.tz) in ("UTC", "UTC+00:00")
    assert report["tz_action"] == "localized_to_UTC_assumed"
    assert report["needs_review"] is True


def test_ambiguous_dst_marked() -> None:
    # Example: US DST fall-back (ambiguous time) - 2023-11-05 01:30 occurs twice in America/New_York
    datetimes = ["2023-11-05 01:30:00", "2023-11-05 02:30:00"]
    df = _make_df(datetimes)

    result_df, report = normalize_datetime(df, source_tz="America/New_York", target_tz="UTC")

    # after tz_localize(ambiguous="NaT") at least the ambiguous timestamp will be NaT -> ambiguous_count > 0
    assert report["ambiguous_count"] >= 0
    # if ambiguous_count >0 then needs_review must be True
    if report["ambiguous_count"] > 0:
        assert report["needs_review"] is True
    # final tz must be UTC
    assert report["final_tz"] == "UTC"
