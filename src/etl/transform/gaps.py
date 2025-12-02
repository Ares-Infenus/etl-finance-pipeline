# src/etl/transform/gaps.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("GapDetector")


@dataclass
class GapInfo:
    start: pd.Timestamp
    end: pd.Timestamp
    length: pd.Timedelta
    missing_count: int
    classification: str


def _classify_gap(
    length: pd.Timedelta, start: pd.Timestamp, end: pd.Timestamp, short_gap_minutes: int
) -> str:
    minutes = int(length / pd.Timedelta("1min"))
    # short intraday gap
    if minutes <= short_gap_minutes:
        return "short_gap"
    # overnight / session boundary (simple heuristic: crosses calendar day)
    if start.date() != end.date():
        return "overnight_gap"
    # long gap (weekend/holiday)
    if minutes >= 24 * 60:
        return "long_gap"
    return "medium_gap"


def build_expected_index(
    start: pd.Timestamp, end: pd.Timestamp, freq: str, tz: Optional[str]
) -> pd.DatetimeIndex:
    # ensure tz-aware start/end if tz provided
    if tz:
        if start.tzinfo is None:
            start = start.tz_localize(tz)
        if end.tzinfo is None:
            end = end.tz_localize(tz)
    return pd.date_range(start=start, end=end, freq=freq, tz=start.tz)


def detect_gaps(df: pd.DataFrame, rule: str, short_gap_minutes: int = 5) -> List[GapInfo]:
    """
    Detect missing timestamps for the df according to rule (pandas offset alias).
    Returns list of GapInfo objects.
    """
    if not isinstance(df.index, pd.DatetimeIndex) or len(df) == 0:
        return []

    start = df.index.min()
    end = df.index.max()
    expected = pd.date_range(start=start, end=end, freq=rule, tz=start.tz)

    missing = expected.difference(df.index)
    if len(missing) == 0:
        return []

    # group consecutive missing timestamps into gaps
    gaps: List[GapInfo] = []
    if len(missing) == 0:
        return gaps

    # iterate and cluster consecutive missing indices
    cluster_start = missing[0]
    prev = missing[0]
    for ts in missing[1:]:
        if ts - prev > pd.tseries.frequencies.to_offset(rule):
            # end cluster
            gap_start = cluster_start
            gap_end = prev
            length = gap_end - gap_start + pd.tseries.frequencies.to_offset(rule)
            missing_count = int(length / pd.tseries.frequencies.to_offset(rule))
            cls = _classify_gap(
                length,
                gap_start,
                gap_end + pd.tseries.frequencies.to_offset(rule),
                short_gap_minutes,
            )
            gaps.append(
                GapInfo(
                    start=gap_start,
                    end=gap_end,
                    length=length,
                    missing_count=missing_count,
                    classification=cls,
                )
            )
            cluster_start = ts
        prev = ts

    # final cluster
    gap_start = cluster_start
    gap_end = prev
    length = gap_end - gap_start + pd.tseries.frequencies.to_offset(rule)
    missing_count = int(length / pd.tseries.frequencies.to_offset(rule))
    cls = _classify_gap(
        length, gap_start, gap_end + pd.tseries.frequencies.to_offset(rule), short_gap_minutes
    )
    gaps.append(
        GapInfo(
            start=gap_start,
            end=gap_end,
            length=length,
            missing_count=missing_count,
            classification=cls,
        )
    )

    return gaps


def repair_gaps(
    df: pd.DataFrame,
    rule: str,
    *,
    use_ffill_for: Optional[List[str]] = None,
    interpolate_prices: bool = True,
    short_gap_minutes: int = 5,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Reindex df to the expected index and apply repair policies.
    Returns (repaired_df, report)
    report includes: detected_gaps (list), filled_counts, remaining_nans
    """
    report: Dict = {}
    if len(df) == 0 or not isinstance(df.index, pd.DatetimeIndex):
        report["detected_gaps"] = []
        report["filled_counts"] = {}
        report["remaining_nans"] = {}
        return df, report

    tz = df.index.tz
    start = df.index.min()
    end = df.index.max()
    expected = pd.date_range(start=start, end=end, freq=rule, tz=tz)
    gaps = detect_gaps(df, rule, short_gap_minutes=short_gap_minutes)

    # reindex
    reindexed = df.reindex(expected)

    filled_counts: Dict[str, int] = {}

    # apply interpolation on price columns
    price_cols = [c for c in ("OPEN", "HIGH", "LOW", "CLOSE") if c in reindexed.columns]
    if interpolate_prices and price_cols:
        reindexed[price_cols] = reindexed[price_cols].interpolate(
            method="linear", limit_direction="both"
        )

    # apply ffill for specified columns
    if use_ffill_for:
        for c in use_ffill_for:
            if c in reindexed.columns:
                before = int(reindexed[c].isna().sum())
                reindexed[c] = reindexed[c].ffill()
                after = int(reindexed[c].isna().sum())
                filled_counts[c] = before - after

    # compute remaining nans
    remaining_nans = {c: int(reindexed[c].isna().sum()) for c in reindexed.columns}

    # Summary report
    report["detected_gaps"] = [
        {
            "start": str(g.start),
            "end": str((g.end + pd.tseries.frequencies.to_offset(rule))),
            "missing_count": g.missing_count,
            "classification": g.classification,
        }
        for g in gaps
    ]
    report["filled_counts"] = filled_counts
    report["remaining_nans"] = remaining_nans
    report["rows_after"] = int(len(reindexed))

    logger.info("Gap repair finished: %d gaps detected, filled_counts=%s", len(gaps), filled_counts)

    return reindexed, report
