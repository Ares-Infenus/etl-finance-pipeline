# src/etl/utils/reporting.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("Reporting")


def _safe_mean(s: pd.Series) -> Optional[float]:
    s2 = s.dropna()
    if s2.empty:
        return None
    return float(s2.mean())


def _to_float_or_none(x: Any) -> Optional[float]:
    return None if x is None else float(x)


def data_quality_report(
    df: pd.DataFrame,
    *,
    compute_indicators: bool = True,
    sma_windows: Iterable[int] = (10, 50),
) -> Dict[str, Any]:
    """
    Produce a data-quality + light indicators report for a normalized DataFrame.

    Report fields:
      - rows, start, end, columns
      - nans_per_column
      - dups_timestamps
      - candles_count
      - mean_range, max_range (if HIGH/LOW exist)
      - close_min/close_max and simple jump heuristic (suspicious_price_jump)
      - indicators: SMA_{w} summary (last/min/max/nans)
    """
    report: Dict = {}
    report["rows"] = int(len(df))
    report["start"] = str(df.index.min()) if len(df) > 0 else None
    report["end"] = str(df.index.max()) if len(df) > 0 else None
    report["columns"] = list(df.columns)

    # NaNs per column
    report["nans_per_column"] = {c: int(df[c].isna().sum()) for c in df.columns}

    # duplicated timestamps
    report["dups_timestamps"] = int(df.index.duplicated().sum())

    # candles count (same as rows but explicit for clarity)
    report["candles_count"] = int(len(df))

    # range metrics (HIGH - LOW)
    if {"HIGH", "LOW"}.issubset(df.columns):
        rng = df["HIGH"] - df["LOW"]
        report["mean_range"] = _safe_mean(rng)
        report["max_range"] = _to_float_or_none(rng.max(skipna=True)) if len(rng) > 0 else None
    else:
        report["mean_range"] = None
        report["max_range"] = None

    # simple CLOSE stats and outlier heuristic
    if "CLOSE" in df.columns:
        closes = df["CLOSE"].dropna()
        if not closes.empty:
            close_min = float(closes.min())
            close_max = float(closes.max())
            median = float(closes.median()) if len(closes) > 0 else None

            report["close_min"] = close_min
            report["close_max"] = close_max
            report["close_median"] = median

            # simple heuristic: ¿hay un salto absurdo? (ej. max >> median)
            suspicious = False
            if median and median != 0:
                ratio_max = close_max / median
                ratio_min = close_min / median
                report["close_max_over_median"] = float(ratio_max)
                report["close_min_over_median"] = float(ratio_min)
                # umbral configurable por estética; 100x es conservador para detectar errores graves
                if ratio_max > 100 or ratio_min < 0.01:
                    suspicious = True
            else:
                report["close_max_over_median"] = None
                report["close_min_over_median"] = None

            report["suspicious_price_jump"] = bool(suspicious)
        else:
            report["close_min"] = report["close_max"] = report["close_median"] = None
            report["close_max_over_median"] = None
            report["close_min_over_median"] = None
            report["suspicious_price_jump"] = False
    else:
        report["close_min"] = report["close_max"] = report["close_median"] = None
        report["close_max_over_median"] = None
        report["close_min_over_median"] = None
        report["suspicious_price_jump"] = False

    # indicators (lightweight): SMA summaries only (no series dumped)
    report["indicators"] = {}
    if compute_indicators and "CLOSE" in df.columns and len(df) > 0:
        close = df["CLOSE"]
        for w in tuple(sma_windows):
            try:
                w_int = int(w)
            except Exception:
                logger.warning("Invalid SMA window: %s. Skipping.", w)
                continue
            sma = close.rolling(window=w_int, min_periods=1).mean()
            sma_non_na = sma.dropna()
            report["indicators"][f"SMA_{w_int}"] = {
                "last": _to_float_or_none(sma_non_na.iloc[-1]) if not sma_non_na.empty else None,
                "nan_count": int(sma.isna().sum()),
                "min": _to_float_or_none(sma_non_na.min()) if not sma_non_na.empty else None,
                "max": _to_float_or_none(sma_non_na.max()) if not sma_non_na.empty else None,
            }

    # small summary notes for humans (optional)
    notes: List[str] = []
    if report["dups_timestamps"] > 0:
        notes.append(f"{report['dups_timestamps']} duplicated timestamps")
    if any(v > 0 for v in report["nans_per_column"].values()):
        notes.append("There are NaNs in some columns")
    if report.get("suspicious_price_jump"):
        notes.append("Suspicious price jump detected (check max/min vs median)")
    report["notes"] = notes

    return report


def save_report(report: Dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    logger.info("Wrote quality report: %s", out_path)
