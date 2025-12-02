from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("Exporter")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def validate_final_df(df: pd.DataFrame) -> Dict:
    """
    Run final validations on a normalized/resampled dataframe before export.
    Returns a report dict. Raises ValueError on fatal issues.
    """
    report: Dict = {}
    # must have a DatetimeIndex tz-aware
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Index must be a DatetimeIndex before export.")
    report["index_dtype"] = str(df.index.dtype)
    tz = df.index.tz
    report["index_tz"] = str(tz)
    if tz is None:
        raise ValueError("Index timezone is not set (expected UTC).")

    # no duplicated timestamps
    dups = int(df.index.duplicated().sum())
    report["duplicated_timestamps"] = dups
    if dups > 0:
        # non-fatal but reported
        logger.warning("DataFrame has %d duplicated timestamps at export time.", dups)

    # OHLC sanity checks if present
    if {"OPEN", "HIGH", "LOW", "CLOSE"}.issubset(df.columns):
        negative_price = False
        if (df[["OPEN", "HIGH", "LOW", "CLOSE"]] < 0).any().any():
            report["negative_prices"] = True
            negative_price = True
        else:
            report["negative_prices"] = False

        # LOW <= min(OPEN,HIGH,CLOSE) & HIGH >= max(...)
        invalid_low = (df["LOW"] > df[["OPEN", "HIGH", "CLOSE"]].min(axis=1)).any()
        invalid_high = (df["HIGH"] < df[["OPEN", "LOW", "CLOSE"]].max(axis=1)).any()
        report["invalid_low"] = bool(invalid_low)
        report["invalid_high"] = bool(invalid_high)

        if negative_price or invalid_low or invalid_high:
            logger.warning(
                "OHLC sanity checks failed: negative_price=%s invalid_low=%s invalid_high=%s",
                report["negative_prices"],
                report["invalid_low"],
                report["invalid_high"],
            )

    # final row count / start / end
    report["rows"] = int(len(df))
    if len(df) > 0:
        report["start"] = str(df.index.min())
        report["end"] = str(df.index.max())
    else:
        report["start"] = None
        report["end"] = None

    return report


def _make_hash_of_df(df: pd.DataFrame, keys: Optional[List[str]] = None) -> str:
    """
    Lightweight content hash: use index min/max and row count and first/last rows of keys.
    Avoid expensive full-data hashing.
    """
    h = hashlib.sha256()
    h.update(str(len(df)).encode())
    if len(df) > 0:
        h.update(str(df.index.min()).encode())
        h.update(str(df.index.max()).encode())
    if keys:
        for k in keys:
            if k in df.columns:
                sample = df[k].dropna().head(3).to_list()
                h.update(str(sample).encode())
    return h.hexdigest()


def _prepare_partition_cols(
    df: pd.DataFrame, partition_cols: Optional[List[str]] = None
) -> List[str]:
    """
    Given desired partition_cols (case-insensitive names), ensure they exist in df (create YEAR/MONTH if requested).
    Returns list of actual column names to pass to pandas.to_parquet partition_cols.
    """
    if not partition_cols:
        return []

    actual: List[str] = []
    lookup = {c.lower(): c for c in df.columns}

    for pc in partition_cols:
        low = pc.lower()
        if low in lookup:
            actual.append(lookup[low])
        else:
            # create YEAR / MONTH from index if requested
            if low == "year":
                if "YEAR" not in df.columns:
                    df["YEAR"] = df.index.year.astype(int)
                actual.append("YEAR")
            elif low == "month":
                if "MONTH" not in df.columns:
                    df["MONTH"] = df.index.month.astype(int)
                actual.append("MONTH")
            else:
                logger.warning(
                    "Requested partition column '%s' not found and not auto-created.", pc
                )
    return actual


def write_parquet_with_metadata(
    df: pd.DataFrame,
    out_path: Path,
    *,
    compression: str = "zstd",
    engine: str = "pyarrow",
    partition_cols: Optional[List[str]] = None,
    metadata: Optional[Dict] = None,
) -> Dict:
    """
    Validate, attach metadata to attrs, write parquet, and return an export report dict.
    """
    _ensure_dir(out_path.parent)

    # Validate
    val_report = validate_final_df(df)

    # Prepare metadata to attach (attrs)
    if metadata is None:
        metadata = {}
    # minimal metadata
    metadata.setdefault("exporter_version", "v1")
    metadata.setdefault("rows", int(len(df)))
    # keep normalization_report if present in attrs
    if "normalization_report" in getattr(df, "attrs", {}):
        metadata["normalization_report"] = df.attrs["normalization_report"]

    # attach attrs
    df = df.copy()
    df.attrs.update(metadata)

    # prepare partition columns (possibly creating YEAR / MONTH)
    # mypy: mapped_partitions can be a list or None
    mapped_partitions: Optional[List[str]] = _prepare_partition_cols(df, partition_cols)
    if mapped_partitions == []:
        mapped_partitions = None

    to_write = df.reset_index()

    # ensure partition columns exist in reset dataframe
    if mapped_partitions:
        mapped_partitions = [c for c in mapped_partitions if c in to_write.columns]
        if not mapped_partitions:
            mapped_partitions = None

    # convert metadata summary to JSON serializable subset and store as metadata file alongside parquet
    export_report: Dict = {
        "path": str(out_path),
        "compression": compression,
        "engine": engine,
        "partition_cols": mapped_partitions,
        "rows": int(len(df)),
    }
    export_report.update(val_report)

    # write parquet
    to_write.to_parquet(
        out_path,
        engine=engine,
        compression=compression,
        index=False,
        partition_cols=mapped_partitions,
    )

    # write sidecar metadata file (json)
    meta_path = out_path.with_name(out_path.name + ".meta.json")
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(export_report, fh, indent=2, ensure_ascii=False)

    # compute light hash
    export_report["content_hash"] = _make_hash_of_df(
        df, keys=["OPEN", "CLOSE"] if "OPEN" in df.columns else None
    )

    logger.info(
        "Wrote parquet: %s (rows=%d, compression=%s, partitions=%s)",
        out_path,
        len(df),
        compression,
        mapped_partitions,
    )

    return export_report


def append_export_log(log_dir: Path, entry: Dict) -> None:
    """
    Append a single JSON line to export_log.ndjson
    """
    _ensure_dir(log_dir)
    log_file = log_dir / "export_log.ndjson"
    with open(log_file, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
