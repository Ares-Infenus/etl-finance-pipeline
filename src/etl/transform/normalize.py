# src/etl/transform/normalize.py
from __future__ import annotations

from typing import Dict, List

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("Normalize")


def normalize_columns(df: pd.DataFrame, columns_map: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Rename columns based on a mapping dictionary.
    Example mapping:
    {
        "OPEN": ["open", "o"],
        "CLOSE": ["close", "c"]
    }
    """
    rename_dict = {}

    for target, variants in columns_map.items():
        for col in df.columns:
            if col.lower() in [v.lower() for v in variants]:
                rename_dict[col] = target

    if rename_dict:
        logger.info(f"Renaming columns: {rename_dict}")

    return df.rename(columns=rename_dict)


def enforce_dtypes(df: pd.DataFrame, required_cols: List[str]) -> pd.DataFrame:
    """Convert columns to proper dtypes."""
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Convert numeric
    numeric_cols = [
        c
        for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "TICKVOL", "SPREAD"]
        if c in df.columns
    ]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    return df


def normalize_datetime(df: pd.DataFrame, source_tz: str | None, target_tz: str) -> pd.DataFrame:
    """Ensure datetime column is index and timezone-aware."""
    datetime_col = None

    for col in df.columns:
        if col.lower() in ["datetime", "timestamp", "time"]:
            datetime_col = col
            break

    if datetime_col is None:
        raise ValueError("No datetime column found.")

    df[datetime_col] = pd.to_datetime(df[datetime_col], errors="coerce")
    df = df.set_index(datetime_col)

    # TZ Handling
    if df.index.tz is None:
        if source_tz is None:
            logger.warning("Timestamp is tz-naive. Assuming UTC as source.")
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_localize(source_tz)

    df.index = df.index.tz_convert(target_tz)

    logger.info(f"Datetime localized to {target_tz}")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicated timestamps."""
    before = len(df)
    df = df[~df.index.duplicated(keep="first")]
    after = len(df)

    removed = before - after
    if removed > 0:
        logger.info(f"Removed {removed} duplicated rows.")

    return df


def normalize_df(
    df: pd.DataFrame,
    columns_map: Dict[str, List[str]],
    required_columns: List[str],
    source_tz: str | None,
    target_tz: str,
) -> pd.DataFrame:
    """
    Complete normalization pipeline:
    - rename columns
    - convert dtypes
    - set & convert timezone
    - remove duplicates
    """

    logger.info("Starting normalization...")

    df = normalize_columns(df, columns_map)
    df = enforce_dtypes(df, required_columns)
    df = normalize_datetime(df, source_tz, target_tz)
    df = remove_duplicates(df)

    df = df.sort_index()

    logger.info("Normalization completed successfully.")

    return df
