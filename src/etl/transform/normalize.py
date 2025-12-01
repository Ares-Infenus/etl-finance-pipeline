# src/etl/transform/normalize.py
from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger("Normalize")

PROTECTED = {"symbol", "ticker", "instrument", "pair"}


def _build_rename_map(df_cols: List[str], columns_map: Dict[str, List[str]]) -> Dict[str, str]:
    """
    Construye el diccionario de columnas a renombrar,
    evitando match por substring y excluyendo columnas protegidas.
    """
    rename_dict: Dict[str, str] = {}
    lowered = {c: c.lower() for c in df_cols}

    for target, variants in columns_map.items():
        target_up = target.upper()
        variants_lower = [v.lower() for v in variants]

        for col, col_lower in lowered.items():

            # ❌ No renombrar columnas protegidas (symbol, ticker...)
            if col_lower in PROTECTED:
                continue

            # ✔ Coincidencia exacta (open == open, OPEN == open)
            if col_lower in variants_lower:
                rename_dict[col] = target_up
                continue

            # ✔ Coincidencia controlada por prefijo o sufijo
            for v in variants_lower:
                if not v:
                    continue

                # prefijo exacto: open_price → OPEN
                if col_lower.startswith(v + "_"):
                    rename_dict[col] = target_up
                    break

                # sufijo exacto: price_open → OPEN
                if col_lower.endswith("_" + v):
                    rename_dict[col] = target_up
                    break

    return rename_dict


def normalize_columns(
    df: pd.DataFrame, columns_map: Dict[str, List[str]]
) -> Tuple[pd.DataFrame, Dict]:
    """
    Rename columns based on a mapping dictionary. Returns (df_copy, report)
    Report contains rename mapping and unmatched columns.
    """
    df = df.copy()
    report: Dict = {"renamed": {}, "unmatched": []}

    rename_dict = _build_rename_map(list(df.columns), columns_map)
    if rename_dict:
        logger.info(f"Renaming columns: {rename_dict}")
        df = df.rename(columns=rename_dict)
        report["renamed"] = rename_dict
    # report columns that look like they could be numeric but not mapped (informativo)
    expected_targets = {t.upper() for t in columns_map.keys()}
    remaining = [c for c in df.columns if c.upper() not in expected_targets]
    report["unmatched"] = remaining
    return df, report


def enforce_dtypes(df: pd.DataFrame, required_cols: List[str]) -> Tuple[pd.DataFrame, Dict]:
    """
    Convert columns to proper dtypes (numeric coerced to NaN on failure).
    Returns (df_copy, report_of_coercions)
    """
    df = df.copy()
    report: Dict = {"missing_required": [], "numeric_coercions": {}}

    # required columns check (case-sensitive after rename; tests expect OPEN/HIGH/LOW/CLOSE)
    for col in required_cols:
        if col not in df.columns:
            report["missing_required"].append(col)

    if report["missing_required"]:
        raise ValueError(f"Missing required column(s): {report['missing_required']}")

    numeric_cols = [
        c
        for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "TICKVOL", "SPREAD"]
        if c in df.columns
    ]

    # Numeric coercion
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        n_coerced = df[c].isna().sum()
        report["numeric_coercions"][c] = int(n_coerced)

        logger.info(f"Dtype coercion: column={c}, coerced_to_NaN={n_coerced}")

    return df, report


def normalize_datetime(
    df: pd.DataFrame, source_tz: str | None, target_tz: str
) -> Tuple[pd.DataFrame, Dict]:
    """
    Ensure datetime column is index and timezone-aware.
    Returns (df_copy, report)
    report includes: datetime_col, coerced_rows (NaT), tz_action (localized/assumed), original_tzinfo
    """
    df = df.copy()
    report: Dict = {"datetime_col": None, "coerced_rows": 0, "tz_action": None, "original_tz": None}

    datetime_col = None
    for col in df.columns:
        if col.lower() in ["datetime", "timestamp", "time"]:
            datetime_col = col
            break

    if datetime_col is None:
        raise ValueError("No datetime column found.")

    report["datetime_col"] = datetime_col
    # coerce to datetime
    coerced = pd.to_datetime(df[datetime_col], errors="coerce")
    report["coerced_rows"] = int(coerced.isna().sum())
    if report["coerced_rows"] > 0:
        logger.warning(
            f"{report['coerced_rows']} rows in {datetime_col} coerced to NaT (pd.to_datetime)."
        )

    df[datetime_col] = coerced
    df = df.set_index(datetime_col)

    # TZ Handling
    orig_tz = getattr(df.index, "tz", None)
    report["original_tz"] = str(orig_tz)
    if df.index.tz is None:
        if source_tz is None:
            logger.warning("Timestamp is tz-naive. Assuming UTC as source.")
            df.index = df.index.tz_localize("UTC")
            report["tz_action"] = "localized_to_UTC_assumed"
        else:
            df.index = df.index.tz_localize(source_tz)
            report["tz_action"] = f"localized_to_{source_tz}"
    else:
        report["tz_action"] = "already_tzaware"

    # finally convert to target
    try:
        df.index = df.index.tz_convert(target_tz)
        report["final_tz"] = target_tz
    except Exception as e:
        logger.error(f"Failed to convert timezone to {target_tz}: {e}")
        raise

    logger.info(f"Datetime localized to {target_tz}")
    return df, report


def remove_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """Remove duplicated timestamps. Returns (df_copy, report)."""
    before = len(df)
    df = df[~df.index.duplicated(keep="first")].copy()
    after = len(df)
    removed = before - after
    report = {"removed_duplicates": int(removed)}
    if removed > 0:
        logger.info(f"Removed {removed} duplicated rows.")
    return df, report


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

    Side effect: attaches a report dict to df.attrs['normalization_report']
    and returns the normalized DataFrame (index=DatetimeIndex tz-aware).
    """
    logger.info("Starting normalization...")

    # Work on a copy to avoid unexpected inplace modifications
    df_work = df.copy()
    full_report: Dict = {}

    df_work, col_report = normalize_columns(df_work, columns_map)
    full_report["columns"] = col_report

    df_work, dtype_report = enforce_dtypes(df_work, required_columns)
    full_report["dtypes"] = dtype_report

    df_work, dt_report = normalize_datetime(df_work, source_tz, target_tz)
    full_report["datetime"] = dt_report

    df_work, dup_report = remove_duplicates(df_work)
    full_report["duplicates"] = dup_report

    # final checks & sort
    df_work = df_work.sort_index()

    # attach report to DataFrame attrs (no firma nueva)
    df_work.attrs["normalization_report"] = full_report

    logger.info("Normalization completed successfully.")
    return df_work
