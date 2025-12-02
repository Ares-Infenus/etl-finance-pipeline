# src/etl/transform/normalize.py
from __future__ import annotations

import logging
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from ..utils.logger import get_logger

logger = logging.getLogger(__name__)
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
    df: pd.DataFrame,
    source_tz: str | None,
    target_tz: str = "UTC",
) -> Tuple[pd.DataFrame, Dict]:
    """
    Normaliza la columna datetime del dataframe a timezone-aware y convierte a target_tz.

    Política:
      - Si el índice es tz-aware: solo tz_convert(target_tz).
      - Si tz-naive:
          * si source_tz provisto -> tz_localize(source_tz, ambiguous="NaT", nonexistent="shift_forward")
          * si source_tz no provisto -> tz_localize("UTC", ambiguous="NaT", nonexistent="shift_forward")
            y report["needs_review"]=True

    Devuelve (df_normalizado, report) donde report contiene keys:
      datetime_col, coerced_rows, tz_action, original_tz, final_tz,
      ambiguous_count, needs_review
    """
    # defensive copy
    df = df.copy()
    report: Dict = {
        "datetime_col": None,
        "coerced_rows": 0,
        "tz_action": None,
        "original_tz": None,
        "final_tz": None,
        "ambiguous_count": 0,
        "needs_review": False,
    }

    # 1) detectar columna datetime (prioridad a columnas explícitas)
    datetime_col = None
    for col in df.columns:
        if col.lower() in {"datetime", "timestamp", "time"}:
            datetime_col = col
            break
    if datetime_col is None:
        raise ValueError("No datetime column found in dataframe.")
    report["datetime_col"] = datetime_col

    # 2) coerción a datetime (sin forzar utc)
    coerced = pd.to_datetime(df[datetime_col], errors="coerce", utc=False)
    coerced_count = int(coerced.isna().sum())
    report["coerced_rows"] = coerced_count
    if coerced_count > 0:
        logger.warning(
            "%d rows in %s coerced to NaT by pd.to_datetime(..., errors='coerce').",
            coerced_count,
            datetime_col,
        )

    df[datetime_col] = coerced
    df = df.set_index(datetime_col)

    # 3) timezone handling
    orig_tz = getattr(df.index, "tz", None)
    report["original_tz"] = str(orig_tz)

    if df.index.tz is None:
        # tz-naive
        if source_tz:
            # validar source_tz
            try:
                ZoneInfo(source_tz)
            except Exception as e:
                logger.exception("Invalid source_tz '%s': %s", source_tz, e)
                raise
            try:
                df.index = df.index.tz_localize(
                    source_tz, ambiguous="NaT", nonexistent="shift_forward"
                )
                report["tz_action"] = f"localized_to_{source_tz}"
            except Exception as e:
                logger.exception("Failed to localize to %s: %s", source_tz, e)
                raise
        else:
            # política: asumir UTC pero marcar para revisión
            logger.warning(
                "Timestamps tz-naive and no source_tz provided — assuming UTC and marking needs_review."
            )
            df.index = df.index.tz_localize("UTC", ambiguous="NaT", nonexistent="shift_forward")
            report["tz_action"] = "localized_to_UTC_assumed"
            report["needs_review"] = True
    else:
        report["tz_action"] = "already_tzaware"

    # 4) contar ambiguous / NaT introducidos por la localización
    try:
        ambiguous_count = int(df.index.isna().sum())
        report["ambiguous_count"] = ambiguous_count
        if ambiguous_count > 0:
            logger.warning(
                "%d timestamps became NaT after localization (ambiguous/nonexistent). Marking needs_review.",
                ambiguous_count,
            )
            report["needs_review"] = True
    except Exception:
        # fallback defensivo
        pass

    # 5) convertir a target tz
    try:
        df.index = df.index.tz_convert(target_tz)
        report["final_tz"] = target_tz
    except Exception as e:
        logger.exception("Failed to convert timezone to %s: %s", target_tz, e)
        raise

    logger.info("Datetime column '%s' normalized to tz %s", datetime_col, report["final_tz"])
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
