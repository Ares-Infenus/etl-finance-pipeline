# src/etl/transform/resample.py
from __future__ import annotations

from typing import Dict

import pandas as pd

from src.etl.utils.logger import get_logger

logger = get_logger("Resampler")


def _build_agg_map(df: pd.DataFrame) -> Dict[str, str]:
    """
    Construye el mapa de agregación OHLCV y columnas auxiliares.
    """
    agg: Dict[str, str] = {}
    if "OPEN" in df.columns:
        agg["OPEN"] = "first"
    if "HIGH" in df.columns:
        agg["HIGH"] = "max"
    if "LOW" in df.columns:
        agg["LOW"] = "min"
    if "CLOSE" in df.columns:
        agg["CLOSE"] = "last"

    # columnas de volumen -> sumar
    for vol in ("VOLUME", "TICKVOL", "VOL"):
        if vol in df.columns:
            agg[vol] = "sum"

    # conservar symbol (si existe) como last
    if "SYMBOL" in df.columns:
        agg["SYMBOL"] = "last"

    return agg


def resample_ohlc(
    df: pd.DataFrame,
    rule: str,
    *,
    drop_incomplete: bool = True,
    extra_numeric_policy: str = "mean",
    keep_time_index: bool = True,
) -> pd.DataFrame:
    """
    Resamplea un DataFrame OHLC-like a la frecuencia `rule` (pandas offset alias).

    Requisitos:
      - índice: pd.DatetimeIndex (preferible tz-aware; no lo cambia aquí).
      - columnas esperadas (opcional): OPEN, HIGH, LOW, CLOSE, VOLUME/TICKVOL.

    Parámetros:
      - rule: '5T', '1H', '1D', ...
      - drop_incomplete: si True, descarta intervalos sin CLOSE (incompletos).
      - extra_numeric_policy: política para columnas numéricas no mapeadas ('mean' o 'sum').
      - keep_time_index: si True mantiene DatetimeIndex en el resultado.

    Retorna:
      DataFrame reindexado/resampleado con la agregación apropiada.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame index must be a DatetimeIndex.")

    agg = _build_agg_map(df)
    if not agg:
        logger.warning("No OHLC/VOLUME columns detected — returning original dataframe.")
        return df.copy()

    # Resample usando el mapa de agregación principal
    res = df.resample(rule).agg(agg)

    # Para columnas numéricas no incluidas explícitamente, aplicar política
    other_cols = [c for c in df.columns if c not in agg.keys()]
    for c in other_cols:
        if pd.api.types.is_numeric_dtype(df[c]):
            if extra_numeric_policy == "mean":
                res[c] = df[c].resample(rule).mean()
            elif extra_numeric_policy == "sum":
                res[c] = df[c].resample(rule).sum()
            else:
                # default: mean
                res[c] = df[c].resample(rule).mean()

    # Por seguridad: si SYMBOL estuvo presente en df pero no en res columns (pandas podría no crearla), intentar setearla
    if "SYMBOL" in df.columns and "SYMBOL" not in res.columns:
        res["SYMBOL"] = df["SYMBOL"].resample(rule).last()

    # Si pedimos drop_incomplete: eliminar intervalos donde CLOSE es NaN
    if drop_incomplete and "CLOSE" in res.columns:
        before = len(res)
        res = res[~res["CLOSE"].isna()]
        after = len(res)
        logger.info("Dropped %d incomplete intervals during resample(%s).", before - after, rule)

    # Mantener el tipo de índice (tz-aware) — no forzamos cambios de tz aquí.
    if keep_time_index:
        return res
    return res.reset_index()
