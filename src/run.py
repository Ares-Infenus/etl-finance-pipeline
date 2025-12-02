# src/run.py
"""
Entry point for the ETL pipeline (minimal, opinionated).
Usage:
    python -m src.run --config config/default.yml --dry-run False
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from src.etl.extract.extractor import Extractor
from src.etl.load.exporter import append_export_log, write_parquet_with_metadata
from src.etl.transform.normalize import normalize_df
from src.etl.utils.config_loader import get_config
from src.etl.utils.logger import get_logger

logger = get_logger("ETL_Run")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def infer_symbol_from_df(df: pd.DataFrame, fallback: str) -> str:
    """
    Detect a symbol/ticker column in the df and return its first value.
    If none found, return fallback.
    Normalizes to uppercase string.
    """
    candidates = ("symbol", "ticker", "pair", "instrument", "sym")
    for col in df.columns:
        if col.lower() in candidates:
            try:
                val = df[col].iloc[0]
                if pd.notna(val):
                    return str(val).upper()
            except Exception:
                continue
    # fallback from provided basename (e.g. "EURUSD_20240101_20240131" -> "EURUSD")
    if isinstance(fallback, str) and "_" in fallback:
        return fallback.split("_")[0].upper()
    return str(fallback).upper()


def resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample an OHLCV-like dataframe to given frequency.
    Expected columns: OPEN, HIGH, LOW, CLOSE, (optional) VOLUME / TICKVOL / SPREAD / SYMBOL
    """
    agg_map: Dict[str, str] = {}
    if "OPEN" in df.columns:
        agg_map["OPEN"] = "first"
    if "HIGH" in df.columns:
        agg_map["HIGH"] = "max"
    if "LOW" in df.columns:
        agg_map["LOW"] = "min"
    if "CLOSE" in df.columns:
        agg_map["CLOSE"] = "last"

    # volume-like columns summed
    for vol_col in ("VOLUME", "TICKVOL", "VOL"):
        if vol_col in df.columns:
            agg_map[vol_col] = "sum"

    # keep symbol by last (if present)
    if "SYMBOL" in df.columns:
        agg_map["SYMBOL"] = "last"

    # fallback: if no OHLC, return original
    if not agg_map:
        logger.warning("No OHLC columns found for resample. Returning original df.")
        return df

    # Perform resample
    res = df.resample(rule).agg(agg_map)

    # For additional numeric columns not covered, try mean
    for col in df.columns:
        if col not in res.columns and pd.api.types.is_numeric_dtype(df[col]) and col not in agg_map:
            res[col] = df[col].resample(rule).mean()

    # Drop intervals without CLOSE (incomplete)
    if "CLOSE" in res.columns:
        res = res[~res["CLOSE"].isna()]

    return res


# NOTE: write_parquet logic replaced by specialized exporter.write_parquet_with_metadata


def data_quality_report(df: pd.DataFrame) -> Dict:
    d = {
        "rows": int(len(df)),
        "start": str(df.index.min()) if len(df) > 0 else None,
        "end": str(df.index.max()) if len(df) > 0 else None,
        "columns": list(df.columns),
        "nans_per_column": {c: int(df[c].isna().sum()) for c in df.columns},
        "dups_timestamps": int(df.index.duplicated().sum()),
    }
    return d


def process_dataframe(
    df: pd.DataFrame,
    cfg: Dict,
    source_tz: Optional[str],
    basename: str,
    out_dir: Path,
) -> None:
    """
    Full pipeline for a single DataFrame:
     - normalize
     - resample (each timeframe)
     - write parquet files (one per timeframe)
     - write quality report JSON
    """
    try:
        logger.info(f"Processing input: {basename}")

        # Normalize column names & types + timezone + dedupe
        normalized = None
        try:
            normalized = normalize_df(
                df,
                columns_map=cfg["schema"]["columns_map"],
                required_columns=cfg["schema"]["required_columns"],
                source_tz=source_tz,
                target_tz=cfg["timezone"]["target"],
            )
        except Exception as e:
            # Log full exception and re-raise so caller knows normalization failed.
            logger.exception("Normalization failed for input (basename=%s): %s", basename, e)
            raise

        # Ensure symbol is recorded in normalization attrs for exporter metadata
        try:
            if "SYMBOL" in normalized.columns:
                # prefer existing SYMBOL column value (first non-null)
                sym_val = normalized["SYMBOL"].dropna().astype(str).iloc[0]
                normalized.attrs["symbol"] = str(sym_val).upper()
            else:
                # fallback: use previously inferred symbol from original df if present
                if "SYMBOL" in df.columns:
                    normalized.attrs["symbol"] = str(df["SYMBOL"].iloc[0]).upper()
        except Exception:
            # non-fatal; exporter can handle missing symbol
            pass

        # ensure sorted
        # === Timezone post-check ===
        try:
            tz = normalized.index.tz
            if tz is None:
                logger.error(
                    "[TZ-ERROR] Normalized DF for %s is tz-naive AFTER normalization.", basename
                )
            else:
                tz_str = str(tz)
                if tz_str != "UTC" and tz_str.lower() != "utc":
                    logger.error(
                        "[TZ-ERROR] Normalized DF for %s has tz=%s, expected UTC.", basename, tz_str
                    )
                else:
                    logger.info("[TZ-CHECK] OK — Index timezone is UTC.")
        except Exception as e:
            logger.exception("[TZ-CHECK] Exception while checking timezone for %s: %s", basename, e)
        # === END TZ CHECK ===

        # generate and save data quality report (sidecar)
        report = data_quality_report(normalized)
        report_path = out_dir / f"{basename}_quality_report.json"
        ensure_dir(report_path.parent)
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        logger.info(f"Wrote quality report: {report_path}")

        # Resample and write for each timeframe in config using exporter
        resample_cfg = cfg.get("resample") or {}
        timeframes = resample_cfg.get("timeframes", [])
        parquet_cfg = cfg.get("parquet", {})
        compression = parquet_cfg.get("compression", "zstd")
        engine = parquet_cfg.get("engine", "pyarrow")
        partition_cols = parquet_cfg.get("partition_cols")

        export_log_dir = Path(cfg["io"].get("reports_path", "data/reports")) / "exports"
        ensure_dir(export_log_dir)

        if not timeframes:
            out_file = out_dir / f"{basename}_raw.parquet"
            try:
                meta = {
                    "symbol": normalized.attrs.get("symbol", None),
                    "timeframe": "raw",
                    "source_basename": basename,
                }
                export_report = write_parquet_with_metadata(
                    normalized,
                    out_file,
                    compression=compression,
                    engine=engine,
                    partition_cols=partition_cols,
                    metadata=meta,
                )
                export_entry = {
                    "basename": basename,
                    "timeframe": "raw",
                    "out_path": str(out_file),
                    "export_report": export_report,
                }
                append_export_log(export_log_dir, export_entry)
            except Exception as e:
                logger.error("Export failed for %s: %s", out_file, e)
        else:
            for tf in timeframes:
                try:
                    res = resample_ohlc(normalized, tf)
                    # name timeframes nicely (1T -> 1m)
                    tf_suffix = tf.replace("T", "m").lower()
                    out_file = out_dir / f"{basename}_{tf_suffix}.parquet"

                    meta = {
                        "symbol": normalized.attrs.get("symbol", None),
                        "timeframe": tf_suffix,
                        "source_basename": basename,
                    }
                    export_report = write_parquet_with_metadata(
                        res,
                        out_file,
                        compression=compression,
                        engine=engine,
                        partition_cols=partition_cols,
                        metadata=meta,
                    )
                    export_entry = {
                        "basename": basename,
                        "timeframe": tf_suffix,
                        "out_path": str(out_file),
                        "export_report": export_report,
                    }
                    append_export_log(export_log_dir, export_entry)

                    logger.info(f"Resampled to {tf}, rows={len(res)} -> wrote {out_file}")
                except Exception as e:
                    logger.error(f"Failed resample for timeframe {tf}: {e}")

    except Exception as exc:
        logger.exception(f"Failed processing {basename}: {exc}")


def main(config_path: str, dry_run: bool = True) -> None:
    cfg = get_config(config_path)

    raw_path = Path(cfg["io"]["raw_path"])
    processed_path = Path(cfg["io"]["processed_path"])
    quarantine_path = Path(cfg["io"].get("quarantine_path", "data/quarantine"))

    ensure_dir(processed_path)
    ensure_dir(quarantine_path)

    extractor = Extractor(raw_path)

    logger.info("Starting extraction...")
    # Extractor ahora devuelve una lista de dicts: {"df","meta","filename"}
    items = extractor.load_all()
    logger.info(
        f"Extraction finished. {len(items)} files loaded (some files may have been quarantined)."
    )

    # Process each DataFrame
    for i, item in enumerate(items):
        df = item["df"]
        meta = item.get("meta", {})

        basename = f"input_{i}"

        # try to find a symbol/name column
        for candidate in ("SYMBOL", "symbol", "TICKER", "ticker"):
            if candidate in df.columns:
                try:
                    basename = str(df[candidate].iloc[0])
                except Exception:
                    pass
                break

        # Add date range to basename if possible to make outputs traceable
        try:
            if "datetime" in (c.lower() for c in df.columns):
                dt_col = next(
                    c for c in df.columns if c.lower() in ("datetime", "timestamp", "time")
                )
                start = pd.to_datetime(df[dt_col], errors="coerce").min()
                end = pd.to_datetime(df[dt_col], errors="coerce").max()
                if pd.notna(start) and pd.notna(end):
                    basename = f"{basename}_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        except Exception:
            pass

        # === NEW: infer and inject SYMBOL column if missing ===
        inferred_symbol = infer_symbol_from_df(df, basename)
        # If any variant of symbol exists, prefer it; else create SYMBOL
        symbol_cols = [
            c for c in df.columns if c.lower() in ("symbol", "ticker", "pair", "instrument", "sym")
        ]
        if symbol_cols:
            # normalize existing symbol column to SYMBOL uppercase
            col = symbol_cols[0]
            df["SYMBOL"] = df[col].astype(str).str.upper()
        else:
            df["SYMBOL"] = inferred_symbol

        # ensure SYMBOL is present and uppercase
        df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper()
        # === END NEW ===

        out_dir_for_df = processed_path / basename
        if dry_run:
            logger.info(f"[dry-run] Would process '{basename}' -> output dir: {out_dir_for_df}")
            continue

        # actual processing
        # TIMEZONE PRIORITY
        config_source_default = cfg["timezone"].get("source_default")
        meta_source_tz = meta.get("source_tz")
        source_to_use = meta_source_tz or config_source_default

        process_dataframe(
            df,
            cfg,
            source_tz=source_to_use,
            basename=basename,
            out_dir=out_dir_for_df,
        )

    logger.info("ETL run finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline (extract -> transform -> load).")
    parser.add_argument("--config", "-c", default="config/default.yml", help="Path to config YAML.")
    parser.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="Execute write operations (default is dry-run).",
    )
    args = parser.parse_args()

    main(config_path=args.config, dry_run=args.dry_run)
