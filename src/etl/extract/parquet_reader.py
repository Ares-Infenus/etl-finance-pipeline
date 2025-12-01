# src/etl/extract/parquet_reader.py
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from ..utils.logger import get_logger
from .base_reader import BaseReader

logger = get_logger("ParquetReader")


class ParquetReader(BaseReader):
    _metadata: Dict[str, Any]

    def __init__(self) -> None:
        self._metadata = {}

    def read(self, path: Path) -> pd.DataFrame:
        try:
            df = pd.read_parquet(path, engine="pyarrow")

            self._metadata = {
                "rows": len(df),
                "columns": df.columns.tolist(),
                "path": str(path),
                "status": "success",
                "type": "parquet",
            }

            logger.info(f"Loaded Parquet: {path} ({len(df)} rows)")
            return df

        except Exception as e:
            self._metadata = {"path": str(path), "status": "error", "error": str(e)}
            logger.error(f"Failed to load Parquet {path}: {e}")
            raise

    def metadata(self) -> Dict[str, Any]:
        return self._metadata
