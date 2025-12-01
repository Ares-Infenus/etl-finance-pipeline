# src/etl/extract/csv_reader.py
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from ..utils.logger import get_logger
from .base_reader import BaseReader

logger = get_logger("CSVReader")


class CSVReader(BaseReader):
    _metadata: Dict[str, Any]

    def __init__(self) -> None:
        self._metadata = {}

    def read(self, path: Path) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                path,
                parse_dates=True,
                infer_datetime_format=True,
                low_memory=False,
                dtype_backend="numpy_nullable",
            )

            self._metadata = {
                "rows": len(df),
                "columns": df.columns.tolist(),
                "path": str(path),
                "status": "success",
                "type": "csv",
            }

            logger.info(f"Loaded CSV: {path} ({len(df)} rows)")
            return df

        except Exception as e:
            self._metadata = {"path": str(path), "status": "error", "error": str(e)}
            logger.error(f"Failed to load CSV {path}: {e}")
            raise

    def metadata(self) -> Dict[str, Any]:
        return self._metadata
