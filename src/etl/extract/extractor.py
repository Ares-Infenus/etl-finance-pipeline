# src/etl/extract/extractor.py
from pathlib import Path
from typing import List

import pandas as pd

from ..utils.logger import get_logger
from .csv_reader import CSVReader
from .file_detector import detect_file_type
from .parquet_reader import ParquetReader

logger = get_logger("Extractor")


class Extractor:
    def __init__(self, raw_path: Path):
        self.raw_path = raw_path
        self.csv_reader = CSVReader()
        self.parquet_reader = ParquetReader()

    def load_all(self) -> List[pd.DataFrame]:
        results = []

        for file in self.raw_path.iterdir():
            if not file.is_file():
                continue

            try:
                file_type = detect_file_type(file)

                if file_type == "csv":
                    df = self.csv_reader.read(file)
                    results.append(df)
                else:
                    df = self.parquet_reader.read(file)
                    results.append(df)

            except Exception:
                logger.error(f"File moved to quarantine: {file}")
                # aquí mover a carpeta quarantine
                continue

        return results
