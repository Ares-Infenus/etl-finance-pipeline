# src/etl/extract/file_detector.py
from pathlib import Path


def detect_file_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".csv":
        return "csv"
    if ext == ".parquet":
        return "parquet"
    raise ValueError(f"Unsupported file type: {ext}")
