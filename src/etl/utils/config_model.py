# src/etl/utils/config_model.py
import json
from pathlib import Path
from typing import Dict, List, Optional

import yaml  # type: ignore
from pydantic import BaseModel, Field, validator


class IOConfig(BaseModel):
    raw_path: Path
    processed_path: Path
    reports_path: Optional[Path] = Path("data/reports")
    quarantine_path: Optional[Path] = Path("data/quarantine")


class ParquetConfig(BaseModel):
    compression: str = "zstd"
    engine: str = "pyarrow"
    partition_cols: Optional[List[str]] = None
    row_group_size: Optional[int] = 65536


class SchemaConfig(BaseModel):
    columns_map: Dict[str, List[str]]
    required_columns: List[str]

    @validator("columns_map")
    def normalize_keys(cls, v: Dict[str, List[str]]) -> Dict[str, List[str]]:
        return {k.upper(): v for k, v in v.items()}


class TimezoneConfig(BaseModel):
    target: str = "UTC"
    source_default: Optional[str] = None
    # policy_if_na: behavior when timestamps are tz-naive and no source_tz is provided
    # allowed: "assume_utc", "require_source", "mark_needs_review"
    policy_if_na: str = "assume_utc"

    @classmethod
    def validate_policy(cls, v: str) -> str:
        allowed = {"assume_utc", "require_source", "mark_needs_review"}
        if v not in allowed:
            raise ValueError(f"policy_if_na must be one of {allowed}")
        return v


class ResampleConfig(BaseModel):
    timeframes: List[str] = Field(default_factory=lambda: ["1T", "5T", "1H"])
    gap_policy: Optional[Dict] = None


class Config(BaseModel):
    io: IOConfig
    parquet: ParquetConfig
    schema: SchemaConfig
    timezone: TimezoneConfig
    resample: Optional[ResampleConfig]
    export: Optional[Dict]
    logging: Optional[Dict]


def load_config_pydantic(path: str = "config/default.yml") -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    raw_s = json.loads(json.dumps(raw))  # evita mutación del dict
    return Config(**raw_s)
