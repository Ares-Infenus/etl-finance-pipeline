from pathlib import Path

import pandas as pd
from pandas import DataFrame

from src.etl.transform.normalize import normalize_df
from src.etl.utils.config_loader import get_config

CONFIG_PATH = "config/default.yml"
cfg = get_config(CONFIG_PATH)


def load_csv(path: str) -> DataFrame:
    return pd.read_csv(path)


def test_normalize_ok(tmp_path: Path) -> None:
    df: DataFrame = load_csv("tests/data/transform_samples/sample_ok.csv")
    res: DataFrame = normalize_df(
        df,
        columns_map=cfg["schema"]["columns_map"],
        required_columns=cfg["schema"]["required_columns"],
        source_tz=cfg["timezone"].get("source_default"),
        target_tz=cfg["timezone"]["target"],
    )

    assert res.index.dtype.kind == "M"
    assert res.index.tz is not None or res.index.tzinfo is not None

    for col in ("OPEN", "HIGH", "LOW", "CLOSE"):
        assert col in res.columns
        assert pd.api.types.is_numeric_dtype(res[col])

    assert res.index.duplicated().sum() == 0


def test_remove_duplicates() -> None:
    df: DataFrame = load_csv("tests/data/transform_samples/sample_duplicates.csv")
    res: DataFrame = normalize_df(
        df,
        columns_map=cfg["schema"]["columns_map"],
        required_columns=cfg["schema"]["required_columns"],
        source_tz=cfg["timezone"].get("source_default"),
        target_tz=cfg["timezone"]["target"],
    )
    assert len(res) == 2


def test_tzaware() -> None:
    df: DataFrame = load_csv("tests/data/transform_samples/sample_tzaware.csv")
    res: DataFrame = normalize_df(
        df,
        columns_map=cfg["schema"]["columns_map"],
        required_columns=cfg["schema"]["required_columns"],
        source_tz=None,
        target_tz=cfg["timezone"]["target"],
    )
    assert res.index.tz is not None


def test_string_close_becomes_nan() -> None:
    df: DataFrame = load_csv("tests/data/transform_samples/sample_strings.csv")
    res: DataFrame = normalize_df(
        df,
        columns_map=cfg["schema"]["columns_map"],
        required_columns=cfg["schema"]["required_columns"],
        source_tz=cfg["timezone"].get("source_default"),
        target_tz=cfg["timezone"]["target"],
    )
    assert pd.isna(res["CLOSE"].iloc[0])
