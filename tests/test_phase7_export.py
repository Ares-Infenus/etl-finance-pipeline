import json
from pathlib import Path

import pandas as pd

from src.etl.load.exporter import append_export_log, write_parquet_with_metadata


def _make_sample_df() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01T00:00:00", periods=10, freq="T", tz="UTC")
    df = pd.DataFrame(
        {
            "OPEN": range(10, 20),
            "HIGH": range(11, 21),
            "LOW": range(9, 19),
            "CLOSE": range(10, 20),
            "VOLUME": [100] * 10,
        },
        index=idx,
    )
    # attach a fake normalization report to simulate pipeline
    df.attrs["normalization_report"] = {"columns": {"renamed": {}, "unmatched": []}}
    return df


def test_write_parquet_with_metadata(tmp_path: Path) -> None:
    df = _make_sample_df()
    out = tmp_path / "sample_1m.parquet"
    report = write_parquet_with_metadata(
        df, out, compression="zstd", engine="pyarrow", partition_cols=["YEAR"]
    )
    assert "rows" in report
    # sidecar metadata file exists
    meta_file = out.with_name(out.name + ".meta.json")
    assert meta_file.exists()
    with open(meta_file, "r", encoding="utf-8") as fh:
        meta = json.load(fh)
    assert meta["rows"] == report["rows"]


def test_append_export_log(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    entry = {"a": 1}
    append_export_log(log_dir, entry)
    log_file = log_dir / "export_log.ndjson"
    assert log_file.exists()
    with open(log_file, "r", encoding="utf-8") as fh:
        lines = fh.read().strip().splitlines()
    assert len(lines) == 1
