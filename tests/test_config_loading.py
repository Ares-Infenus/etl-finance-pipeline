def test_load_default_config() -> None:
    from src.etl.utils.config_loader import get_config

    cfg = get_config("config/default.yml")

    assert "io" in cfg
    assert "parquet" in cfg
    assert "schema" in cfg
    assert cfg["io"]["raw_path"] != ""
    assert cfg["parquet"]["compression"] in ["zstd", "snappy", "gzip"]
