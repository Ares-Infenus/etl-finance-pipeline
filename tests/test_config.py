from src.etl.utils.config_loader import get_config


def test_config_loads() -> None:
    cfg = get_config("config/testing.yml")

    assert cfg["io"]["raw_path"].startswith("tests")
    assert "OPEN" in cfg["schema"]["columns_map"]
