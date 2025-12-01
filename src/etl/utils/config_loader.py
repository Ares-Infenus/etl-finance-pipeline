# src/etl/utils/config_loader.py
import os
from typing import Any, Dict

import yaml  # type: ignore


def _expand_env(val: str) -> str:
    return os.path.expandvars(val)


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    def walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [walk(v) for v in obj]
        if isinstance(obj, str):
            return _expand_env(obj)
        return obj

    return walk(raw)


def validate_config(cfg: Dict[str, Any]) -> None:
    io = cfg.get("io", {})
    parquet = cfg.get("parquet", {})
    schema = cfg.get("schema", {})

    if "raw_path" not in io or "processed_path" not in io:
        raise ValueError("io.raw_path y io.processed_path son obligatorios en config")

    if "compression" not in parquet:
        raise ValueError("parquet.compression debe estar definido")

    if "columns_map" not in schema or "required_columns" not in schema:
        raise ValueError("schema.columns_map y schema.required_columns son requeridos")


def get_config(path: str = "config/default.yml") -> Dict[str, Any]:
    cfg = load_yaml(path)
    validate_config(cfg)
    return cfg


if __name__ == "__main__":
    cfg = get_config()
    print("Raw path:", cfg["io"]["raw_path"])
