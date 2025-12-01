# src/run.py (entrypoint minimal)
from src.extract.reader import read_inputs  # tus módulos
from src.transform.normalize import normalize_df
from src.utils.config_loader import get_config

cfg = get_config("config/default.yml")

# ejemplo usage:
raw_path = cfg["io"]["raw_path"]
processed_path = cfg["io"]["processed_path"]
columns_map = cfg["schema"]["columns_map"]
tz_target = cfg["timezone"]["target"]

# 1) leer
df = read_inputs(raw_path)

# 2) normalizar columnas según config
df = normalize_df(df, columns_map)

# 3) timezone handling
# (tu función que localiza/convierte a tz_target)

# 4) resample según cfg["resample"]["timeframes"]
# 5) write_parquet(..., compression=cfg["parquet"]["compression"])
