import pandas as pd
import pytest

from src.etl.transform.normalize import _build_rename_map, normalize_df

columns_map = {
    "OPEN": ["open", "o"],
    "HIGH": ["high", "h"],
    "LOW": ["low", "l"],
    "CLOSE": ["close", "c"],
}

required_columns = ["OPEN", "HIGH", "LOW", "CLOSE"]


def test_protected_columns_not_renamed() -> None:
    """
    Verifica que columnas protegidas como 'symbol' NO se renombren,
    y que el pipeline falle correctamente cuando faltan columnas requeridas.
    """

    df = pd.DataFrame(
        {
            "timestamp": ["2024-01-01 00:00:00"],
            "symbol": ["EURUSD"],  # PROTECTED → NO debe renombrarse
            "close": [4],
            "low_value": [3],
        }
    )

    # 1. Comprobar que symbol NO se renombra
    rename_map = _build_rename_map(df.columns.tolist(), columns_map)
    assert "symbol" not in rename_map

    # 2. El pipeline debe lanzar ValueError por columnas
    #    requeridas faltantes: OPEN y HIGH
    with pytest.raises(ValueError) as exc:
        normalize_df(
            df,
            columns_map=columns_map,
            required_columns=required_columns,
            source_tz="UTC",
            target_tz="UTC",
        )

    msg = str(exc.value)
    assert "Missing required column(s)" in msg
    assert "OPEN" in msg
    assert "HIGH" in msg
