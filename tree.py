import os
from typing import List


def mostrar_carpetas(ruta: str, nivel: int = 0, max_nivel: int = 2, prefijo: str = "") -> None:
    if nivel > max_nivel:
        return

    try:
        entradas: List[str] = os.listdir(ruta)
    except FileNotFoundError:
        return

    carpetas = [d for d in entradas if os.path.isdir(os.path.join(ruta, d))]

    for i, nombre in enumerate(carpetas):
        ruta_completa = os.path.join(ruta, nombre)
        es_ultimo = i == len(carpetas) - 1
        conector = "└── " if es_ultimo else "├── "
        print(prefijo + conector + nombre)
        nuevo_prefijo = prefijo + ("    " if es_ultimo else "│   ")
        mostrar_carpetas(ruta_completa, nivel + 1, max_nivel, nuevo_prefijo)


if __name__ == "__main__":
    mostrar_carpetas(
        "C:\\Users\\spinz\\OneDrive\\Desktop\\Proyect_Portfolio\\Proyect_Pipeline_ETL_Finance",
        max_nivel=2,
    )
