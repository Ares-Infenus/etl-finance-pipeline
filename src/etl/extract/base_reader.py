# src/etl/extract/base_reader.py
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

import pandas as pd


class BaseReader(ABC):

    @abstractmethod
    def read(self, path: Path) -> pd.DataFrame:
        """Reads a file and returns a DataFrame."""
        pass

    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """Returns metadata about the last read."""
        pass
