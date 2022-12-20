from abc import ABC, abstractmethod
from typing import Any


class CsvCollectorInterface(ABC):

    @abstractmethod
    def get_files(self, path_files) -> list[dict[str, str | Any]]:
        pass
