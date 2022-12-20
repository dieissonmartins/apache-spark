import os

from mss import exception

from src.drivers.interfaces import CsvCollectorInterface
from src.errors.extract_error import ExtractError


class ExtractCompanies:
    def __init__(self, init_csv_collector: CsvCollectorInterface) -> None:
        self.__csv_collector = init_csv_collector
        self.__full_path = os.path.dirname(os.path.realpath(__file__))

    def extract(self):
        try:

            path = self.__full_path + '/../../../tmp/empresas/'
            companies = self.__csv_collector.get_files(path)

            ret = companies

            return ret

        except Exception as e:
            raise ExtractError(str(e)) from exception
