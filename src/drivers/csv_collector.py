from typing import Any

from src.utils.pyspark import Pyspark


class CsvCollector:
    def get_files(self, path_files) -> list[dict[str, str | Any]]:
        # start apache spark
        spark = Pyspark().init()

        # carrega dados dos file csv
        data = spark.read.csv(path_files, sep=';', inferSchema=True)

        ret = data

        return ret
