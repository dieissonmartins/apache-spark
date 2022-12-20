import os
import findspark


class Pyspark:
    def init(self):
        full_path = os.path.dirname(os.path.realpath(__file__))
        full_path_spark = full_path + '/../../spark/'

        os.environ["SPARK_HOME"] = full_path_spark

        findspark.init()

        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .master('local[*]') \
            .appName("Iniciando com Spark") \
            .config('spark.ui.port', '4050') \
            .getOrCreate()

        ret = spark

        return ret
