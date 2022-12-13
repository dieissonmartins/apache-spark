import os

full_path = os.path.dirname(os.path.realpath(__file__))

# spark-version: spark-3.3.1-bin-hadoop3
os.environ["SPARK_HOME"] = full_path + "/spark/"

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .getOrCreate()

print(spark)
