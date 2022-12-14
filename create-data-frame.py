import os

full_path = os.path.dirname(os.path.realpath(__file__))
full_path_spark = full_path + '/spark/'

# spark-version: spark-3.3.1-bin-hadoop3
os.environ["SPARK_HOME"] = full_path + "/spark/"

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .getOrCreate()

data = [
    {
        'Name': 'dieisson',
        'Age': 18
    },
    {
        'Name': 'maria',
        'Age': 22
    }
]

df = spark.createDataFrame(data)

df.show()
