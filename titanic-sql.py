import os

full_path = os.path.dirname(os.path.realpath(__file__))
full_path_spark = full_path + '/spark/'

# spark-version: spark-3.3.1-bin-hadoop3
os.environ["SPARK_HOME"] = full_path + "/spark/"

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

filePath = full_path + '/tmp/titanic.csv'

# Leitura de dados
titanic = (
    spark
    .read
    .format('csv')
    .option("delimiter", ";")
    .option("header", True)
    .option("inferSchema", True)
    .load(filePath)
)

# ver estrutura
titanic.printSchema()

# view dados
titanic.show()

# iniciar uma view sql
titanic.createOrReplaceTempView('titanic')

spark.sql("""
    SELECT 
        Name, Sex, Pclass, Age
    FROM titanic
""").show()
