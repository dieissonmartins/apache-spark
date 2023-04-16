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
    .master('local[1]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Leitura de dados
titanic = (
    spark
    .read
    .format('csv')
    .option("delimiter", ";")
    .option("header", True)
    .option("inferSchema", True)
    .load("https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv")
)

# ver estrutura
titanic.printSchema()

# view dados
titanic.show()

# selecionar colunas
titanic.select('Name', 'Sex', 'Pclass', 'Survived')
