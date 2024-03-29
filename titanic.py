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
limit = 10
titanic.show(n=limit, truncate=False)

# selecionar colunas
titanic.select('Name', 'Sex', 'Pclass', 'Survived')

# filtrar linhas
titanic.where("Sex='male' AND Survived=1").show()

# agregar valores
titanic.agg(
    f.mean('Age').alias('med_idade'),
    f.min('Age').alias('min_idade'),
    f.max('Age').alias('max_idade'),
    f.stddev('Age').alias('desvio_padrao_idade'),
).show()

# metricas agrupadas
(
    titanic
    .groupBy('Sex')
    .agg(
        f.mean('Age').alias('med_idade'),
        f.min('Age').alias('min_idade'),
        f.max('Age').alias('max_idade'),
        f.stddev('Age').alias('desvio_padrao_idade')
    )
    .show()
)

# ordenacao dos dados
(
    titanic
    .groupBy('Sex', 'Pclass', 'Survived')
    .agg(
        f.mean('Age').alias('med_idade'),
        f.min('Age').alias('min_idade'),
        f.max('Age').alias('max_idade'),
        f.stddev('Age').alias('desvio_padrao_idade')
    )
    .orderBy('Pclass', 'Sex', 'Survived')
    .show()
)
