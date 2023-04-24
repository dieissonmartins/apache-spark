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

params = {
    "header": True,
    "inferSchema": True,
    "sep": ","
}

cards = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-cartoes.csv', **params)
)

goal = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-gols.csv', **params)
)

statistic = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-estatisticas-full.csv', **params)
)

schemafull = """
ID int, rodada int, data string, hora string, dia string, mandante string, visitante string, formacao_mandante string, formacao_visitante string, tecnico_mandante string, tecnico_visitante string, vencedor string, arena string, mandante_placar int, visitante_placar int, mandante_estado string, visitante_estado string, estado_vencedor string
"""

informations_full = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-full.csv', header=True, sep=",", schema=schemafull)
)

informations_full.select(
    "ID", "rodada", "data", "hora", "dia"
).show()
