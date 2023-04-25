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

goals = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-gols.csv', **params)
)

statistics = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-estatisticas-full.csv', **params)
)

schemafull = """
ID int, rodada int, data string, hora string, dia string, mandante string, visitante string, formacao_mandante string, formacao_visitante string, tecnico_mandante string, tecnico_visitante string, vencedor string, arena string, mandante_placar int, visitante_placar int, mandante_estado string, visitante_estado string, estado_vencedor string
"""

full_informations = (
    spark
    .read
    .csv(full_path + '/tmp/campeonato-brasileiro-full.csv', header=True, sep=",", schema=schemafull)
)

full_informations.select(
    "ID", "rodada", "data", "hora", "dia"
).show()

# start temp views
cards.createOrReplaceTempView('cards')
goals.createOrReplaceTempView('goals')
statistics.createOrReplaceTempView('statistics')
full_informations.createOrReplaceTempView('full_informations')

# Testes ------------------------------------|

# 1 - Em quantas partidas o Palmeiras recebeu cartões amarelos quando jogava como visitante?

# with spark
(
    full_informations
    .select('ID', 'visitante')
    .join(cards, cards.partida_id == full_informations.ID, 'inner')
    .where("clube = 'Palmeiras' AND cartao = 'Amarelo'")
    .select('ID')
    .distinct()
    .agg(
        f.count('ID').alias('contagem')
    )
    .show()
)

# with SQL
spark.sql("""
SELECT
    COUNT(DISTINCT a.ID)
FROM full_informations as a 
JOIN cards b ON(a.ID = b.partida_id)
WHERE 
 b.clube = 'Palmeiras' 
 AND b.cartao = 'Amarelo'
""").show()
