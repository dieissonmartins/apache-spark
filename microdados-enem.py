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

# baixe no arquivo Microdados do Enem 2020 na pasta temp
# https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem

