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

# dados da empresas
path_companies = full_path + '/tmp/empresas'
companies = spark.read.csv(path_companies, sep=';', inferSchema=True)
companies_count = companies.count()
print('Quantidade de empresas: ' + str(companies_count))

# dados socios
path_partners = full_path + '/tmp/socios'
partners = spark.read.csv(path_partners, sep=';', inferSchema=True)
partners_count = partners.count()
print('Quantidade de socios: ' + str(partners_count))

# dados estabelecimentos
path_establishments = full_path + '/tmp/estabelecimentos'
establishments = spark.read.csv(path_establishments, sep=';', inferSchema=True)
establishments_count = establishments.count()
print('Quantidade de estabelecimentos: ' + str(establishments_count))
