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

filePath = full_path + '/tmp/MICRODADOS_ENEM_2020.csv'

# Leitura de dados
microdados_enem = (
    spark
    .read
    .format('csv')
    .option("delimiter", ";")
    .option("header", True)
    .option("inferSchema", True)
    .load(filePath)
)

# ver estrutura
#microdados_enem.printSchema()

# view dados
#microdados_enem.show()

# iniciar uma view sql
microdados_enem.createOrReplaceTempView('microdados_enem')


spark.sql("""
    SELECT 
         TP_SEXO
        ,CO_PROVA_CN
        ,CO_PROVA_CH
        ,CO_PROVA_LC
        ,CO_PROVA_MT
        ,NU_NOTA_CN
        ,NU_NOTA_CH
        ,NU_NOTA_LC
        ,NU_NOTA_MT
    FROM microdados_enem
""").show()


# média da nota em matemática apenas para as alunas do sexo Feminino
spark.sql("""
SELECT AVG(NU_NOTA_MT) FROM microdados_enem WHERE TP_SEXO = 'F'
""").show()