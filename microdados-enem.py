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
# microdados_enem.printSchema()

# view dados
# microdados_enem.show()

# iniciar uma view sql
microdados_enem.createOrReplaceTempView('microdados_enem')

spark.sql("""
    SELECT 
       *
    FROM microdados_enem
     WHERE NO_MUNICIPIO_ESC = 'Rio de Janeiro'
""").show()

print('média da nota em matemática apenas para as alunas do sexo Feminino:')
spark.sql("""
SELECT AVG(NU_NOTA_MT) FROM microdados_enem WHERE TP_SEXO = 'F'
""").show()

print(
    'média da nota em Ciências Humanas para os alunos do sexo masculino que estudaram numa escola no estado de São Paulo')
spark.sql("""
SELECT AVG(NU_NOTA_CH) FROM microdados_enem WHERE TP_SEXO = 'M' AND SG_UF_ESC = 'SP'
""").show()

# print('cidade (município da escola) que possui a MAIOR nota em Ciências Naturais')
# spark.sql("""
# SELECT MAX(NU_NOTA_CN), CO_MUNICIPIO_PROVA FROM microdados_enem
# """).show()

print('média da nota em Ciências Humanas dos alunos que estudaram numa escola na cidade de Natal')
spark.sql("""
SELECT AVG(NU_NOTA_CH) FROM microdados_enem WHERE CO_MUNICIPIO_ESC = 2408102
""").show()

#print('município (município da escola) que obteve a maior nota MÉDIA em matemática?')
#spark.sql("""
#SELECT AVG(NU_NOTA_MT), NO_MUNICIPIO_PROVA
#FROM microdados_enem
#GROUP BY NO_MUNICIPIO_PROVA
#ORDER BY AVG(NU_NOTA_MT) DESC
#""").show()

print('pessoas que estudaram numa escola no estado do Rio de Janeiro do sexo masculino obtiveram nota em matemática maior do que 600')
spark.sql("""
SELECT COUNT(DISTINCT NU_INSCRICAO) 
FROM microdados_enem 
WHERE TP_SEXO = 'M' AND SG_UF_ESC = 'RJ' AND NU_NOTA_MT > 600
""").show()

print('pessoas que estudaram numa escola em Recife fizeram a prova do ENEM nessa mesma cidade')
spark.sql("""
SELECT COUNT(DISTINCT NU_INSCRICAO) 
FROM microdados_enem 
WHERE NO_MUNICIPIO_ESC = 'Recife' AND NO_MUNICIPIO_ESC = NO_MUNICIPIO_PROVA
""").show()

#print('mulheres que moram sozinhas, estudaram numa escola no estado de SP fizeram a prova de Ciências Humanas do ENEM em 2020')
#spark.sql("""
#SELECT COUNT(DISTINCT NU_INSCRICAO)
#FROM microdados_enem
#WHERE SG_UF_ESC = 'SP' AND TP_PRESENCA_CH = 1 AND TP_DEPENDENCIA_ADM_ESC = 1
#""").show()

print('média em Ciências Humanas dos alunos que estudaram numa escola no estado de Santa Catarina e possuem PELO MENOS 1 banheiro em casa')
spark.sql("""
SELECT AVG(NU_NOTA_CH) 
FROM microdados_enem 
WHERE SG_UF_ESC = 'SC' AND TP_SIT_FUNC_ESC >= 1
""").show()


#print('média em matemática dos alunos cuja mãe possui PELO MENOS o ensino superior completo, do sexo feminino que estudaram numa escola em Belo Horizonte')
