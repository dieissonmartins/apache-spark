import os

full_path = os.path.dirname(os.path.realpath(__file__))
full_path_spark = full_path + '/spark/'

# spark-version: spark-3.3.1-bin-hadoop3
os.environ["SPARK_HOME"] = full_path + "/spark/"

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .getOrCreate()

# dados da empresas
path_companies = full_path + '/tmp/empresas'

# carrega dados do csv
companies = spark.read.csv(path_companies, sep=';', inferSchema=True)

# quanlidade de dados
companies_count = companies.count()
print('Quantidade de empresas: ' + str(companies_count))

# tipagem das conlunas
companies.printSchema()

# consulta dados com limite de 100 linhas
companies = companies.limit(10)

# define nome para as colunas
companies_col_names = [
    'cnpj_basico',
    'razao_social_nome_empresarial',
    'natureza_juridica',
    'qualificacao_do_responsavel',
    'capital_social_da_empresa',
    'porte_da_empresa',
    'ente_federativo_responsavel'
]

# renomea colunas
for index, col_name in enumerate(companies_col_names):
    companies = companies.withColumnRenamed(f"_c{index}", col_name)

companies = companies.withColumn('capital_social_da_empresa', f.regexp_replace('capital_social_da_empresa', ',', '.'))
companies = companies.withColumn('capital_social_da_empresa', companies.capital_social_da_empresa.cast(DoubleType()))

# imprime o dataframe de empresas
print(companies.select(
    'razao_social_nome_empresarial',
    'capital_social_da_empresa'
).toPandas())

companies.printSchema()

# dados socios
path_partners = full_path + '/tmp/socios'
partners = spark.read.csv(path_partners, sep=';', inferSchema=True)
partners_count = partners.count()
print('Quantidade de socios: ' + str(partners_count))
partners.printSchema()

print('DataFrame com contagem de colunas no valores nullos')
partners.select([
    f.count(f.when(f.isnull(c), 1)).alias(c) for c in partners.columns
]).show()

# NaN not as number (não é um numero)
# substitui colunas NaN para 0
partners.na.fill(0).limit(10).show()  # para campos de tipos numerico
partners.na.fill('-').limit(10).show()  # para campos de tipos string

# ordenando os dados
# crescente (ascending=True)
# decrescente (ascending=False)
companies \
    .select('razao_social_nome_empresarial', 'capital_social_da_empresa') \
    .orderBy('razao_social_nome_empresarial', ascending=True) \
    .show()

# where com os dados
companies \
    .select('razao_social_nome_empresarial', 'capital_social_da_empresa') \
    .where('capital_social_da_empresa==4000') \
    .orderBy('razao_social_nome_empresarial', ascending=True) \
    .show()

# where string inicia com % startswith()
companies \
    .select('razao_social_nome_empresarial', 'capital_social_da_empresa') \
    .where(companies.razao_social_nome_empresarial.startswith('REGISTRO')) \
    .orderBy('razao_social_nome_empresarial', ascending=True) \
    .show()

# where string finaliza com % endswith()
companies \
    .select('razao_social_nome_empresarial', 'capital_social_da_empresa') \
    .where(companies.razao_social_nome_empresarial.endswith('IMO')) \
    .orderBy('razao_social_nome_empresarial', ascending=True) \
    .show()


# dados estabelecimentos
# path_establishments = full_path + '/tmp/estabelecimentos'
# establishments = spark.read.csv(path_establishments, sep=';', inferSchema=True)
# establishments_count = establishments.count()
# establishments.printSchema()
# print('Quantidade de estabelecimentos: ' + str(establishments_count))
# establishments_pandas = establishments.limit(100).toPandas()
# print(establishments_pandas)
