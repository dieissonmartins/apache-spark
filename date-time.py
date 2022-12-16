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

data = [
    {'data': 20221213},
    {'data': 20221214},
    {'data': 20221215},
    {'data': 20221216}
]

# dados dicionario com datas
df = spark.createDataFrame(data)

# formato original
df.printSchema()

# formata data para padraodate
df = df \
    .withColumn(
    'data',
    f.to_date(df.data.cast(StringType()), 'yyyyMMdd')
)

# formato novo
df.printSchema()

# mostra do tipo pandas
print(df.toPandas())
