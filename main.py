from src.utils.pyspark import Pyspark

spark = Pyspark().init()

data = [
    {'data': 20221213},
    {'data': 20221214},
    {'data': 20221215},
    {'data': 20221216}
]

# dados dicionario com datas
df = spark.createDataFrame(data)

# formato original
df.show()

df.printSchema()
