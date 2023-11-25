from pyspark.sql import SparkSession, Row


spark = SparkSession.builder.getOrCreate()

data = [Row(name="Test", x=1)]

df = spark.createDataFrame(data)

df.show()