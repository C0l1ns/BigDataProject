from pyspark.sql import SparkSession, Row
from schemas import AKAS_SCHEMA, TITTLE_BASICS_SCHEMA

spark = SparkSession.builder.getOrCreate()

df = (
    spark.read.format("csv")
    .option("delimiter", "\t")
    .option("inferSchema", True)
    .option("header", True)
    .schema(TITTLE_BASICS_SCHEMA)
    .load("./datasets/title_basics.tsv")
)

df.printSchema()
df.show(n=100)
