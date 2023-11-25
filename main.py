from pyspark.sql import SparkSession

from utils.context import populate_context
from reports import popular_movies


spark = SparkSession.builder.getOrCreate()

context = {}
populate_context(spark, context)

popular_movies(context).show()
