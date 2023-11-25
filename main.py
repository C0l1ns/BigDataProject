from pyspark.sql import SparkSession

from common.context import populate_context
from reports import popular_movies, average_rate_per_genre


spark = SparkSession.builder.getOrCreate()

context = {}
populate_context(spark, context)

popular_movies(context).show()
average_rate_per_genre(context).show()
