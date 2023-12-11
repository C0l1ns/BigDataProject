from pyspark.sql import SparkSession

from common.context import populate_context
from reports import *


spark = SparkSession.builder.getOrCreate()

context = {}
populate_context(spark, context)

popular_movies(context).show()
average_rate_per_genre(context).show()
average_episodes_per_rating(context).show(n=91)
directors_with_most_films(context).show()