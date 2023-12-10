from pyspark.sql import SparkSession

from common.context import populate_context
from reports import *


spark = SparkSession.builder.getOrCreate()

context = {}
populate_context(spark, context)

# popular_movies(context).show()
# popular_movies_by_country(context).show()
# average_rate_per_genre(context).show()
# average_episodes_per_rating(context).show()
total_number_of_movies_each_year_per_genre(context)
