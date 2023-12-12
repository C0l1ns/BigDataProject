from pyspark.sql import SparkSession

from common.context import populate_context
from reports import *


spark = SparkSession.builder.getOrCreate()

context = {}
populate_context(spark, context)

# popular_movies(context).show()
# total_number_of_movies_each_year_per_genre(context)
# top_genres_over_time(context)
# directors_best_titles(context)

# average_rate_per_genre(context).show()
# popular_movies_by_country(context).show()
# most_popular_title_of_each_actor(context).show()
# last_film_of_each_director(context).show()
# actors_who_are_younger_thirty(context).show()
# titles_available_in_ukraine(context).show()

# average_episodes_per_rating(context).show(n=91)
# directors_with_most_films(context).show()
# films_with_biggest_crew(context).show()
# films_in_each_language(context).show()
# most_popular_directors(context).show()
directors_with_biggest_amount_of_genres(context).show()
