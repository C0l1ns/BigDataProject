import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def popular_movies(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    ratings = context["ratings"]

    df = (
        title_basics.alias("tb")
        .join(ratings.alias("r"), f.col("r.tconst") == f.col("tb.tconst"))
        .filter(
            (f.col("r.averageRating") > 8.5)
            & (f.col("tb.runtimeMinutes") > 60)
            & (f.col("r.numVotes") > 50000)
            & (f.col("tb.titleType").isin("tvMovie", "movie"))
        )
        .select(
            f.col("tb.primaryTitle").alias("Title"),
            f.col("r.averageRating"),
            f.col("tb.runtimeMinutes"),
            f.col("r.numVotes"),
        )
        .orderBy(f.desc("r.numVotes"))
    )

    return df


def popular_movies_by_country():
    pass


def average_rate_per_genre(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    ratings = context["ratings"]
    title_basics_transformed = (
        title_basics
        .withColumn("genre", f.split("genres", ","))
        .drop("genres")
        .withColumn("genre", f.explode("genre"))
    )

    df = (
        title_basics_transformed.alias("tb")
        .filter(f.col("tb.genre") != "\\N")
        .join(ratings.alias("r"), f.col("r.tconst") == f.col("tb.tconst"))
        .groupBy("tb.genre")
        .avg("r.averageRating")
    )

    return df

# find average ammount of episodes in tv-show's season grouped by it's rating
# also added total number of episodes analyed grouped by rating for comparison
def average_episodes_per_rating(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    episode = context["episode"]
    ratings = context["ratings"]

    df = (
        title_basics.alias("tb")
        .join(episode.alias("e"), f.col("tb.tconst") == f.col("e.parentTconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy("r.averageRating", "tb.tconst", "e.seasonNumber")
        .agg(
            f.avg("e.episodeNumber").alias("AverageEpisodes"),
            f.count("e.episodeNumber").alias("NumberOfEpisodes"),
        )
        .groupBy("r.averageRating")
        .agg(
            f.avg("AverageEpisodes").alias("AverageEpisodes"),
            f.sum("NumberOfEpisodes").alias("NumberOfEpisodes"),
        )
        .select(
            f.col("r.averageRating").alias("AverageRating"),
            f.round("AverageEpisodes", 1).alias("AverageEpisodes"),
            f.round("NumberOfEpisodes", 1).alias("NumberOfEpisodes"),
        )
        .orderBy(f.desc("r.averageRating"))
    )

    return df
