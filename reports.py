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
        .join(ratings.alias("r"), f.col("r.tconst") == f.col("tb.tconst"))
        .groupBy("tb.genre")
        .avg("r.averageRating")
        .filter(f.col() != "\N")
    )

    return df
