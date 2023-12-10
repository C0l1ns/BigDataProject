import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window


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


def popular_movies_by_country(context: dict[str, DataFrame]) -> DataFrame:
    akas = context["akas"]
    ratings = context["ratings"]
    window = Window.partitionBy("region").orderBy(f.col("numVotes").desc())

    df = (
        akas.alias("a")
        .join(ratings.alias("r"), f.col("r.tconst") == f.col("a.titleId"))
        .withColumn("rowNumber", f.row_number().over(window))
        .filter(f.col("rowNumber") == 1)
        .drop(f.col("rowNumber"))
        .select(
            f.col("a.region").alias("Region"),
            f.col("r.numVotes").alias("MaxNumberOfVotes"),
            f.col("r.averageRating").alias("AvarageRating"),
        )
    )

    return df


def average_rate_per_genre(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    ratings = context["ratings"]

    df = (
        title_basics.alias("tb")
        .withColumn("genre", f.split("genres", ","))
        .drop("genres")
        .withColumn("genre", f.explode("genre"))
        .filter(f.col("genre") != "\\N")
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy("genre")
        .agg(
            f.avg("r.averageRating").alias("AverageRating"),
            f.count("*").alias("MoviesCount"),
        )
        .select(
            f.col("genre").alias("Genre"),
            f.round("AverageRating", 1).alias("AverageRating"),
            f.col("MoviesCount"),
        )
        .orderBy(f.col("MoviesCount").desc())
    )

    return df


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
            f.col("r.averageRating").alias("rating"),
            f.round("AverageEpisodes", 1).alias("averageEpisodes"),
            f.round("NumberOfEpisodes", 1).alias("numberOfEpisodes"),
        )
        .orderBy(f.desc("r.averageRating"))
    )

    return df


def total_number_of_movies_each_year_per_genre(context: dict[str, DataFrame]):
    # maybe rewrite it to find best movie by total rate for each year by genre
    title_basics = context["title_basics"]

    df = (
        title_basics.alias("tb")
        .withColumn("genre", f.split("genres", ","))
        .drop("genres")
        .withColumn("genre", f.explode("genre"))
        .filter((f.col("genre") != "\\N") & (f.col("StartYear").isNotNull()))
        .groupBy("Genre")
        .pivot("StartYear")
        .count()
    )

    df.write.csv("./report", header=True, mode="overwrite")
