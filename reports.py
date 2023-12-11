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


def popular_movies_by_country(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]

    df = (
        title_basics.alias("tb")

    )


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

def directors_with_most_films(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]

    crew_transformed = (
        crew
        .withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .groupBy("nb.nconst", "nb.primaryName", "nb.birthYear")
        .agg(f.count("tb.tconst").alias("NumberOfFilms"))
        .orderBy(f.desc("NumberOfFilms"))
        .select(
            f.col("nb.primaryName").alias("DirectorName"),
            f.col("nb.birthYear").alias("YearOfBirth"),
            f.col("NumberOfFilms")
        )
    )

    return df