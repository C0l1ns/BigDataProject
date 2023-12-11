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

def most_popular_title_of_each_actor(context: dict[str, DataFrame]) -> DataFrame:
    principals = context["principals"]
    ratings = context["ratings"]
    name_basics = context["name_basics"]
    title_basics = context["title_basics"]

    df = (
        principals.alias("p")
        .filter(
            (f.col("p.category") == "actor")
            | (f.col("p.category") == "actress")
        )
        .join(ratings.alias("r"), f.col("p.tconst") == f.col("r.tconst"))
        .join(name_basics.alias("nb"), f.col("p.nconst") == f.col("nb.nconst"))
        .join(title_basics.alias("tb"), f.col("p.tconst") == f.col("tb.tconst"))
        .groupBy("nb.primaryName", "tb.primaryTitle")
        .agg(
            f.max("r.numVotes").alias("NumVotes"),
        )
        .select(
            f.col("nb.primaryName").alias("Name"),
            f.col("tb.primaryTitle").alias("Title"),
            f.col("NumVotes"),
        )
        .orderBy(f.col("NumVotes").desc())
    )

    return df

def last_film_of_each_director(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    principals = context["principals"]
    name_basics = context["name_basics"]
    window = Window.partitionBy("primaryName").orderBy(f.col("startYear").desc())

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.startYear") < 2024)
        .join(principals.alias("p"), f.col("tb.tconst") == f.col("p.tconst"))
        .join(name_basics.alias("nb"), f.col("nb.nconst") == f.col("p.nconst"))
        .filter(f.col("p.category") == "director")
        .withColumn("rowNumber", f.row_number().over(window))
        .filter(f.col("rowNumber") == 1)
        .drop(f.col("rowNumber"))
        .select(
            f.col("nb.primaryName").alias("DirectorName"),
            f.col("tb.startYear").alias("FilmYear"),
            f.col("tb.primaryTitle").alias("Title"),
        )
        .orderBy(f.col("FilmYear").desc())
    )

    return df


def actors_who_are_younger_thirty(context: dict[str, DataFrame]) -> DataFrame:
    name_basics = context["name_basics"]

    name_basics_transformed = (
        name_basics
        .withColumn("profession", f.split("primaryProfession", ","))
        .drop("primaryProfession")
        .withColumn("profession", f.explode("profession"))
    )
    df = (
        name_basics_transformed
        .filter(
            (f.col("profession") == "actor")
            | (f.col("profession") == "actress")
        )
        .filter(
            (f.col("birthYear") > 1993)
            & (f.col("deathYear").isNotNull())
        )
        .select(
            f.col("primaryName").alias("ActorName"),
            f.col("birthYear").alias("YearOfBirth"),
        )
        .orderBy(f.col("birthYear").desc())
    )

    return df


def titles_available_in_ukraine(context: dict[str, DataFrame]) -> DataFrame:
    akas = context["akas"]

    df = (
        akas
        .filter(f.col("region") == "UA")
        .select(
            f.col("title").alias("Title"),
        )
        .orderBy(f.col("Title").asc())
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