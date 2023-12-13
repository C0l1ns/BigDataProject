import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType


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
        .withColumn("genre", f.explode(f.split("genres", ",")))
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
        .filter(f.col("p.category").isin("actor", "actress"))
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

    df = (
        name_basics.withColumn(
            "profession", f.explode(f.split("primaryProfession", ","))
        )
        .filter(f.col("profession").isin("actor", "actress"))
        .filter((f.col("birthYear") > 1993) & (f.col("deathYear").isNotNull()))
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
        akas.filter(f.col("region") == "UA")
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
        .withColumn("genre", f.explode(f.split("genres", ",")))
        .filter((f.col("genre") != "\\N") & (f.col("StartYear").isNotNull()))
        .groupBy("Genre")
        .pivot("StartYear")
        .count()
    )

    return df


def directors_with_most_films(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]

    crew_transformed = crew.withColumn("director", f.explode(f.split("directors", ",")))

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
            f.col("NumberOfFilms"),
        )
    )

    return df


def top_genres_over_time(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    ratings = context["ratings"]
    window = Window.partitionBy("startYear").orderBy(f.desc("GenreCount"))

    most_popular_genres_over_time = (
        title_basics.withColumn("Genre", f.explode(f.split("genres", ",")))
        .filter(
            (f.col("Genre") != "\\N")
            & (f.col("startYear").isNotNull())
            & (f.col("StartYear") <= f.year(f.current_date()))
        )
        .groupBy("startYear", "Genre")
        .agg(f.count("*").alias("GenreCount"))
        .withColumn("GenreRank", f.row_number().over(window))
        .filter(f.col("GenreRank") == 1)
        .select(f.col("startYear").alias("StartYear"), "Genre")
        .orderBy(f.col("StartYear").desc())
    )

    top_rated_genres_each_year = (
        title_basics.alias("tb")
        .withColumn("Genre", f.split("genres", ",")[0])
        .filter(f.col("Genre") != "\\N")
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy(f.col("tb.startYear"), f.col("Genre"))
        .agg(f.max("r.numVotes").alias("MaxNumVotes"))
    )

    df = (
        most_popular_genres_over_time.alias("p")
        .join(
            top_rated_genres_each_year.alias("t"),
            (f.col("p.StartYear") == f.col("t.StartYear"))
            & (f.col("p.Genre") == f.col("t.Genre")),
        )
        .select(f.col("p.StartYear"), f.col("p.Genre"), f.col("MaxNumVotes"))
        .orderBy(f.col("StartYear").desc())
    )

    return df


def directors_best_titles(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]
    crew = context["crew"]

    window = Window.partitionBy("nconst").orderBy(f.col("NumVotes").desc())
    df = (
        ratings.alias("r")
        .join(crew.alias("c"), f.col("r.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nm"), f.col("c.directors") == f.col("nm.nconst"))
        .join(title_basics.alias("tb"), f.col("tb.tconst") == f.col("r.tconst"))
        .withColumn("TopVoted", f.row_number().over(window))
        .filter(f.col("TopVoted") == 1)
        .drop("TopVoted")
        .select(
            f.col("nm.primaryName").alias("FullName"),
            f.col("nm.primaryProfession").alias("PrimaryProfession"),
            f.col("tb.primaryTitle").alias("MostRatedTittle"),
            f.col("r.averageRating").alias("Rating"),
            f.col("tb.startYear").alias("ReleaseDate"),
        )
    )

    return df


def films_with_biggest_crew(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    ratings = context["ratings"]

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .filter(f.col("r.numVotes") > 1000)
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .groupBy("tb.tconst", "tb.primaryTitle")
        .agg(f.countDistinct("c.director").alias("CrewSize"))
        .orderBy(f.desc("CrewSize"))
        .select(
            f.col("tb.primaryTitle").alias("Title"),
            f.col("CrewSize"),
        )
    )


def last_and_least_successful_film_of_each_director(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    window_last_film = Window.partitionBy("nconst").orderBy(f.col("startYear").desc())
    window_least_successful_film = Window.partitionBy("nconst").orderBy(f.col("numVotes").asc())

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .filter(f.col("tb.startYear") > 1000)
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .withColumn("rowNumber1", f.row_number().over(window_last_film))
        .filter(f.col("rowNumber1") == 1)
        .withColumn("rowNumber2", f.row_number().over(window_least_successful_film))
        .filter(f.col("rowNumber2") == 1)
        .select(
            f.col("nb.primaryName").alias("DirectorName"),
            f.col("nb.birthYear").alias("YearOfBirth"),
            f.col("tb.startYear").alias("YearOfFilm"),
            f.col("r.numVotes").alias("NumberOfVotes"),
        )
    )

    return df


def most_popular_directors(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy("nb.nconst", "nb.primaryName")
        .agg(f.sum("r.numVotes").alias("NumberOfReviews"))
        .orderBy(f.desc("NumberOfReviews"))
        .select(
            f.col("nb.primaryName"),
            f.col("NumberOfReviews"),
        )
    )

    return df


def directors_with_biggest_amount_of_genres(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]

    title_basics_transformed = (
        title_basics.withColumn("genre", f.split("genres", ","))
        .drop("genres")
        .withColumn("genre", f.explode("genre"))
    )

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics_transformed.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy("nb.nconst", "nb.primaryName")
        .agg(f.countDistinct("tb.genre").alias("NumberOfGenres"))
        .orderBy(f.desc("NumberOfGenres"))
        .select(
            f.col("nb.primaryName"),
            f.col("NumberOfGenres"),
        )
    )

    return df


def actor_analytics(context: dict[str, DataFrame]) -> DataFrame:
    name_basics = context["name_basics"].alias("nb")
    principals = context["principals"].alias("p")
    title_basics = (
        context["title_basics"]
        .alias("tb")
        .withColumn("Genre", f.explode(f.split("genres", ",")))
    )
    ratings = context["ratings"].alias("r")
    window = Window.partitionBy("Genre").orderBy(f.col("AverageRating").desc())

    df = (
        name_basics.join(principals, f.col("nb.nconst") == f.col("p.nconst"))
        .join(title_basics, f.col("p.tconst") == f.col("tb.tconst"))
        .join(ratings, f.col("tb.tconst") == f.col("r.tconst"))
        .filter((f.col("p.category") == "actor") & (f.col("tb.titleType") == "movie"))
        .filter(f.col("Genre") != "\\N")
        .groupBy("nb.primaryName", "Genre")
        .agg(
            f.countDistinct("tb.tconst").alias("NumberOfTitles"),
            f.round(f.avg("r.averageRating"), 2).alias("AverageRating"),
            f.collect_list("tb.genres").alias("GenresList"),
        )
        .withColumn("GenreRank", f.rank().over(window))
        .orderBy(f.col("NumberOfTitles").desc())
        .withColumn("GenresList", f.col("GenresList").cast(StringType()))
    )

    return df


def films_with_biggest_crew(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    ratings = context["ratings"]

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .filter(f.col("r.numVotes") > 1000)
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .groupBy("tb.tconst", "tb.primaryTitle")
        .agg(f.countDistinct("c.director").alias("CrewSize"))
        .orderBy(f.desc("CrewSize"))
        .select(
            f.col("tb.primaryTitle").alias("Title"),
            f.col("CrewSize"),
        )
    )

    return df


def last_and_least_successful_film_of_each_director(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    window_last_film = Window.partitionBy("nconst").orderBy(f.col("startYear").desc())
    window_least_successful_film = Window.partitionBy("nconst").orderBy(f.col("numVotes").asc())

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .filter(f.col("tb.startYear") > 1000)
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .withColumn("rowNumber1", f.row_number().over(window_last_film))
        .filter(f.col("rowNumber1") == 1)
        .withColumn("rowNumber2", f.row_number().over(window_least_successful_film))
        .filter(f.col("rowNumber2") == 1)
        .select(
            f.col("nb.primaryName").alias("DirectorName"),
            f.col("nb.birthYear").alias("YearOfBirth"),
            f.col("tb.startYear").alias("YearOfFilm"),
            f.col("r.numVotes").alias("NumberOfVotes"),
        )
    )

    return df


def most_popular_directors(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy("nb.nconst", "nb.primaryName")
        .agg(f.sum("r.numVotes").alias("NumberOfReviews"))
        .orderBy(f.desc("NumberOfReviews"))
        .select(
            f.col("nb.primaryName"),
            f.col("NumberOfReviews"),
        )
    )

    return df


def directors_with_biggest_amount_of_genres(context: dict[str, DataFrame]) -> DataFrame:
    title_basics = context["title_basics"]
    crew = context["crew"]
    name_basics = context["name_basics"]
    ratings = context["ratings"]

    title_basics_transformed = (
        title_basics.withColumn("genre", f.split("genres", ","))
        .drop("genres")
        .withColumn("genre", f.explode("genre"))
    )

    crew_transformed = (
        crew.withColumn("director", f.split("directors", ","))
        .drop("directors")
        .withColumn("director", f.explode("director"))
    )

    df = (
        title_basics_transformed.alias("tb")
        .filter(f.col("tb.titleType").isin("tvMovie", "movie"))
        .join(crew_transformed.alias("c"), f.col("tb.tconst") == f.col("c.tconst"))
        .join(name_basics.alias("nb"), f.col("c.director") == f.col("nb.nconst"))
        .join(ratings.alias("r"), f.col("tb.tconst") == f.col("r.tconst"))
        .groupBy("nb.nconst", "nb.primaryName")
        .agg(f.countDistinct("tb.genre").alias("NumberOfGenres"))
        .orderBy(f.desc("NumberOfGenres"))
        .select(
            f.col("nb.primaryName"),
            f.col("NumberOfGenres"),
        )
    )

    return df