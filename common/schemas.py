import pyspark.sql.types as t

AKAS_SCHEMA = t.StructType(
    [
        t.StructField("titleId", t.StringType()),
        t.StructField("ordering", t.IntegerType()),
        t.StructField("title", t.StringType()),
        t.StructField("region", t.StringType()),
        t.StructField("language", t.StringType()),
        t.StructField("types", t.StringType()),
        t.StructField("attributes", t.StringType()),
        t.StructField("isOriginalTittle", t.BooleanType()),
    ]
)

TITLE_BASICS_SCHEMA = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("titleType", t.StringType()),
        t.StructField("primaryTitle", t.StringType()),
        t.StructField("originalTitle", t.StringType()),
        t.StructField("isAdult", t.BooleanType()),
        t.StructField("startYear", t.IntegerType()),
        t.StructField("endYear", t.IntegerType()),
        t.StructField("runtimeMinutes", t.DoubleType()),
        t.StructField("genres", t.StringType()),
    ]
)

CREW_SCHEMA = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("directors", t.StringType()),
        t.StructField("writers", t.StringType()),
    ]
)

EPISODES_SCHEMA = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("parentTconst", t.StringType()),
        t.StructField("seasonNumber", t.IntegerType()),
        t.StructField("episodeNumber", t.IntegerType()),
    ]
)

PRINCIPALS = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("ordering", t.IntegerType()),
        t.StructField("nconst", t.StringType()),
        t.StructField("category", t.StringType()),
        t.StructField("job", t.StringType()),
        t.StructField("characters", t.StringType()),
    ]
)

RATINGS_SCHEMA = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("averageRating", t.DoubleType()),
        t.StructField("numVotes", t.IntegerType()),
    ]
)

NAME_BASICS_SCHEMA = t.StructType(
    [
        t.StructField("nconst", t.StringType()),
        t.StructField("primaryName", t.StringType()),
        t.StructField("birthYear", t.IntegerType()),
        t.StructField("deathYear", t.IntegerType()),
        t.StructField("primaryProfession", t.StringType()),
        t.StructField("knownForTitles", t.StringType()),
    ]
)
