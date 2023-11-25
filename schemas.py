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

TITTLE_BASICS_SCHEMA = t.StructType(
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

TITTLE_EPISODES_SCHEMA = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("parentTconst", t.StringType()),
        t.StructField("seasonNumber", t.IntegerType()),
        t.StructField("episodeNumber", t.IntegerType()),
    ]
)

TITTLE_PRINCIPALS = t.StructType(
    [
        t.StructField("tconst", t.StringType()),
        t.StructField("ordering", t.IntegerType()),
        t.StructField("nconst", t.StringType()),
        t.StructField("category", t.StringType()),
        t.StructField("job", t.StringType()),
        t.StructField("carachers", t.StringType()),
    ]
)

TITLE_RATINGS_SCHEMA = t.StructType(
    [
        t.StructField("tconst", t.StringType(), True),
        t.StructField("averageRating", t.DoubleType(), True),
        t.StructField("numVotes", t.IntegerType(), True),
    ]
)

NAME_BASICS_SCHEMA = t.StructType(
    [
        t.StructField("nconst", t.StringType(), True),
        t.StructField("primaryName", t.StringType(), True),
        t.StructField("birthYear", t.IntegerType(), True),
        t.StructField("deathYear", t.IntegerType(), True),
        t.StructField("primaryProfession", t.ArrayType(t.StringType(), True)),
        t.StructField("knownForTitles", t.ArrayType(t.StringType(), True)),
    ]
)
