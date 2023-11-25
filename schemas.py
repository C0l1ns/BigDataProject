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
        t.StructField("startYear", t.TimestampType()),
        t.StructField("endYear", t.TimestampType()),
        t.StructField("runtimeMinutes", t.DoubleType()),
        t.StructField("genres", t.StringType()),
    ]
)
