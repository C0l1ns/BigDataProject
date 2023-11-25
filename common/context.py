import os
from pyspark.sql import SparkSession
from common.schemas import (
    AKAS_SCHEMA,
    TITLE_BASICS_SCHEMA,
    RATINGS_SCHEMA,
    CREW_SCHEMA,
    EPISODES_SCHEMA,
    NAME_BASICS_SCHEMA,
    PRINCIPALS,
)


def load_dataframe(spark: SparkSession, schema, path, base_path="./datasets/"):
    return (
        spark.read.format("csv")
        .option("delimiter", "\t")
        .option("header", True)
        .schema(schema)
        .load(os.path.join(base_path, path))
    )


def populate_context(spark, context):
    context["akas"] = load_dataframe(spark, AKAS_SCHEMA, "akas.tsv")
    context["crew"] = load_dataframe(spark, CREW_SCHEMA, "crew.tsv")
    context["episode"] = load_dataframe(spark, EPISODES_SCHEMA, "episode.tsv")
    context["name_basics"] = load_dataframe(
        spark, NAME_BASICS_SCHEMA, "name_basics.tsv"
    )
    context["principals"] = load_dataframe(spark, PRINCIPALS, "principals.tsv")
    context["ratings"] = load_dataframe(spark, RATINGS_SCHEMA, "ratings.tsv")
    context["title_basics"] = load_dataframe(
        spark, TITLE_BASICS_SCHEMA, "title_basics.tsv"
    )
