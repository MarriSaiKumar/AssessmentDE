# Databricks notebook source
import pytest
from pyspark.sql.functions import col, countDistinct

@pytest.fixture(scope="module")
def df_fact():
    return spark.read.table("spark_catalog.silver_tvmaze.fact_table")

def test_required_fields_not_null(df_fact):
    required_fields = ["show_id", "show_name", "season", "episode_name", "runtime"]
    null_counts = df_fact.select([countDistinct(col(f)).alias(f) for f in required_fields]).collect()[0]
    for field in required_fields:
        assert df_fact.filter(col(field).isNull()).count() == 0, f"{field} contains nulls"

def test_runtime_positive(df_fact):
    assert df_fact.filter(col("runtime") <= 0).count() == 0, "runtime must be > 0"

def test_show_name_unique_per_id(df_fact):
    dup_count = (
        df_fact.groupBy("show_id")
        .agg(countDistinct("show_name").alias("unique_names"))
        .filter(col("unique_names") > 1)
        .count()
    )
    assert dup_count == 0, "show_name is not unique per show_id"

# COMMAND ----------


