# Databricks notebook source
df_fact = spark.read.table("spark_catalog.silver_tvmaze.fact_table")

# COMMAND ----------

from pyspark.sql.functions import count 

episodes_per_season = (
    df_fact
    .select("show_id", "show_name", "season", "episode_name") 
    .groupBy("show_id", "show_name", "season")
    .agg(count("episode_name").alias("episode_count"))
)

episodes_per_season.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_tvmaze.episodes_per_season")

# COMMAND ----------

avg_runtime = df_fact.select("show_id", "show_name", "season", "episode_name", "runtime")\
    .groupBy("show_id", "show_name")\
    .avg("runtime") \
    .withColumnRenamed("avg(runtime)", "avg_runtime")

avg_runtime.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_tvmaze.avg_runtime")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window = Window.partitionBy("show_id").orderBy(desc("count"))
top_cast = df_fact.groupBy("show_id", "cast_name") \
    .count() \
    .withColumn("rank", row_number().over(window)) \
    .filter("rank <= 3")\
    .drop("rank","count")

top_cast.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_tvmaze.top_cast")


# COMMAND ----------

common_genres = df_fact.groupBy("genre") \
    .count() \
    .orderBy("count", ascending=False)

common_genres.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_tvmaze.common_genres")

