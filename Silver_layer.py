# Databricks notebook source
base_path=f"abfss://sourcedata@storageforassessment.dfs.core.windows.net"
bronze_path=f"{base_path}/bronze/raw"

# COMMAND ----------

df_shows = spark.read.table("spark_catalog.bronze_tvmaze.shows")
df_episodes = spark.read.table("spark_catalog.bronze_tvmaze.episodes")
df_cast = spark.read.table("spark_catalog.bronze_tvmaze.cast")


# COMMAND ----------

from pyspark.sql.functions import explode,col

silver_shows = df_shows.select(
    col("id").alias("show_id"),
    col("name").alias("show_name"),
    col("language"),
    col("rating.average").alias("rating_avg"),
    explode("genres").alias("genre"),
    col("type").alias("show_type"),
    col("url"),
    col("weight"),
    col("premiered")
).fillna({
    "rating_avg":0,
    "genre": "N/A",
    "url": "N/A",
    "weight":0,
}).dropDuplicates(["show_id"])


silver_episodes = df_episodes.select(
    col("id").alias("show_id"),
    col("name").alias("episode_name"),
    col("season"),
    col("number"),
    col("airdate"),
    col("runtime")
).fillna({
    "runtime":0,
    "episode_name": "N/A"
}).dropDuplicates(["show_id"])

silver_cast = df_cast.select(
    col("person.id").alias("show_id"),
    col("person.name").alias("cast_name"),
    col("person.country.name").alias("person_country"),
    col("character.id").alias("character_id"),
    col("character.name").alias("character_name")
).fillna({
    "cast_name": "N/A",
    "character_name": "N/A",
    "person_country": "N/A"
}).dropDuplicates(["show_id"])

# COMMAND ----------

silver_shows.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver_tvmaze.shows")
silver_episodes.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver_tvmaze.episodes")
silver_cast.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("silver_tvmaze.cast")

# COMMAND ----------

from pyspark.sql.functions import broadcast

fact_df = silver_episodes \
    .join(broadcast(silver_shows), "show_id") \
    .join(silver_cast, "show_id")

# COMMAND ----------

fact_df = fact_df.select(
    "show_id",
    "show_name",
    "episode_name",
    "rating_avg",
    "language",
    "genre",
    "season",
    "airdate",
    "runtime",
    "cast_name",
    "character_name"
)
#display(fact_df)

# COMMAND ----------

fact_df.write \
    .partitionBy("season") \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true")\
    .saveAsTable("silver_tvmaze.fact_table")

# COMMAND ----------

spark.sql("OPTIMIZE silver_tvmaze.shows ZORDER BY (show_id)")
spark.sql("OPTIMIZE silver_tvmaze.episodes ZORDER BY (show_id)")
spark.sql("OPTIMIZE silver_tvmaze.cast ZORDER BY (show_id)")
spark.sql("OPTIMIZE silver_tvmaze.fact_table ZORDER BY (show_id)")