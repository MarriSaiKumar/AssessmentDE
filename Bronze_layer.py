# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
base_path=f"abfss://sourcedata@storageforassessment.dfs.core.windows.net"
bronze_path=f"{base_path}/bronze/raw"
catalog_name="spark_catalog"
schema_name="bronze_tvmaze"
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

def fetch_shows():
    url = "https://api.tvmaze.com/shows"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()
shows_data = fetch_shows()

def fetch_episodes(show_id):
    url = f"https://api.tvmaze.com/shows/{show_id}/episodes"
    response = requests.get(url)
    return response.json()

def fetch_cast(show_id):
    url = f"https://api.tvmaze.com/shows/{show_id}/cast"
    response = requests.get(url)
    return response.json()

episodes_data=[]
cast_data=[]
for show in shows_data[:50]:
    show_id = show["id"]
    try:
        episodes_data.extend(fetch_episodes(show_id))
        cast_data.extend(fetch_cast(show_id))
    except Exception as e:
        print(f"Error fetching episodes for show {show_id}: {e}")
        


# COMMAND ----------

shows_rdd = spark.sparkContext.parallelize([json.dumps(item) for item in shows_data])
df_shows_data = spark.read.json(shows_rdd)

episodes_rdd = spark.sparkContext.parallelize([json.dumps(item) for item in episodes_data])
df_episodes_data = spark.read.json(episodes_rdd)

cast_rdd = spark.sparkContext.parallelize([json.dumps(item) for item in cast_data])
df_cast_data = spark.read.json(cast_rdd)

# COMMAND ----------

df_shows_data.write.mode("overwrite").json(f"{bronze_path}/shows")
df_episodes_data.write.mode("overwrite").json(f"{bronze_path}/episodes")
df_cast_data.write.mode("overwrite").json(f"{bronze_path}/cast")

# COMMAND ----------

df_shows = spark.read.json(f"{bronze_path}/shows")
df_shows.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"bronze_tvmaze.shows")

df_episodes = spark.read.json(f"{bronze_path}/episodes")
df_episodes.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"bronze_tvmaze.episodes")

df_cast = spark.read.json(f"{bronze_path}/cast")
df_cast.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"bronze_tvmaze.cast")