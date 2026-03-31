# Databricks notebook source
base_path=f"abfss://sourcedata@storageforassessment.dfs.core.windows.net"
bronze_path=f"{base_path}/bronze/raw"
silver_path = f"{base_path}/silver"
gold_path= f"{silver_path}/gold"
catalog_name="spark_catalog"
#schema_name="silver_tvmaze"

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS bronze_tvmaze
    LOCATION '{base_path}/bronze'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.bronze_tvmaze.shows
    USING DELTA
    LOCATION '{bronze_path}/table/table_shows'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.bronze_tvmaze.episodes
    USING DELTA
    LOCATION '{bronze_path}/table/table_episodes'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.bronze_tvmaze.cast
    USING DELTA
    LOCATION '{bronze_path}/table/table_cast'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS silver_tvmaze
    LOCATION '{base_path}/silver'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.silver_tvmaze.shows
    USING DELTA
    LOCATION '{silver_path}/table/table_shows'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.silver_tvmaze.episodes
    USING DELTA
    LOCATION '{silver_path}/table/table_episodes'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.silver_tvmaze.cast
    USING DELTA
    LOCATION '{silver_path}/table/table_cast'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.silver_tvmaze.fact_table
    USING DELTA
    LOCATION '{silver_path}/table/fact'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS gold_tvmaze
    LOCATION '{base_path}/Gold'
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold_tvmaze.episodes_per_season
    USING DELTA
    LOCATION '{gold_path}/table/episodes_per_season'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold_tvmaze.avg_runtime
    USING DELTA
    LOCATION '{gold_path}/table/avg_runtime'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold_tvmaze.top_cast
    USING DELTA
    LOCATION '{gold_path}/table/top_cast'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold_tvmaze.common_genres
    USING DELTA
    LOCATION '{gold_path}/table/common_genres'
""")