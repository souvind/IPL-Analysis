# Databricks notebook source
# MAGIC %md
# MAGIC # IPL Data Transform Notebook
# MAGIC This notebook reads raw matches and deliveries CSVs, cleans, and writes Parquet outputs.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Paths (update as needed)
matches_path = "dbfs:/FileStore/ipl/matches.csv"
deliveries_path = "dbfs:/FileStore/ipl/deliveries.csv"
output_path = "dbfs:/FileStore/ipl/processed"

# COMMAND ----------

# DBTITLE 1,Read CSVs
matches = spark.read.option("header","true").csv(matches_path)
deliveries = spark.read.option("header","true").csv(deliveries_path)
display(matches.limit(5))
display(deliveries.limit(5))

# COMMAND ----------

# DBTITLE 1,Clean & Write Parquet
matches_clean = matches.select(col("id").cast("int").alias("match_id"), col("season").cast("int"), "team1", "team2", "winner", "player_of_match")
matches_clean.write.mode("overwrite").parquet(output_path + "/matches")
deliveries.selectExpr("cast(match_id as int) as match_id", "batsman", "batsman_runs").write.mode("overwrite").parquet(output_path + "/deliveries")

print("Done.")
