# transform.py
# PySpark job to process IPL matches and deliveries CSVs into Parquet and compute sample aggregates.
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main(matches_path, deliveries_path, output_path):
    spark = SparkSession.builder.appName("ipl-transform").getOrCreate()

    matches = spark.read.option("header", "true").csv(matches_path)
    deliveries = spark.read.option("header", "true").csv(deliveries_path)

    # Basic cleaning / type casting
    matches_clean = matches.select(
        col("id").cast("int").alias("match_id"),
        col("season").cast("int"),
        col("city"),
        col("date"),
        col("team1"),
        col("team2"),
        col("toss_winner"),
        col("toss_decision"),
        col("result"),
        col("winner"),
        col("player_of_match")
    )

    deliveries_clean = deliveries.select(
        col("match_id").cast("int"),
        col("inning").cast("int"),
        col("bowling_team"),
        col("batting_team"),
        col("bowler"),
        col("batsman"),
        col("batsman_runs").cast("int"),
        col("total_runs").cast("int"),
        col("over").cast("int"),
        col("ball").cast("int"),
        col("dismissal_kind")
    )

    # Write cleaned parquet
    matches_out = f"{output_path}/matches"
    deliveries_out = f"{output_path}/deliveries"
    matches_clean.write.mode("overwrite").parquet(matches_out)
    deliveries_clean.write.mode("overwrite").parquet(deliveries_out)
    print(f"Wrote matches to {matches_out} and deliveries to {deliveries_out}")

    # Sample aggregate: top batsmen by runs
    agg_batsman = deliveries_clean.groupBy("batsman").sum("batsman_runs").withColumnRenamed("sum(batsman_runs)", "total_runs").orderBy(col("total_runs").desc())
    agg_batsman.write.mode("overwrite").parquet(f"{output_path}/agg_batsman")

    # Sample aggregate: team wins per season
    wins = matches_clean.groupBy("season", "winner").count().withColumnRenamed("count", "wins").orderBy("season", "wins")
    wins.write.mode("overwrite").parquet(f"{output_path}/team_wins")

    print("Aggregates written. Done.")
    spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--matches", default="data/raw/matches.csv")
    parser.add_argument("--deliveries", default="data/raw/deliveries.csv")
    parser.add_argument("--output", default="data/processed")
    args = parser.parse_args()
    main(args.matches, args.deliveries, args.output)
