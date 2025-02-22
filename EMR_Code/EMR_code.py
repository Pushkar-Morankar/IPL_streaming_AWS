from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, count
import boto3

# Initialize Spark session
spark = SparkSession.builder.appName("IPLFantasyETL").getOrCreate()

# Define S3 input path
s3_input = "s3a://ipl-fantasy-league-2025-pushkar/row-data/deliveries.csv"

# Read deliveries data
deliveries_df = spark.read.csv(s3_input, header=True, inferSchema=True)

# Calculate batting stats: Sum batsman_runs per match_id and batter
batting_stats = deliveries_df.groupBy("match_id", "batter").agg(
    sum_("batsman_runs").alias("runs")
).withColumnRenamed("batter", "player_id")

# Calculate bowling stats: Count wickets (excluding run outs) per match_id and bowler
bowling_stats = deliveries_df.filter(
    (col("is_wicket") == 1) & (col("dismissal_kind") != "run out")
).groupBy("match_id", "bowler").agg(
    count("*").alias("wickets")
).withColumnRenamed("bowler", "player_id")

# Combine batting and bowling stats with a full outer join
player_stats = batting_stats.join(bowling_stats, ["match_id", "player_id"], "outer")

# Fill missing values with 0
player_stats = player_stats.na.fill({"runs": 0, "wickets": 0})

# Calculate fantasy points: runs + 5 * wickets
player_stats = player_stats.withColumn("points", col("runs") + 5 * col("wickets"))

# Select final columns
player_stats = player_stats.select("match_id", "player_id", "runs", "wickets", "points")

# Collect data for writing to DynamoDB
player_stats_list = player_stats.collect()

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("FantasyPlayerScores")

# Write to DynamoDB in batches
with table.batch_writer() as batch:
    for row in player_stats_list:
        batch.put_item(
            Item={
                "match_id": str(row["match_id"]),  # Convert to string for DynamoDB
                "player_id": row["player_id"],
                "runs": row["runs"],
                "wickets": row["wickets"],
                "points": row["points"]
            }
        )

# Stop Spark session
spark.stop()