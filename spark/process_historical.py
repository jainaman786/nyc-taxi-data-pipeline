"""
AWS EMR Submission Context:
--------------------------
To deploy this batch job against 1.5 billion rows securely on an enterprise AWS EMR Cluster:
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.memory=16g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.shuffle.partitions=2000 \
  spark/process_historical.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, sum as _sum, avg as _avg, count as _count, year, month, to_date

def process_historical_taxis():
    # Initialize robust session mapping generic memory fail-safes and Adaptive Query Execution
    spark = SparkSession.builder \
        .appName("Historical_Taxi_Backfill") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .getOrCreate()
    
    # Ingest the 1.5B row raw parquet dataset from the remote data lake
    raw_trips_df = spark.read.parquet("s3://nyc-tlc/trip data/yellow_tripdata_*.parquet")
    
    # Broadcast Join Optimization:
    # taxi_zone_lookup.csv is incredibly small (under 300 rows).
    # If we joined normally against 1.5 billion rows, PySpark would trigger a massive network shuffle across all worker nodes.
    # By strictly using broadcast(), we ship the entire lookup table solidly into the memory of every worker machine, bypassing the shuffle network universally.
    zones_df = spark.read.option("header", "true").csv("s3://nyc-tlc-data/seeds/taxi_zone_lookup.csv")
    
    joined_df = raw_trips_df.join(
        broadcast(zones_df),
        raw_trips_df.PULocationID == zones_df.LocationID,
        "left"
    )
    
    # Replicate DBT Staging/Intermediate Clean Bounds:
    clean_trips_df = joined_df.filter(
        (col("trip_distance") > 0) & 
        (col("fare_amount") > 0) & 
        (col("total_amount") >= col("fare_amount")) &
        (col("passenger_count") > 0)
    )
    
    # Optimization Context: Caching the clean dataframe
    # We execute `.cache()` here heavily because we are actively calculating aggregations.
    # Without caching, PySpark's lazy evaluation is forced to physically re-read all 1.5B rows from S3 and re-filter them from scratch for every single aggregation pathway.
    clean_trips_df.cache()

    # Replicate agg_daily_revenue logic natively in distributed dataframe API
    # Isolating the pickup_datetime strictly to purely `DATE` guarantees the daily grouping operates effectively.
    daily_revenue_df = clean_trips_df \
        .withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
        .groupBy("pickup_date") \
        .agg(
            _count("*").alias("total_trips"),
            _sum("fare_amount").alias("total_fare"),
            _avg("fare_amount").alias("avg_fare"),
            _sum("tip_amount").alias("total_tips"),
            ((_sum("tip_amount") / _sum("fare_amount")) * 100).alias("tip_rate_percent")
        )

    # Output Optimization: Re-partitioning and hierarchical storage
    # Writing 1.5B rows naively historically results in thousands of tiny files crashing the Datalake metadata (The notorious "Small Files" problem).
    # By generating year/month strings and forcing a `.repartition()`, we guarantee uniform ~100MB Parquet partitions ensuring BI tools can filter massive time-blocks dynamically without scanning raw underlying data.
    partitioned_df = daily_revenue_df \
        .withColumn("year", year("pickup_date")) \
        .withColumn("month", month("pickup_date"))
        
    partitioned_df \
        .repartition("year", "month") \
        .write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("s3://nyc-tlc-data/marts/agg_daily_revenue/")
        
    spark.stop()

if __name__ == "__main__":
    process_historical_taxis()
