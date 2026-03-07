from ast import expr

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lag, sum, when, coalesce, lit, count, desc
from pyspark.sql.window import Window

def load_lastfm_data(spark: SparkSession, path: str = "data/input/lastfm-dataset-1k/") -> DataFrame:
    """Load lastfm dataset into a Spark DataFrame"""
    df = (spark.read
        .option("delimiter", "\t")
        .csv(f"{path}/userid-timestamp-artid-artname-traid-traname.tsv", 
             header=False, inferSchema=True))
    
    df = (df.withColumnRenamed("_c0", "user_id")
          .withColumnRenamed("_c1", "timestamp")
          .withColumnRenamed("_c2", "artist_id")
          .withColumnRenamed("_c3", "artist_name")
          .withColumnRenamed("_c4", "track_id")
          .withColumnRenamed("_c5", "track_name"))

    return df

def generate_sessions(df: DataFrame) -> DataFrame:
    """Generate sessions - consecutive tracks within 20 minutes"""
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

    df = (df
        .withColumn("prev_timestamp", lag("timestamp").over(window_spec))
        .withColumn("time_diff",
                    coalesce(
                        (col("timestamp").cast("long") - col("prev_timestamp").cast("long")),
                        lit(0))
                        .cast("int"))
        .withColumn("session_id",
                    sum(when(col("time_diff") > 1200, 1).otherwise(0)).over(window_spec))
    )

    return df

def validate_sessions(df: DataFrame) -> DataFrame:
    """Debug: Show session stats"""
    return df.groupBy("user_id", "session_id").agg(count("*").alias("track_count"))
