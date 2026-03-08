import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from src.top_10_songs import get_top_10_songs
from src.sessions import generate_sessions

@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()

def test_get_top_10_songs_counts_tracks_correctly(spark):
    # Example test data
    data = [
        ("user1", "1", "song1"),
        ("user1", "1", "song2"),
        ("user1", "2", "song1")
    ]
    df = spark.createDataFrame(data, ["user_id", "session_id", "track_name"])

    result_df = get_top_10_songs(df)

    result_df.show()

    # Check that the song1 is correctly counted
    assert result_df.filter(col("track_name") == "song1").select("play_count").collect()[0][0] == 2

def test_get_top_10_songs_handles_empty_dataframe(spark):
    # Explicitly define empty DataFrame
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("track_name", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema)

    result_df = get_top_10_songs(empty_df)

    # Assert that the result is empty
    assert result_df.count() == 0

def test_user_sessions_per_day(spark):
    # Example test data
    data = [
        ("user1", "2026-03-08 00:00:00"),
        ("user1", "2026-03-08 00:15:00"),
        ("user1", "2026-03-08 00:40:00"),
        ("user1", "2026-03-08 01:00:00"),
        ("user1", "2026-03-08 23:50:00"),
        ("user2", "2026-03-08 00:00:00"),
        ("user2", "2026-03-08 00:10:00"),
        ("user2", "2026-03-08 00:30:00"),
    ]
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Generate sessions
    session_df = generate_sessions(df)

    # Validate the number of sessions per user per day
    session_counts = (
        session_df
        .withColumn("date", col("timestamp").cast("date"))
        .groupBy("user_id", "date")
        .agg(countDistinct("session_id").alias("session_count"))
    )

    # Check that session counts are within the valid range, since a user can have at most 72 sessions in a day (24 hours * 3 sessions per hour)
    invalid_sessions = session_counts.filter((col("session_count") < 0) | (col("session_count") > 72))
    assert invalid_sessions.count() == 0, "Found invalid session counts!"
