import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from src.top_10_songs import get_top_10_songs

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
