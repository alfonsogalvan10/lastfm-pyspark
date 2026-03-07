from pyspark.sql.functions import count, desc
from pyspark.sql import DataFrame

def get_top_10_songs(df: DataFrame) -> DataFrame:
    """Top 10 songs in top 50 longest sessions"""
    top_50_sessions = (df
        .groupBy("user_id", "session_id")
        .agg(count("*").alias("track_count"))
        .orderBy(desc("track_count"))
        .limit(50))
    
    top_session_songs = df.join(
        top_50_sessions.select("user_id", "session_id"), 
        ["user_id", "session_id"], 
        "inner"
    )
    
    return (top_session_songs
        .groupBy("track_name")
        .agg(count("*").alias("play_count"))
        .orderBy(desc("play_count"))
        .limit(10))
