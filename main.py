from pyspark.sql import SparkSession
from src.sessions import load_lastfm_data, generate_sessions, validate_sessions
from src.top_10_songs import get_top_10_songs

def main():
    spark = SparkSession.builder.appName("FMDatasetSparkChallenge").getOrCreate()
    
    df_raw = load_lastfm_data(spark)
    df_sessions = generate_sessions(df_raw)

    validate_sessions(df_sessions).show(20)  # Show the first 20 rows of the validated sessions
    
    top_10 = get_top_10_songs(df_sessions)
    top_10.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/top_10_songs")

if __name__ == "__main__":
    main()