import logging
from pyspark.sql import SparkSession
from src.sessions import load_lastfm_data, generate_sessions, validate_sessions
from src.top_10_songs import get_top_10_songs
from src.forecast_sessions import (
    get_top_user,
    aggregate_session_count,
    prepare_time_series_data,
    forecast_session_count,
    plot_session_count_forecast,
    evaluate_model
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_top_10_songs(df_sessions):
    """Process and save the top 10 songs."""
    logging.info("Processing top 10 songs...")
    top_10 = get_top_10_songs(df_sessions)
    top_10.coalesce(1).write.mode("overwrite").option("header", "true").csv("data/output/top_10_songs")
    logging.info("Top 10 songs saved to 'data/output/top_10_songs'.")

def forecast_top_user_sessions(df_sessions):
    """Forecast session count for the top user."""
    logging.info("Identifying the top user...")
    top_user_id = get_top_user(df_sessions)
    logging.info(f"Top user identified: {top_user_id}")

    logging.info("Aggregating session count over time...")
    aggregated_df = aggregate_session_count(df_sessions, top_user_id)

    logging.info("Preparing data for forecasting...")
    time_series_df = prepare_time_series_data(aggregated_df)

    logging.info("Forecasting session count...")
    model, forecast = forecast_session_count(time_series_df)

    # Filter forecast to include only future dates
    future_forecast = forecast[forecast['ds'] > time_series_df['ds'].max()]

    logging.info("Evaluating the model...")
    evaluate_model(time_series_df, forecast)

    logging.info("Plotting the forecast...")
    plot_session_count_forecast(model, forecast)
    logging.info("Plot saved as 'session_count_forecast.png'")

    logging.info("Saving forecasted data to CSV...")
    future_forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].to_csv("data/output/session_count_forecast.csv", index=False)
    logging.info("Forecast data saved to 'data/output/session_count_forecast.csv'.")

def main():
    """Main entry point for the program."""
    logging.info("Starting the FMDatasetSparkChallenge application...")
    
    # Initialize Spark session
    spark = (SparkSession.builder
             .appName("FMDatasetSparkChallenge")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())
    
    try:
        # Load and process data
        logging.info("Loading raw data...")
        df_raw = load_lastfm_data(spark)
        logging.info("Raw data loaded successfully.")

        logging.info("Generating sessions...")
        df_sessions = generate_sessions(df_raw)
        logging.info("Sessions generated successfully.")

        # Validate sessions (debugging step)
        logging.info("Validating sessions...")
        validate_sessions(df_sessions).show(20)

        # Process top 10 songs
        process_top_10_songs(df_sessions)

        # Forecast session count for the top user
        forecast_top_user_sessions(df_sessions)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        spark.stop()
        logging.info("Spark session stopped. Application finished.")

if __name__ == "__main__":
    main()
