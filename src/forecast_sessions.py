import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import countDistinct, desc, date_trunc
from prophet import Prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

def get_top_user(df_sessions: DataFrame) -> str:
    """Get the user with the highest number of sessions."""
    top_user = (df_sessions
                .groupBy("user_id")
                .agg(countDistinct("session_id").alias("session_count"))
                .orderBy(desc("session_count"))
                .limit(1)
                .collect()[0]["user_id"])
    return top_user

def aggregate_session_count(df_sessions: DataFrame, top_user_id: str, time_granularity: str = "day") -> DataFrame:
    """Aggregate session count over time for the top user."""
    return (df_sessions
            .filter(df_sessions.user_id == top_user_id)
            .withColumn("time_period", date_trunc(time_granularity, "timestamp"))
            .groupBy("time_period")
            .agg(countDistinct("session_id").alias("session_count"))
            .orderBy("time_period"))

def prepare_time_series_data(aggregated_df: DataFrame):
    """Prepare and clean time series data for forecasting."""
    logger = logging.getLogger(__name__)
    
    # Convert to Pandas DataFrame
    time_series_df = aggregated_df.toPandas().rename(columns={"time_period": "ds", "session_count": "y"})
    
    # Log data overview and summary statistics
    logger.info("Time Series Data Overview:")
    logger.info(time_series_df.info())
    logger.info("Summary Statistics:")
    logger.info(time_series_df.describe())
    
    # Check for missing values
    if time_series_df.isnull().values.any():
        logger.warning("Missing values detected. Filling missing values with forward fill...")
        time_series_df = time_series_df.fillna(method='ffill')  # Forward fill missing values
    
    # Log potential outliers without removing them
    Q1 = time_series_df['y'].quantile(0.25)
    Q3 = time_series_df['y'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = time_series_df[(time_series_df['y'] < lower_bound) | (time_series_df['y'] > upper_bound)]
    logger.info(f"Number of potential outliers detected: {len(outliers)}")
    if not outliers.empty:
        logger.info("Potential outliers detected:")
        logger.info(outliers)
    
    return time_series_df

def forecast_session_count(time_series_df):
    """Forecast session count using Facebook Prophet with linear growth."""
    # Ensure the floor is set to 0, since session counts cannot be negative
    time_series_df['floor'] = 0

    # Initialize the Prophet model with linear growth
    model = Prophet(growth="linear")
    
    # Fit the model
    model.fit(time_series_df)
    
    # Create forecast dataframe for 3 months
    future = model.make_future_dataframe(periods=90)
    future['floor'] = 0  # Add the floor to the future dataframe
    
    # Generate the forecast
    forecast = model.predict(future)
    
    return model, forecast

def evaluate_model(time_series_df, forecast):
    """Evaluate the model using historical data."""
    logger = logging.getLogger(__name__)

    # Filter forecast to include only historical dates
    historical_forecast = forecast[forecast['ds'] <= time_series_df['ds'].max()]

    # Align on 'ds' column to ensure consistency
    merged = time_series_df.merge(historical_forecast[['ds', 'yhat']], on='ds', how='inner')

    # Validate data
    if merged.empty:
        logger.error("No overlapping data between time_series_df and forecast for evaluation.")
        raise ValueError("No overlapping data between time_series_df and forecast for evaluation.")

    if len(merged['y']) != len(merged['yhat']):
        logger.error("Mismatch in lengths of actual and predicted values.")
        raise ValueError("Mismatch in lengths of actual and predicted values.")

    # Calculate evaluation metrics
    mae = mean_absolute_error(merged['y'], merged['yhat'])
    rmse = np.sqrt(mean_squared_error(merged['y'], merged['yhat']))

    # Log the evaluation metrics
    logger.info(f"Model Evaluation - MAE: {mae:.2f}, RMSE: {rmse:.2f}")

    return mae, rmse

def plot_session_count_forecast(model, forecast):
    """Plot the forecasted session count and save as an image."""
    model.plot(forecast)
    plt.title("Session Count Forecast")
    plt.savefig("data/output/session_count_forecast.png")
