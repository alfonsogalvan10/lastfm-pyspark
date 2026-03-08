# FMDatasetSparkChallenge

## Project Overview
This project processes and analyzes the Last.fm dataset using Spark. It generates user sessions, identifies the top 10 songs, and forecasts session counts for the top user.

## Data Handling
The `data/` folder contains input and output data files. These files are excluded from version control to keep the repository lightweight.

### Instructions to Obtain Data
1. Download the dataset from the [Last.fm Dataset](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html).
2. Place the dataset in the `data/input/` folder.

### Output Data
Processed data will be saved in the `data/output/` folder:
- `top_10_songs/`: Contains the top 10 songs played by users.
- `session_count_forecast.csv`: Contains the forecasted session counts for the top user.

## Setup and Usage

### Prerequisites
- Docker Desktop installed on your system.
- VSCode with the Remote - Containers extension (optional, for development).

### Step-by-Step Instructions
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd FMDatasetSparkChallenge
   ```

2. **Prepare the Dataset**:
   - Download the Last.fm dataset and place it in the `data/input/lastfm-dataset-1k/` folder.

3. **Build and Start the Development Container**:
   - Open the project in VSCode.
   - When prompted, reopen the project in the development container.
   - Alternatively, build the container manually:
     ```bash
     docker build -t fmdataset-spark .
     docker run --rm -it fmdataset-spark
     ```

4. **Run the Application**:
   - Inside the container, execute the following command:
     ```bash
     python3 main.py
     ```

5. **View the Results**:
   - The output files will be saved in the `data/output/` folder.

## Assumptions
- **Dataset Format**: The input dataset is assumed to be in the format provided by Last.fm, with tab-separated values.
- **Session Definition**: A session consists of one or more songs played by a user, where each song starts within 20 minutes of the previous song.
- **Forecasting**: The forecasting model assumes that the session count follows a predictable time series pattern. Additionally, the single series allows to convert aggregation to pandas dataframe to fit model in-memory. Otherwise, Pandas UDF would be required to handle full Spark cluster parallelism.

## Things to Improve
If more time were available, the following improvements could be made:
1. **Error Handling**: Add more robust error handling for edge cases, such as missing or malformed data.
2. **Testing**: Expand the test suite to cover more edge cases and validate the forecasting logic.
3. **Performance**: Optimize the Spark jobs for larger datasets, including partitioning and caching strategies.
4. **Visualization**: Enhance the output with more detailed visualizations of the results.
5. **Documentation**: Provide more detailed documentation for each module and function.
6. **Containerization**: Improve the Docker setup to support deployment in production environments.

## License
This project is licensed under the MIT License.
