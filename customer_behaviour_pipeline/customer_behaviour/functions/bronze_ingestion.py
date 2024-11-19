import os
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_spark_session(app_name: str) -> SparkSession:
    """Initialize and return a SparkSession configured for GCS."""
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5,com.google.guava:guava:31.1-jre") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS")) \
            .getOrCreate()


        # Additional Hadoop configuration for SparkContext
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
        hadoop_conf.set("google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

        logging.info("Spark session initialized successfully.")
        logging.info(f"Spark version: {spark.version}")
        return spark
    except Exception as e:
        logging.error(f"Failed to initialize Spark session: {e}")
        raise


def define_schema() -> StructType:
    """Define the schema for the dataset."""
    return StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
    ])


def download_from_kaggle_if_missing(dataset_name: str, local_path: str):
    """Download dataset from Kaggle if not already present."""
    if not os.path.exists(local_path):
        try:
            logging.info(f"Dataset not found locally. Downloading to {local_path}...")
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_file(
                dataset=dataset_name,
                file_name="2019-Oct.csv",
                path=os.path.dirname(local_path)
            )
            # Unzip the file if necessary
            logging.info(f"Unzipping downloaded file...")
            import zipfile
            zip_file_path = f"{os.path.dirname(local_path)}/2019-Oct.csv.zip"
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(os.path.dirname(local_path))
            os.remove(zip_file_path)  # Remove the zip file after extraction
            logging.info(f"Dataset downloaded and extracted successfully to {local_path}")
        except Exception as e:
            logging.error(f"Failed to download dataset: {e}")
            raise
    else:
        logging.info(f"Dataset already exists locally at {local_path}")


def validate_schema(df, expected_schema):
    """Validate that the DataFrame matches the expected schema."""
    actual_schema = df.schema
    if actual_schema != expected_schema:
        raise ValueError(f"Schema mismatch!\nExpected: {expected_schema}\nActual: {actual_schema}")
    logging.info("Schema validation passed.")


def ingest_data(spark, input_path, output_path, schema):
    """Ingest raw data from local file system and save it to the Bronze layer in GCS."""
    try:
        logging.info(f"Reading data from {input_path}...")
        # Read data from the local file system
        raw_df = spark.read.csv(input_path, schema=schema, header=True)

        # Validate schema
        validate_schema(raw_df, schema)

        # Repartition data for efficient processing
        raw_df = raw_df.repartition(4)

        logging.info(f"Writing data to GCS Bronze layer at {output_path}...")
        # Write data to the Bronze layer in Parquet format
        raw_df.write.format("parquet").mode("overwrite").save(output_path)

        logging.info(f"Data successfully written to Bronze layer at {output_path}")
    except Exception as e:
        logging.error(f"Data ingestion failed: {e}")
        raise


if __name__ == "__main__":
    # Fetch environment variables
    PROJECT_ID = os.getenv("PROJECT_ID")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "./data/2019-Oct.csv")
    KAGGLE_DATASET_NAME = os.getenv("KAGGLE_DATASET_NAME", "mkechinov/ecommerce-behavior-data-from-multi-category-store")
    BRONZE_LAYER_PATH = os.getenv("BRONZE_LAYER_PATH", f"gs://{BUCKET_NAME}/bronze/")

    try:
        # Step 1: Download dataset from Kaggle if missing
        download_from_kaggle_if_missing(KAGGLE_DATASET_NAME, LOCAL_DATA_PATH)

        # Step 2: Initialize Spark session
        spark = get_spark_session("BronzeIngestionJob")

        # Step 3: Define schema
        schema = define_schema()

        # Step 4: Perform data ingestion
        ingest_data(spark, LOCAL_DATA_PATH, BRONZE_LAYER_PATH, schema)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
