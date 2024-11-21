import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, length, isnan, when
from google.cloud import storage

# Load environment variables from .env file
load_dotenv()

# Load configurations from .env
project_id = os.getenv("PROJECT_ID")
bucket_name = os.getenv("BUCKET_NAME")
local_data_path = os.getenv("LOCAL_DATA_PATH")
kaggle_dataset_name = os.getenv("KAGGLE_DATASET_NAME")
bronze_layer_path = os.getenv("BRONZE_LAYER_PATH")
google_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
kaggle_config_dir = os.getenv("KAGGLE_CONFIG_DIR")

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials

# Step 1: Download Dataset from Kaggle
def download_kaggle_dataset(kaggle_dataset_name, download_path):
    kaggle_json_path = os.path.join(kaggle_config_dir, "kaggle.json")
    os.environ["KAGGLE_CONFIG_DIR"] = kaggle_config_dir

    if not os.path.exists(kaggle_json_path):
        raise FileNotFoundError("kaggle.json not found in the specified KAGGLE_CONFIG_DIR")

    print(f"Downloading Kaggle dataset {kaggle_dataset_name}...")
    os.system(f"kaggle datasets download -d {kaggle_dataset_name} -p {download_path}")
    os.system(f"unzip -o {download_path}/*.zip -d {download_path}")
    print("Dataset downloaded and unzipped.")

# Step 2: Upload Dataset to GCP Bucket
def upload_to_gcp_bucket(local_path, bucket_name, destination_path):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    for root, _, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            blob_path = os.path.join(destination_path, file)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file)
            print(f"Uploaded {file} to {blob_path}")

# Step 3: Data Validation in PySpark
def validate_data(df):
    summary = {}
    for column in df.columns:
        col_summary = {}

        # Total count of values in the column
        total_count = df.count()
        col_summary["total_count"] = total_count

        # Distinct values and their count
        distinct_values = df.select(column).distinct().count()
        col_summary["distinct_values_count"] = distinct_values

        # Check if column is an ID column
        if "id" in column.lower():
            col_summary["id_distinct_count"] = distinct_values

        # Min and max length of the field
        lengths = df.select(length(col(column)).alias("len"))
        min_length = lengths.agg({"len": "min"}).collect()[0][0]
        max_length = lengths.agg({"len": "max"}).collect()[0][0]
        col_summary["min_length"] = min_length
        col_summary["max_length"] = max_length

        # Count of null, NaN, or empty values
        null_count = df.filter(col(column).isNull()).count()
        nan_count = df.filter(isnan(col(column))).count()
        empty_count = df.filter(col(column) == "").count()
        col_summary["null_count"] = null_count
        col_summary["nan_count"] = nan_count
        col_summary["empty_count"] = empty_count

        # Store the summary for the column
        summary[column] = col_summary

    return summary

# Main Execution
if __name__ == "__main__":
    try:
        # Step 1: Download Dataset
        download_kaggle_dataset(kaggle_dataset_name, local_data_path)

        # Step 2: Upload Dataset to Bronze Layer (GCP)
        upload_to_gcp_bucket(local_data_path, bucket_name, bronze_layer_path)

        # Step 3: Initialize SparkSession
        spark = SparkSession.builder \
            .appName("Data Validation") \
            .getOrCreate()

        # Step 4: Load Dataset from GCP Bronze Layer
        dataset_path = f"{bronze_layer_path}/*.csv"  # Adjust based on your dataset
        df = spark.read.option("header", "true").csv(dataset_path)

        # Step 5: Validate Data
        data_summary = validate_data(df)

        # Step 6: Print the Validation Summary
        for column, stats in data_summary.items():
            print(f"Summary for column '{column}':")
            for stat_name, value in stats.items():
                print(f"  {stat_name}: {value}")
            print()

    except Exception as e:
        print(f"An error occurred: {e}")
