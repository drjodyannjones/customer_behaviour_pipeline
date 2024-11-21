import os
from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import shutil
from urllib.parse import urlparse


def download_dataset(dataset_name, file_name, config_dir, data_path):
    try:
        os.environ["KAGGLE_CONFIG_DIR"] = config_dir
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_file(
                dataset=dataset_name,
                file_name=file_name,
                path=os.path.dirname(data_path))
        zip_file_path = f"{data_path}{file_name}.zip"
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(data_path)

        for file in os.listdir(data_path):
            if file.endswith("zip"):
                file_no_longer_needed = os.path.join(data_path, file)
                os.remove(file_no_longer_needed)
    except Exception as e:
        print(f"Error downlowing dataset: {e}")


def upload_to_gcs(source_file_path):
    try:
        google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not google_credentials_path:
            print("Error: GOOGLE_APPLICATION_CREDENTIALS is not set in the .env file.")
            return

        gcs_path = os.getenv("BRONZE_LAYER_PATH")
        if not gcs_path:
            print("Error: GCS_PATH is not set in the .env file.")
            return

        parsed_url = urlparse(gcs_path)
        folder_name = parsed_url.path.lstrip('/')
        dataset_filename = "".join([file for file in os.listdir(source_file_path) if file.endswith("csv")])
        data_source = os.path.join(source_file_path, dataset_filename)
        destination_path = os.path.join(folder_name, dataset_filename)

        project_id = os.getenv("PROJECT_ID")
        gcs_bucket_name = os.getenv("BUCKET_NAME")
        client = storage.Client(project=project_id)
        bucket = client.bucket(gcs_bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(data_source)
        shutil.rmtree(data_source, ignore_errors=True)
    except Exception as e:
        raise Exception(f"Failed to upload file to GCS: {e}")


if __name__ == "__main__":
    dataset_name = os.getenv("KAGGLE_DATASET_NAME")
    file_name = "2019-Oct.csv"
    kaggle_config_dir = os.getenv("KAGGLE_CONFIG_DIR")
    data_path = os.getenv("LOCAL_DATA_PATH")

    download_dataset(dataset_name, file_name, kaggle_config_dir, data_path=data_path)
    upload_to_gcs(data_path)
