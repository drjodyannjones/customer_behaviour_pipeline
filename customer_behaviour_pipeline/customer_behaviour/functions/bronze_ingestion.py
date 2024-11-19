import os
import tempfile
import subprocess
from google.cloud import storage
import logging
from kaggle.api.kaggle_api_extended import KaggleApi


kaggle_dataset_identifier = os.getenv('KAGGLE_DATASET_NAME')
bucket = os.getenv('BUCKET_NAME')


def get_and_save_data_from_kaggle(dataset_name: str, file_name: None, download_path):
    try:
        download_path = tempfile.mkdtemp()
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_file(
            dataset=dataset_name,
            file_name=file_name,
            path=download_path)
        logging.info(f"Downloading data to directory {download_path}")
        return download_path
    except subprocess.CalledProcessError as e:
        logging.error(f"Error downlowing dataset: {e}")


if __name__ == "__main__":
    get_and_save_data_from_kaggle(kaggle_dataset_identifier, "2019-Oct.csv")


# def download_from_kaggle_if_missing(dataset_name: str, local_path: str):
#     """Download dataset from Kaggle if not already present."""
#     if not os.path.exists(local_path):
#         try:
#             logging.info(f"Dataset not found locally. Downloading to {local_path}...")
#             api = KaggleApi()
#             api.authenticate()
#             api.dataset_download_file(
#                 dataset=dataset_name,
#                 file_name="2019-Oct.csv",
#                 path=os.path.dirname(local_path)
#             )
#             # Unzip the file if necessary
#             logging.info(f"Unzipping downloaded file...")
#             import zipfile
#             zip_file_path = f"{os.path.dirname(local_path)}/2019-Oct.csv.zip"
#             with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
#                 zip_ref.extractall(os.path.dirname(local_path))
#             os.remove(zip_file_path)  # Remove the zip file after extraction
#             logging.info(f"Dataset downloaded and extracted successfully to {local_path}")
#         except Exception as e:
#             logging.error(f"Failed to download dataset: {e}")
#             raise
#     else:
#         logging.info(f"Dataset already exists locally at {local_path}")
