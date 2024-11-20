from airflow import DAG
from airflow.operators.python import PythonOperator

# GCP Airflow Connectors
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)


from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
import os
import subprocess
import logging
import json

# Define constants
KAGGLE_CONFIG_PATH = os.getenv("KAGGLE_CONFIG_PATH")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
BRONZE_TABLE = os.getenv("BRONZE_TABLE")
SILVER_TABLE = os.getenv("SILVER_TABLE")
GOLD_TABLE = os.getenv("GOLD_TABLE")

# Default args for the DAG
default_args = {
    "owner": "Jody",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Helper functions
def authenticate_kaggle():
    """Authenticate with Kaggle API."""
    with open(KAGGLE_CONFIG_PATH, "r") as f:
        kaggle_key = json.load(f)
    os.environ["KAGGLE_USERNAME"] = kaggle_key["username"]
    os.environ["KAGGLE_KEY"] = kaggle_key["key"]
    logging.info("Kaggle API authenticated.")

def download_data():
    """Download data from Kaggle."""
    authenticate_kaggle()
    subprocess.run(["kaggle", "datasets", "download", "-d", "dataset-name", "-p", "/tmp"], check=True)
    logging.info("Dataset downloaded successfully.")

def load_to_bronze():
    """Load raw data into Bronze layer in BigQuery."""
    # Example logic for loading to BigQuery
    # Replace with the actual BigQuery load command or library call
    logging.info("Data loaded into Bronze layer.")

def run_dbt_task(target):
    """Run a DBT model."""
    subprocess.run(["dbt", "run", "--target", target, "--project-dir", DBT_PROJECT_DIR], check=True)
    logging.info(f"DBT {target} models executed successfully.")

# DAG definition
with DAG(
    "ecommerce_pipeline_dag",
    default_args=default_args,
    description="End-to-end ecommerce data pipeline",
    schedule_interval="@monthly",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task group: Data Ingestion
    with TaskGroup("data_ingestion") as data_ingestion:
        download_data_task = PythonOperator(
            task_id="download_data",
            python_callable=download_data,
        )
        load_to_bronze_task = PythonOperator(
            task_id="load_to_bronze",
            python_callable=load_to_bronze,
        )
        download_data_task >> load_to_bronze_task

    # Task group: DBT Transformations
    with TaskGroup("dbt_transformations") as dbt_transformations:
        dbt_bronze_to_silver = PythonOperator(
            task_id="bronze_to_silver",
            python_callable=lambda: run_dbt_task("bronze_to_silver"),
        )
        dbt_silver_to_gold = PythonOperator(
            task_id="silver_to_gold",
            python_callable=lambda: run_dbt_task("silver_to_gold"),
        )
        dbt_bronze_to_silver >> dbt_silver_to_gold

    # Task: Validation
    validate_gold = BigQueryCheckOperator(
        task_id="validate_gold_tables",
        sql=f"SELECT COUNT(*) FROM {GOLD_TABLE_PREFIX}most_popular_brand",
        use_legacy_sql=False,
    )

    # Task: Notification
    completion_notification = PythonOperator(
        task_id="completion_notification",
        python_callable=lambda: logging.info("Pipeline completed successfully."),
    )

    # Task dependencies
    data_ingestion >> dbt_transformations >> validate_gold >> completion_notification
