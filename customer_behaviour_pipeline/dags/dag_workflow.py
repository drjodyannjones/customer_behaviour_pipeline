from airflow import DAG
from customer_behaviour_pipeline.customer_behaviour.functions.bronze_ingestion import download_dataset, upload_to_gcs
import pendulum
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.task_group import TaskGroup
import os


def get_previous_month_filename(execution_date):
    previous_month = execution_date.subtract(months=1)
    previous_month_filename = previous_month.format("YYYY-MMM") + ".csv"
    if previous_month_filename > "2020-Apr.csv":
        raise ValueError(f"No dataset available for {previous_month_filename}. Stopping pipeline.")
    return previous_month_filename


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    'customer_behavior_pipeline',
    description='A pipeline to extract raw data, \
        transform data then aggregate data',
    schedule_interval='@monthly',
    catchup=False,
    default_args=default_args,
    start_date=pendulum.datetime(2019, 11, 1, tz='UTC')
    ) as dag:
        kaggle_dataset_name = os.getenv("KAGGLE_DATASET_NAME")
        kaggle_config_dir = os.getenv("KAGGLE_CONFIG_DIR")
        local_data_path = os.getenv("LOCAL_DATA_PATH")

        with TaskGroup("data_ingestion") as data_ingestion:
            extract_previous_month_filename_task = PythonOperator(
                task_id = 'get_previous_month_filename',
                python_callable=get_previous_month_filename,
                op_kwargs={'execution_date': '{{ execution_date }}'}
            )
            download_data_task = PythonOperator(
                task_id = 'download data from kaggle',
                python_callable=download_dataset,
                 op_kwargs={'dataset_name':kaggle_dataset_name,
                            'config_dir': kaggle_config_dir,
                            'file_name': '{{ ti.xcom_pull(task_ids="data_ingestion.get_previous_month_filename") }}',
                            'data_path': local_data_path}
            )
            upload_data_to_cloud_task = PythonOperator(
                task_id = 'upload_data_to_gcp',
                python_callable=upload_to_gcs,
                op_args={'data_path': local_data_path}
            )
            extract_previous_month_filename_task > download_data_task > upload_data_to_cloud_task
