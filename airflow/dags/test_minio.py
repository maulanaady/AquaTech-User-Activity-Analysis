from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_to_minio():
    s3_hook = S3Hook(aws_conn_id='minio_connection')
    local_file_path = '/opt/airflow/data/service-account.json'
    s3_bucket_name = 'warehouse'
    s3_key = 'service-account.json'
    s3_hook.load_file(local_file_path, s3_key, bucket_name=s3_bucket_name)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 4),
    'retries': 1
}

with DAG('minio_example_dag', default_args=default_args, schedule=None) as dag:
    upload_to_minio_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_file_to_minio
    )

upload_to_minio_task

if __name__ == '__main__':
    dag.test()
