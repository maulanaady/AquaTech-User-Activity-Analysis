import json,sys
import csv,logging
import time
import glob
import datetime
import os
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.decorators import task

from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# local_file_path = "/opt/airflow/data/output.csv"
# gcs_file_path = "output.csv"
bucket_name = "event_zoomcamp_bucket"

transform_dataset = Dataset("bigquery/event_data")


def _validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

with DAG(
    dag_id="ingest_data",
    start_date=pendulum.today('UTC').add(days=-15),
    schedule='0 */1 * * *',
    max_active_runs=1,
    catchup=True) as dag:

    @task
    def get_data(source_path,dest_path, data_interval_start=None, data_interval_end=None, **context):
        files = glob.glob("{}/{}/*.json".format(source_path,data_interval_start.strftime('%Y-%m-%d')))    # filter folder based on running date
        suffix_filenames = [int(Path(file).stem.split('-')[1]) for file in files]
        start_time = int((data_interval_start).strftime('%H%M%S')) 
        end_time = int((data_interval_end).strftime('%H%M%S')) 
        filtered_filenames = list(filter(lambda x: start_time <= x < end_time, suffix_filenames))  # filter files based on execution time
        json_files = ['{0}/{1}/{2}-'.format(source_path,data_interval_start.strftime('%Y-%m-%d'), data_interval_start.strftime('%Y%m%d'))+str(filtered_filename).zfill(6)+'.json' for filtered_filename in filtered_filenames]
        
        records = list()
        for file in json_files:
            with open(file, "r") as f:
                x = f.read()
                if _validateJSON(x) == True:
                    data = json.loads(x)
                    for i in range(len(data)):
                        record = data[i]
                        record["file"] = os.path.basename(file)
                        records.append(record)
                else:
                    print(f"{file} is not json format")
        
        # Remove $ sign from keys in all dictionaries
        records = [{key.replace("$", ""): value for key, value in d.items()} for d in records]

        if records:
        # Rename 'timestamp' key to 'event_time'
            for entry in records:
                if 'timestamp' in entry:
                    entry['event_time'] = entry.pop('timestamp')
                    # Get execution's date
                    # execution_date = data_interval_start.date()
                    # Extract the time part from the original value
                    # original_time = datetime.datetime.strptime(entry['event_time'], '%Y-%m-%dT%H:%M:%S.%fZ').time()
                    # Combine today's date with the original time
                    # new_datetime = datetime.datetime.combine(execution_date, original_time)

                    # Format the combined datetime to match the original format
                    # entry['event_time'] = new_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    
            fieldnames = sorted(set().union(*(d.keys() for d in records)))
            csv_file = 'output_{}_{}.csv'.format(data_interval_start.strftime('%Y%m%d%H%M%S'),data_interval_end.strftime('%Y%m%d%H%M%S'))
            
            with open(f'{dest_path}/{csv_file}', 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                for row in records:
                    filled_row = {}
                    for key in fieldnames:
                        if key == 'distinct_id':
                            filled_row[key] = ''.join(filter(str.isdigit, row.get(key, '')))[:4]
                        else:
                            filled_row[key] = row.get(key, '')
                    writer.writerow(filled_row)
            print(f"CSV file generated successfully: {csv_file}")
            return(csv_file)
        else:
            return None    

    @task
    def upload_gcs(gcp_conn_id, **context):
        xcom = context["task_instance"].xcom_pull(task_ids='get_data', key='return_value')
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        if xcom:
            hook = GCSHook(gcp_conn_id=gcp_conn_id)
            hook.upload(
                    bucket_name="event_zoomcamp_bucket",
                    object_name=context["task_instance"].xcom_pull(task_ids='get_data', key='return_value'),
                    filename="/opt/airflow/data/{}".format(context["task_instance"].xcom_pull(task_ids='get_data', key='return_value')),
                )
            os.remove("/opt/airflow/data/{}".format(context["task_instance"].xcom_pull(task_ids='get_data', key='return_value')))    

    @task.short_circuit
    def checking_runtime(data_interval_start=None):
        data_interval_start = data_interval_start.strftime('%H%M%S')
        if data_interval_start == '000000':
            return True
        else:
            return False

    trigger = EmptyOperator(task_id="trigger_transformation", outlets=[transform_dataset])

    # get_data('/opt/airflow/data/raw_data','/opt/airflow/data') >> upload_gcs("google_client") 
    # upload_gcs("google_client") >> checking_runtime() >> trigger

    chain(get_data('/opt/airflow/data/raw_data','/opt/airflow/data'),upload_gcs("google_client"),checking_runtime(),trigger)

if __name__ == '__main__':
    dag.test()


