"""
An example DAG that uses Cosmos to render a dbt project.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow import Dataset
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from cosmos import DbtTaskGroup, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.config import ProjectConfig
from cosmos.constants import TestBehavior   # , InvocationMode



DEFAULT_DBT_ROOT_PATH = "/opt/airflow/data/dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

transform_dataset = Dataset("bigquery/zoomcamp-ady.event_zoomcamp_dataset.event_data")

profile_config = ProfileConfig(
    profile_name="user_activity",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
    conn_id = 'google_client',
    profile_args = {"project" : "zoomcamp-ady", 
                    "dataset" : "event_zoomcamp_dataset",
                    "keyfile" : "/opt/airflow/data/service-account.json"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path='/home/airflow/.local/bin/dbt',
    # invocation_mode = InvocationMode.DBT_RUNNER,
)


@dag(
    schedule=[transform_dataset],
    start_date=datetime(2024, 2, 29),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2},    
)
def event_data_transformations():
    @task
    def execute_query(data_interval_start=None,data_interval_end=None):
        dt = data_interval_start.strftime('%Y%m%d')
        data_interval_end = data_interval_end.strftime('%Y-%m-%d %H:%M:%S')
        ext_query = f"""
            create or replace external table event_zoomcamp_dataset.ext_event_data
            OPTIONS (
            format = 'CSV',
            uris = ['gs://event_zoomcamp_bucket/output_{dt}*.csv']
            )
        """
        insert_query = f"""
            MERGE INTO
            event_zoomcamp_dataset.event_data AS T
            USING
            (
                with tmp as (SELECT *, TIMESTAMP('{data_interval_end}') as dl_updated_at,
                ROW_NUMBER() OVER (PARTITION BY distinct_id, event_time ORDER BY event_time ASC) as rn
                FROM event_zoomcamp_dataset.ext_event_data)
                select * EXCEPT (rn)
                from tmp 
                where rn = 1  
            ) AS S
            ON T.distinct_id = S.distinct_id
            AND T.event_time = S.event_time
            WHEN MATCHED THEN 
            UPDATE SET
                T.distinct_id = S.distinct_id		,
                T.elements = S.elements	,
                T.event = S.event	,
                T.event_time = S.event_time,
                T.file = S.file,
                T.geoip_city_name = S.geoip_city_name	,
                T.geoip_continent_code = S.geoip_continent_code	,
                T.geoip_continent_name = S.geoip_continent_name	,
                T.geoip_country_code = S.geoip_country_code	,
                T.geoip_country_name = S.geoip_country_name	,
                T.geoip_latitude = S.geoip_latitude,
                T.geoip_longitude = S.geoip_longitude,
                T.geoip_subdivision_1_code = S.geoip_subdivision_1_code	,
                T.geoip_subdivision_1_name = S.geoip_subdivision_1_name	,
                T.geoip_time_zone = S.geoip_time_zone	,
                T.uuid  =S.uuid,
                T.dl_updated_at = S.dl_updated_at
            WHEN NOT MATCHED BY TARGET THEN
            INSERT ROW
        """
        sql = [ext_query,insert_query]
        hook = BigQueryHook(
                    gcp_conn_id='google_client',
                    use_legacy_sql=False,
                    location=None,
                    impersonation_chain=None,
                )
        job_id = [ hook.run_query(
                    sql=s,
                    destination_dataset_table=None,
                    write_disposition='WRITE_TRUNCATE',
                    allow_large_results=False,
                    flatten_results=None,
                    create_disposition='CREATE_IF_NEEDED',
                    priority='INTERACTIVE',
                )
                for s in sql
            ]
        return job_id
        
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(dbt_project_path="/opt/airflow/data/dbt/user_activity",
                                    #  dbt_vars={"runtime": "{{ data_interval_end }}"}
                                     ),
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH,emit_datasets=False),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            # "install_deps": True,  # install any necessary dependencies before running any dbt command
            # "full_refresh": True,  # used only in dbt commands that support this flag
            "vars": '{"runtime": {{ data_interval_end }} }',
            "fail_fast": True,
            "quiet": True,
            "should_store_compiled_sql": True
        },
    )

    execute_query() >> transform_data
dag = event_data_transformations()
    


if __name__ == '__main__':
    dag.test()
