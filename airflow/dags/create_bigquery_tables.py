from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

dag = DAG('create_bigquery_tables',
          description='A DAG to execute SQL files from a specific path',
          start_date=datetime(2024, 4, 24),
          template_searchpath=["/opt/airflow/data/"],
          schedule='@once'
        )


execute_query = BigQueryExecuteQueryOperator(
    task_id='execute_query',
    gcp_conn_id='google_client',
    sql='ddl_table.sql',
    use_legacy_sql=False,
    dag=dag
)


if __name__ == '__main__':
    dag.test()