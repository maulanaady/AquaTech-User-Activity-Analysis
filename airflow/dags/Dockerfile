FROM apache/airflow:2.8.4
USER root

COPY requirements.txt /

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt