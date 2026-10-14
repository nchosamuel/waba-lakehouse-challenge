FROM waba-lakehouse-airflow:latest
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root src /app/src
