FROM apache/airflow:2.10.5-python3.11
# No Docker socket, extra provider or JVM needed in the orchestration container.
COPY --chown=airflow:root infra/airflow/start.py infra/airflow/init.py /opt/airflow/
RUN mkdir -p /opt/airflow/state
ENTRYPOINT ["python", "/opt/airflow/start.py"]
