FROM waba/app:20261014
COPY src /app/src
COPY scripts/ingest_metadata.py scripts/metadata_api.py /app/
COPY infra/openmetadata/curation.py /app/
ENV PYTHONPATH=/app/src
CMD ["python", "/app/ingest_metadata.py"]
