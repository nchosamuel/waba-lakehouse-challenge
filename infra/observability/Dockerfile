FROM python:3.11.14-slim
RUN pip install --no-cache-dir kafka-python==2.2.15
COPY scripts/observability_exporter.py /app/exporter.py
USER 1000
CMD ["python", "/app/exporter.py"]
