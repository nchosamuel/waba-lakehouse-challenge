FROM apache/superset:5.0.0
USER root
RUN uv pip install trino==0.336.0 authlib==1.3.2
COPY infra/superset /app/waba-superset
ENV PYTHONPATH=/app/waba-superset
ENV SUPERSET_CONFIG_PATH=/app/waba-superset/superset_config.py
USER superset
CMD ["python", "/app/waba-superset/start.py"]
