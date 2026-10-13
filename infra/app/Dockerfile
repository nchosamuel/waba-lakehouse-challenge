FROM python:3.11.12-slim
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/pip pip install --timeout 120 --retries 5 streamlit==1.46.0 boto3==1.37.38
ENV PYTHONPATH=/app/src
CMD ["streamlit", "run", "app/main.py", "--server.address=0.0.0.0", "--browser.gatherUsageStats=false"]
