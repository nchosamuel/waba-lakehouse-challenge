FROM waba-lakehouse-app:latest
COPY app /app/app
COPY src /app/src
COPY tests /app/tests
COPY scripts/test_unit.py /app/scripts/test_unit.py
