"""Idempotent local Airflow initialization; passwords never printed."""

import subprocess
import signal
import sys
import time

subprocess.run([sys.executable, "/opt/airflow/init.py"], check=True)
children = [
    subprocess.Popen(["airflow", "scheduler"]),
    subprocess.Popen(["airflow", "webserver", "--port", "8081"]),
]


def stop(*_):
    for child in children:
        child.terminate()
    for child in children:
        child.wait(timeout=20)
    sys.exit(0)


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
while True:
    if any(child.poll() is not None for child in children):
        stop()
    time.sleep(2)
