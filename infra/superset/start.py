"""Idempotent local Superset initialization, credentials never printed."""

import os
import subprocess
import sys

subprocess.run(["superset", "db", "upgrade"], check=True)
from superset.app import create_app
from superset import db

app = create_app()
with app.app_context():
    security = app.appbuilder.sm
    username = os.environ["SUPERSET_ADMIN_USER"]
    if not security.find_user(username=username):
        security.add_user(
            username,
            "WABA",
            "Admin",
            "admin@example.invalid",
            security.find_role("Admin"),
            os.environ["SUPERSET_ADMIN_PASSWORD"],
        )
subprocess.run(["superset", "init"], check=True)
subprocess.run([sys.executable, "/app/waba-superset/provision.py"], check=True)
os.execvp(
    "gunicorn",
    [
        "gunicorn",
        "--bind",
        "0.0.0.0:8088",
        "--workers",
        "1",
        "--threads",
        "4",
        "--timeout",
        "120",
        "superset.app:create_app()",
    ],
)
