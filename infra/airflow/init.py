import os
import subprocess
from airflow import settings
from airflow.models import Pool
from airflow.www.app import create_app

subprocess.run(["airflow", "db", "migrate"], check=True)
app = create_app()
with app.app_context():
    security = app.appbuilder.sm
    username = os.environ["AIRFLOW_ADMIN_USER"]
    if not security.find_user(username=username):
        security.add_user(
            username=username,
            first_name="WABA",
            last_name="Admin",
            email="admin@example.invalid",
            role=security.find_role("Admin"),
            password=os.environ["AIRFLOW_ADMIN_PASSWORD"],
        )
with settings.Session() as session:
    pool = session.query(Pool).filter(Pool.pool == "spark_local").one_or_none()
    if pool is None:
        session.add(
            Pool(
                pool="spark_local",
                slots=1,
                description="One Spark JVM on the M3 Pro",
                include_deferred=False,
            )
        )
    session.commit()
