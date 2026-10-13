from pathlib import Path
import secrets

path = Path(__file__).parent / ".env"
if not path.exists():
    with path.open("x") as stream:
        stream.write(
            "MINIO_ROOT_USER=waba-local\nMINIO_ROOT_PASSWORD="
            + secrets.token_urlsafe(24)
            + "\n"
        )
    path.chmod(0o600)
    print("Local environment created.")
else:
    print("Existing local environment preserved.")

# Extend the private environment without replacing any existing credentials.
existing = {
    line.split("=", 1)[0] for line in path.read_text().splitlines() if "=" in line
}
extra = {
    "SPARK_RUNNER_TOKEN": secrets.token_urlsafe(32),
    "NIFI_ADMIN_USER": "samuel",
    "NIFI_ADMIN_PASSWORD": secrets.token_urlsafe(24),
    "AIRFLOW_ADMIN_USER": "samuel",
    "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(24),
    "AIRFLOW_SECRET_KEY": secrets.token_urlsafe(32),
}
with path.open("a") as stream:
    for key, value in extra.items():
        if key not in existing:
            stream.write(f"{key}={value}\n")
path.chmod(0o600)
