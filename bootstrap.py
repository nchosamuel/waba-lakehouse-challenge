from pathlib import Path
import secrets
import base64

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
    "WABA_LINEAGE_ENABLED": "false",
    "OPENMETADATA_DB_PASSWORD": secrets.token_urlsafe(24),
    "OPENMETADATA_ADMIN_PASSWORD": secrets.token_urlsafe(24) + "Aa1!",
    "OPENMETADATA_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
    "KEYCLOAK_METADATA_SERVICE_SECRET": secrets.token_urlsafe(32),
    "WABA_TRINO_SSO_ENABLED": "false",
    "KEYCLOAK_TRINO_SECRET": secrets.token_urlsafe(32),
    "KEYCLOAK_BI_SERVICE_SECRET": secrets.token_urlsafe(32),
    "KEYCLOAK_VALIDATION_SERVICE_SECRET": secrets.token_urlsafe(32),
    "KEYCLOAK_DASHBOARD_SERVICE_SECRET": secrets.token_urlsafe(32),
    "TRINO_TLS_PASSWORD": secrets.token_urlsafe(24),
    "TRINO_INTERNAL_SECRET": secrets.token_urlsafe(32),
    "WABA_SSO_ENABLED": "false",
    "KEYCLOAK_ADMIN_USER": "waba-identity-admin",
    "KEYCLOAK_ADMIN_PASSWORD": secrets.token_urlsafe(24),
    "KEYCLOAK_SUPERSET_SECRET": secrets.token_urlsafe(32),
    "SSO_GROUP_ADMIN_PASSWORD": secrets.token_urlsafe(24),
    "SSO_VIEWER_PASSWORD": secrets.token_urlsafe(24),
    "SSO_COMPLIANCE_PASSWORD": secrets.token_urlsafe(24),
    **{f"SSO_ANALYST_{country}_PASSWORD": secrets.token_urlsafe(24) for country in ("CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH")},
    "SUPERSET_ADMIN_USER": "samuel",
    "SUPERSET_ADMIN_PASSWORD": secrets.token_urlsafe(24),
    "SUPERSET_SECRET_KEY": secrets.token_urlsafe(32),
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
