"""Local BI configuration; generated secret and persistent metadata required."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False  # Local HTTP port-forward; use TLS/CSP in the target ingress.
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True, "ALLOW_ADHOC_SUBQUERY": False}
ENABLE_PROXY_FIX = True
ROW_LIMIT = 10000
SQL_MAX_ROW = 10000
SUPERSET_WEBSERVER_TIMEOUT = 120
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60}

# OIDC is enabled by the explicit sso lifecycle action. Local DB API credentials
# remain available to the private provisioning verifier; interactive login is OIDC.
if os.environ.get("WABA_SSO_ENABLED") == "true":
    from flask_appbuilder.security.manager import AUTH_OAUTH
    from keycloak_security import WabaSecurityManager

    AUTH_TYPE = AUTH_OAUTH
    CUSTOM_SECURITY_MANAGER = WabaSecurityManager
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Public"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
        "group_admin": ["Admin"],
        "viewer": ["Gamma", "WABA Viewer"],
        "compliance_officer": ["Gamma", "WABA Compliance"],
        **{f"country_{c}": ["Gamma", f"WABA Analyst {c}"] for c in ("CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH")},
    }
    OAUTH_PROVIDERS = [{
        "name": "keycloak", "icon": "fa-key", "token_key": "access_token",
        "remote_app": {
            "client_id": "waba-superset",
            "client_secret": os.environ["KEYCLOAK_SUPERSET_SECRET"],
            "client_kwargs": {"scope": "openid profile email", "code_challenge_method": "S256"},
            "server_metadata_url": "http://keycloak.governance:8080/realms/waba/.well-known/openid-configuration",
            "api_base_url": "http://keycloak.governance:8080/realms/waba/protocol/openid-connect/",
        },
    }]

if os.environ.get("WABA_TRINO_SSO_ENABLED") == "true":
    from trino_auth import KeycloakServiceAuth
    def DB_CONNECTION_MUTATOR(uri, params, username, security_manager, source):
        if uri.drivername == "trino":
            params.setdefault("connect_args", {}).update(auth=KeycloakServiceAuth(), verify="/etc/waba/ca.crt", http_scheme="https")
        return uri, params
