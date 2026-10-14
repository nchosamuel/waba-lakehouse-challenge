"""Local BI configuration; generated secret and persistent metadata required."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False  # Local HTTP port-forward; use TLS/CSP in the target ingress.
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
ENABLE_PROXY_FIX = True
ROW_LIMIT = 10000
SQL_MAX_ROW = 10000
SUPERSET_WEBSERVER_TIMEOUT = 120
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60}
