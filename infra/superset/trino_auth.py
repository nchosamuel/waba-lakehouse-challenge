"""Confidential Keycloak service client for the shared Superset SQL connection."""
import os
import threading
import time
import requests
from trino.auth import Authentication
from requests.auth import AuthBase


class KeycloakServiceAuth(Authentication, AuthBase):
    def __init__(self):
        self._token=None
        self._expires=0
        self._lock=threading.Lock()

    def set_http_session(self,session):
        session.auth=self
        return session

    def __call__(self,request):
        with self._lock:
            if self._expires<time.time()+30:
                response=requests.post('http://keycloak.governance:8080/realms/waba/protocol/openid-connect/token',
                    data={'grant_type':'client_credentials','client_id':'waba-bi','client_secret':os.environ['KEYCLOAK_BI_SERVICE_SECRET']},timeout=30)
                response.raise_for_status()
                result=response.json();self._token=result['access_token'];self._expires=time.time()+result['expires_in']
            request.headers['Authorization']='Bearer '+self._token
        return request

