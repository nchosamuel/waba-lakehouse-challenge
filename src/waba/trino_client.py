"""Machine-to-machine Keycloak credentials, verified TLS and scoped Trino identity."""
import json
import os
import ssl
import time
import urllib.parse
import urllib.request

_cached = None


def enabled():
    return os.environ.get('WABA_TRINO_SSO_ENABLED') == 'true'


def credentials():
    global _cached
    if _cached and _cached[0]>time.time()+30:
        return _cached[1]
    form={'grant_type':'client_credentials','client_id':os.environ['TRINO_SERVICE_CLIENT'],
          'client_secret':os.environ['TRINO_SERVICE_SECRET']}
    endpoint='http://keycloak.governance:8080/realms/waba/protocol/openid-connect/token'
    with urllib.request.urlopen(urllib.request.Request(endpoint,data=urllib.parse.urlencode(form).encode()),timeout=30) as response:
        result=json.load(response)
    _cached=(time.time()+result['expires_in'],result['access_token'])
    return _cached[1]


def query(sql):
    secured=enabled()
    endpoint='https://trino:8443' if secured else 'http://trino:8080'
    headers={'X-Trino-User':'service-account-'+os.environ['TRINO_SERVICE_CLIENT'] if secured else 'waba-dashboard'}
    if secured:
        headers['Authorization']='Bearer '+credentials()
    client=urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile='/etc/waba/ca.crt'))) if secured else urllib.request.build_opener()
    def get(url,data=None):
        # Never forward a bearer token to a server-controlled arbitrary nextUri.
        target=urllib.parse.urlparse(url);origin=urllib.parse.urlparse(endpoint)
        if (target.scheme,target.hostname,target.port)!=(origin.scheme,origin.hostname,origin.port):
            raise RuntimeError('Unexpected Trino continuation origin')
        with client.open(urllib.request.Request(url,data=data,headers=headers),timeout=120) as response:
            return json.load(response)
    result=get(endpoint+'/v1/statement',sql.encode())
    rows=[];columns=[]
    while True:
        if 'error' in result:
            raise RuntimeError(result['error']['message'])
        columns=result.get('columns',columns)
        rows.extend(result.get('data',[]))
        if 'nextUri' not in result:
            return columns,rows
        result=get(result['nextUri'])
