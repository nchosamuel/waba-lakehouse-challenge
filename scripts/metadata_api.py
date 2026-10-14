"""Private local catalogue API access. Tokens and signing keys are never printed."""
import base64
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import urllib.error

ROOT=Path(__file__).resolve().parents[1]


def admin_token():
    def encode(value):return base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).decode().rstrip('=')
    data=encode({'alg':'RS256','typ':'JWT','kid':'waba-local-catalog'})+'.'+encode({'iss':'waba-catalog','sub':'waba-catalog-admin','email':'waba-catalog-admin@example.invalid','iat':int(time.time()),'exp':int(time.time())+7200,'isBot':False})
    signed=subprocess.check_output(['openssl','dgst','-sha256','-sign',str(ROOT/'.tools/metadata-keys/private.pem')],input=data.encode())
    return data+'.'+base64.urlsafe_b64encode(signed).decode().rstrip('=')


class MetadataApi:
    def __init__(self,token=None,base=None):
        self.base=base or os.environ.get('OPENMETADATA_API_URL','http://localhost:8585/api/v1')
        self.token=token or os.environ.get('OPENMETADATA_API_TOKEN')
        if not self.token and os.environ.get('OPENMETADATA_ADMIN_PASSWORD'):
            req=urllib.request.Request(self.base+'/users/login',data=json.dumps({'email':'waba-catalog-admin@example.invalid','password':base64.b64encode(os.environ['OPENMETADATA_ADMIN_PASSWORD'].encode()).decode()}).encode(),headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=30) as response:self.token=json.load(response)['accessToken']
        if not self.token:self.token=admin_token()

    def request(self,path,data=None,method=None):
        req=urllib.request.Request(self.base+'/'+path,data=json.dumps(data).encode() if data is not None else None,
            headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json'},method=method)
        try:
            response=urllib.request.urlopen(req,timeout=90)
        except urllib.error.HTTPError as error:
            try:message=json.load(error).get('message','Request rejected')
            except Exception:message='Request rejected'
            raise RuntimeError(str(error.code)+' '+path+': '+message) from None
        with response:
            content=response.read()
            if not content:return None
            try:return json.loads(content)
            except json.JSONDecodeError:return {"message":content.decode()}
