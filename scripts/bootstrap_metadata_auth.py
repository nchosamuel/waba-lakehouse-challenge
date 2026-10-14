"""Replace the initial catalogue password, without email or credential output."""
import base64
import subprocess
import time
import urllib.request
from kubernetes import ROOT,KUBECTL,CONTEXT,private_env
from metadata_api import MetadataApi


def initialize():
    forward=subprocess.Popen([KUBECTL,'--context',CONTEXT,'-n','governance','port-forward','svc/openmetadata','18585:8585'],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        deadline=time.monotonic()+30
        while True:
            if forward.poll() is not None:raise RuntimeError('Temporary catalogue port-forward failed')
            try:
                with urllib.request.urlopen('http://localhost:18585/api/v1/system/version',timeout=2):break
            except Exception:
                if time.monotonic()>deadline:raise
                time.sleep(1)
        api=MetadataApi(base='http://localhost:18585/api/v1');password=private_env()['OPENMETADATA_ADMIN_PASSWORD']
        try:
            api.request('users/login',{'email':'waba-catalog-admin@example.invalid','password':base64.b64encode(password.encode()).decode()},'POST')
        except RuntimeError:
            api.request('users/changePassword',{'oldPassword':'admin','newPassword':password,'confirmPassword':password,'requestType':'SELF'},'PUT')
        print('Catalogue authentication initialized; no default password retained.')
    finally:
        forward.terminate();forward.wait(timeout=10)


if __name__=='__main__':initialize()
