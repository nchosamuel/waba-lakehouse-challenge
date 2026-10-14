"""Build and run the scheduled external catalogue connector on Kubernetes."""
import json
import fcntl
import platform
from pathlib import Path
import sys
import tempfile
from kubernetes import ROOT,run,kube,apply,private_env,wait_job


def main():
    lock=(ROOT/'.tools/kubernetes.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    values=private_env()
    secret={'OPENMETADATA_ADMIN_PASSWORD':values['OPENMETADATA_ADMIN_PASSWORD'],'OPENMETADATA_API_URL':'http://openmetadata:8585/api/v1','WABA_TRINO_SSO_ENABLED':'true','TRINO_SERVICE_CLIENT':'waba-metadata','TRINO_SERVICE_SECRET':values['KEYCLOAK_METADATA_SERVICE_SECRET'],'TRINO_ENDPOINT':'https://trino.serving:8443','AWS_ACCESS_KEY_ID':values['MINIO_ROOT_USER'],'AWS_SECRET_ACCESS_KEY':values['MINIO_ROOT_PASSWORD'],'AWS_DEFAULT_REGION':'us-east-1'}
    kube('apply','--server-side','-f','-',data=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':'waba-metadata-pipeline','namespace':'governance'},'stringData':secret}))
    import base64
    kube('apply','--server-side','-f','-',data=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':'waba-metadata-trust','namespace':'governance'},'data':{'ca.crt':base64.b64encode((ROOT/'.tools/trino-tls/ca.crt').read_bytes()).decode()}}))
    image='waba/metadata-ingestion:20261014'
    run(['docker','build','-f','infra/openmetadata/Dockerfile','-t',image,'.'])
    with tempfile.TemporaryDirectory(prefix='waba-ingest-image-') as tmp:
        archive=str(Path(tmp)/'image.tar');arch='arm64' if platform.machine()=='arm64' else 'amd64'
        run(['docker','image','save','--platform','linux/'+arch,'-o',archive,image]);run(['kind','load','image-archive','--name','waba',archive])
    kube('apply','-f','infra/k8s/55-metadata-ingestion.yaml')
    name='metadata-discovery-'+__import__('uuid').uuid4().hex[:8]
    kube('create','job',name,'--from=cronjob/metadata-discovery','-n','governance')
    wait_job(name,'governance')
    print('Metadata ingestion completed:',name)


if __name__=='__main__':main()
