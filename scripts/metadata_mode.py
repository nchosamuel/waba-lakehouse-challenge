"""Staged native metadata deployment; preserve all data volumes."""
import base64
import fcntl
import json
import platform
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from kubernetes import ROOT,run,kube,apply,private_env,wait_job,wait_deployment,pause_observability


def main():
    lock=(ROOT/'.tools/kubernetes.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    run([sys.executable,'bootstrap.py'])
    if private_env().get('WABA_TRINO_SSO_ENABLED')!='true':
        raise RuntimeError('Enable secured Trino before metadata discovery')
    apps=json.loads(kube('get','sparkapplications','-n','processing','-o','json',capture=True))['items']
    if any(a.get('status',{}).get('applicationState',{}).get('state') not in ('COMPLETED','FAILED') for a in apps):
        raise RuntimeError('Stop active Spark applications before entering metadata mode')
    pause_observability()
    kube('scale','deployment/superset','-n','serving','--replicas=0')
    kube('scale','deployment/kafka','deployment/nifi','-n','ingestion','--replicas=0')
    kube('scale','deployment/airflow','deployment/spark-runner','-n','processing','--replicas=0')
    run([sys.executable,'scripts/configure_trino_identity.py'])
    current=json.loads(kube('get','configmap/trino-security','-n','serving','-o','json',capture=True))['data']['rules.json']
    desired=(ROOT/'infra/trino/security/rules.json').read_text()
    if current!=desired:
        kube('apply','-f','infra/k8s/46-trino-security.yaml')
        kube('rollout','restart','deployment/trino','-n','serving');wait_deployment('trino','serving')
    values=private_env();directory=ROOT/'.tools/metadata-keys';directory.mkdir(exist_ok=True)
    if not (directory/'private_key.der').exists():
        run(['openssl','genpkey','-algorithm','RSA','-pkeyopt','rsa_keygen_bits:2048','-out',str(directory/'private.pem')])
        run(['openssl','pkcs8','-topk8','-nocrypt','-in',str(directory/'private.pem'),'-outform','DER','-out',str(directory/'private_key.der')])
        run(['openssl','rsa','-in',str(directory/'private.pem'),'-pubout','-outform','DER','-out',str(directory/'public_key.der')])
        for p in directory.iterdir():p.chmod(0o600)
    for name,body in [('waba-metadata-runtime',{'stringData':{'POSTGRES_PASSWORD':values['OPENMETADATA_DB_PASSWORD'],'DB_USER_PASSWORD':values['OPENMETADATA_DB_PASSWORD'],'FERNET_KEY':values['OPENMETADATA_FERNET_KEY']}}),('waba-metadata-keys',{'data':{n:base64.b64encode((directory/n).read_bytes()).decode() for n in ('private_key.der','public_key.der')}})]:
        kube('apply','--server-side','-f','-',data=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':'governance'},'type':'Opaque',**body}))
    for image in ('postgres:15.12','opensearchproject/opensearch:2.19.1','docker.getcollate.io/openmetadata/server:1.9.0'):
        if subprocess.run(['docker','image','inspect',image],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
            run(['docker','pull',image])
        loaded=run(['docker','exec','waba-control-plane','ctr','-n','k8s.io','images','ls','-q'],capture=True)
        if image in loaded:
            continue
        with tempfile.TemporaryDirectory(prefix='waba-metadata-image-') as tmp:
            archive=str(Path(tmp)/'image.tar');arch='arm64' if platform.machine()=='arm64' else 'amd64'
            run(['docker','image','save','--platform','linux/'+arch,'-o',archive,image]);run(['kind','load','image-archive','--name','waba',archive])
    kube('apply','-f','infra/k8s/50-openmetadata.yaml')
    kube('scale','deployment/metadata-db','deployment/metadata-search','-n','governance','--replicas=1')
    for component in ('metadata-db','metadata-search'):wait_deployment(component,'governance')
    resources=json.loads((ROOT/'infra/k8s/50-openmetadata.yaml').read_text())['items']
    template=next(r for r in resources if r['kind']=='Deployment' and r['metadata']['name']=='openmetadata')['spec']['template']
    container=template['spec']['containers'][0]
    for probe in ('startupProbe','readinessProbe','livenessProbe'):container.pop(probe,None)
    container['command']=['/bin/bash','-ec','./bootstrap/openmetadata-ops.sh -d migrate --force']
    template['spec']['restartPolicy']='Never'
    name='metadata-migrate-'+__import__('uuid').uuid4().hex[:8]
    apply({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':'governance'},'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,'template':template}})
    wait_job(name,'governance')
    kube('scale','deployment/openmetadata','-n','governance','--replicas=1');wait_deployment('openmetadata','governance')
    if subprocess.run(['.tools/kubectl','--context','kind-waba','get','cronjob/metadata-discovery','-n','governance'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
        kube('patch','cronjob/metadata-discovery','-n','governance','--type=merge','-p','{"spec":{"suspend":false}}')
    from bootstrap_metadata_auth import initialize
    initialize()
    print('Metadata services ready on Kubernetes; all source volumes preserved.')


if __name__=='__main__':main()
