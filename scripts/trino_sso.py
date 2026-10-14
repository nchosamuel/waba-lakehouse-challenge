"""Enable native Trino HTTPS/OIDC; preserve PVCs and original Compose profile."""
import base64
import fcntl
import time
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
from kubernetes import ROOT, kube, apply, private_env, run, wait_deployment, IMAGE_TAG


def secret(name,values=None,data=None):
    kube('apply','--server-side','-f','-',data=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':'serving'},'type':'Opaque',**({'stringData':values} if values is not None else {'data':data})}))


def main():
    lock_dir=ROOT/'.tools';lock_dir.mkdir(exist_ok=True)
    lock=(lock_dir/'kubernetes.lock').open('w')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise RuntimeError('Another Kubernetes lifecycle command is running')
    applications=json.loads(kube('get','sparkapplications','-n','processing','-o','json',capture=True))['items']
    if any(a.get('status',{}).get('applicationState',{}).get('state') not in ('COMPLETED','FAILED') for a in applications):
        raise RuntimeError('Switch to the BI profile before enabling SQL security')
    run([sys.executable,'bootstrap.py'])
    run([sys.executable,'scripts/configure_trino_identity.py'])
    values=private_env()
    directory=ROOT/'.tools/trino-tls';directory.mkdir(parents=True,exist_ok=True)
    if not (directory/'server.p12').exists():
        run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(directory/'server.key'),'-out',str(directory/'ca.crt'),'-days','30','-subj','/CN=trino', '-addext','subjectAltName=DNS:localhost,DNS:trino,DNS:trino.serving,DNS:trino.serving.svc.cluster.local,IP:127.0.0.1'])
        run(['openssl','pkcs12','-export','-in',str(directory/'ca.crt'),'-inkey',str(directory/'server.key'),'-out',str(directory/'server.p12'),'-passout','env:WABA_CERT_PASSWORD'],env=dict(os.environ,WABA_CERT_PASSWORD=values['TRINO_TLS_PASSWORD']))
        for p in directory.iterdir():p.chmod(0o600)
    secret('waba-trino-tls',data={p.name:base64.b64encode(p.read_bytes()).decode() for p in (directory/'ca.crt',directory/'server.p12')})
    secret('waba-trino-runtime',{k:values[k] for k in ('MINIO_ROOT_USER','MINIO_ROOT_PASSWORD','TRINO_TLS_PASSWORD','TRINO_INTERNAL_SECRET','KEYCLOAK_TRINO_SECRET')})
    kube('apply','-f','infra/k8s/46-trino-security.yaml')
    wait_deployment('trino','serving')
    # Both app service credentials are private: dashboard is read-only Gold;
    # validation is the separately privileged deployment/verification identity.
    app_secret={k:values[k] for k in ('MINIO_ROOT_USER','MINIO_ROOT_PASSWORD','KEYCLOAK_VALIDATION_SERVICE_SECRET')}
    app_secret.update(AWS_ACCESS_KEY_ID=values['MINIO_ROOT_USER'],AWS_SECRET_ACCESS_KEY=values['MINIO_ROOT_PASSWORD'],AWS_REGION='us-east-1',PYTHONPATH='/app/src',WABA_TRINO_SSO_ENABLED='true',TRINO_SERVICE_CLIENT='waba-dashboard',TRINO_SERVICE_SECRET=values['KEYCLOAK_DASHBOARD_SERVICE_SECRET'])
    secret('waba-runtime',app_secret)
    bi={k:values[k] for k in ('SUPERSET_ADMIN_USER','SUPERSET_ADMIN_PASSWORD','SUPERSET_SECRET_KEY','KEYCLOAK_SUPERSET_SECRET','KEYCLOAK_BI_SERVICE_SECRET')}
    bi.update(WABA_SSO_ENABLED='true',WABA_TRINO_SSO_ENABLED='true');secret('waba-bi-runtime',bi)
    for component,dockerfile in [('app','infra/k8s/app.Dockerfile'),('superset','infra/superset/Dockerfile')]:
        image='waba/'+component+':'+IMAGE_TAG
        run(['docker','build','-f',dockerfile,'-t',image,'.'])
        with tempfile.TemporaryDirectory(prefix='waba-secure-image-') as tmp:
            archive=str(Path(tmp)/'image.tar');arch='arm64' if platform.machine()=='arm64' else 'amd64'
            run(['docker','image','save','--platform','linux/'+arch,'-o',archive,image]);run(['kind','load','image-archive','--name','waba',archive])
        patch={'spec':{'template':{'spec':{'containers':[{'name':component,'volumeMounts':[{'name':'trust','mountPath':'/etc/waba','readOnly':True}]}],'volumes':[{'name':'trust','secret':{'secretName':'waba-trino-tls','items':[{'key':'ca.crt','path':'ca.crt'}]}}]}}}}
        kube('patch','deployment/'+component,'-n','serving','--type=strategic','-p',json.dumps(patch));kube('rollout','restart','deployment/'+component,'-n','serving');wait_deployment(component,'serving')
    env=ROOT/'.env';lines=env.read_text().splitlines();env.write_text('\n'.join('WABA_TRINO_SSO_ENABLED=true' if l.startswith('WABA_TRINO_SSO_ENABLED=') else l for l in lines)+'\n')
    # A bound HTTP health listener is not proof that all plugins initialized.
    os.environ['WABA_RUNTIME']='kubernetes'
    from verification_runtime import query
    deadline=time.monotonic()+120
    while True:
        try:
            assert query('SELECT current_user')==[['service-account-waba-validation']]
            break
        except Exception:
            if time.monotonic()>=deadline:raise
            time.sleep(2)
    run([sys.executable,'scripts/create_bi_views.py'])
    run([sys.executable,'scripts/create_lambda_view.py'],env=dict(os.environ,WABA_RUNTIME='kubernetes'))
    print('Trino HTTPS/OIDC enabled; private CA trust mounted in SQL clients.')


if __name__=='__main__':main()
