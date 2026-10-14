"""Resource-bounded local monitoring. Existing data volumes and credentials preserved."""
import argparse
import fcntl
import json
import platform
from pathlib import Path
import sys
import tempfile
from kubernetes import ROOT,run,kube,private_env,pause_metadata,wait_deployment


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--ingestion',action='store_true',help='Also start Kafka/NiFi to measure their real telemetry');args=parser.parse_args()
    lock=(ROOT/'.tools/kubernetes.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    apps=json.loads(kube('get','sparkapplications','-n','processing','-o','json',capture=True))['items']
    if any(a.get('status',{}).get('applicationState',{}).get('state') not in ('COMPLETED','FAILED') for a in apps):raise RuntimeError('Wait for active Spark writers before switching to the local monitoring profile')
    run([sys.executable,'bootstrap.py']);pause_metadata()
    kube('scale','deployment/superset','-n','serving','--replicas=0')
    kube('scale','deployment/airflow','deployment/spark-runner','-n','processing','--replicas=0')
    kube('scale','deployment/nifi','deployment/kafka','-n','ingestion','--replicas='+str(int(args.ingestion)))
    values=private_env()
    for name,ns,data in [('waba-grafana','monitoring',{'GF_SECURITY_ADMIN_USER':values['GRAFANA_ADMIN_USER'],'GF_SECURITY_ADMIN_PASSWORD':values['GRAFANA_ADMIN_PASSWORD']}),('waba-observability','processing',{'NIFI_ADMIN_USER':values['NIFI_ADMIN_USER'],'NIFI_ADMIN_PASSWORD':values['NIFI_ADMIN_PASSWORD']})]:
        kube('apply','--server-side','-f','-',data=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':ns},'stringData':data}))
    run([sys.executable,'scripts/k8s/render_observability.py'])
    run(['docker','build','-f','infra/observability/Dockerfile','-t','waba/observability:20261014','.'])
    for image in ('prom/prometheus:v3.5.0','grafana/grafana:12.2.0','grafana/loki:3.5.5','grafana/alloy:v1.10.2','waba/observability:20261014'):
        run(['docker','pull',image]) if not image.startswith('waba/') else None
        with tempfile.TemporaryDirectory(prefix='waba-monitor-image-') as tmp:
            archive=str(Path(tmp)/'image.tar');arch='arm64' if platform.machine()=='arm64' else 'amd64'
            run(['docker','image','save','--platform','linux/'+arch,'-o',archive,image]);run(['kind','load','image-archive','--name','waba',archive])
    kube('apply','-f','infra/k8s/60-observability.yaml')
    for namespace,names in [('monitoring',['prometheus','loki','grafana']),('processing',['alloy','observability-exporter'])]:
        kube('scale',*['deployment/'+n for n in names],'-n',namespace,'--replicas=1')
        for name in names:wait_deployment(name,namespace)
    if args.ingestion:
        for name in ('kafka','nifi'):wait_deployment(name,'ingestion')
    print('Native monitoring ready. Grafana: localhost:3000 after port-forward; data preserved.')


if __name__=='__main__':main()
