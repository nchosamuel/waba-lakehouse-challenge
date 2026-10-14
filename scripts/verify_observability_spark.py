"""Run one real CI batch and prove native Spark metrics and JSON log collection."""
import fcntl
import json
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime,timezone
from kubernetes import ROOT,kube,execute,wait_deployment
from observability_api import prometheus,loki


def main():
    lock=(ROOT/'.tools/kubernetes.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    original={}
    for name in ('kafka','nifi'):
        original[name]=json.loads(kube('get','deployment/'+name,'-n','ingestion','-o','json',capture=True))['spec'].get('replicas',1)
    kube('scale','deployment/kafka','deployment/nifi','-n','ingestion','--replicas=0')
    kube('wait','--for=delete','pod','-n','ingestion','-l','app in (kafka,nifi)','--timeout=120s')
    stop=threading.Event();samples=[]
    def sample():
        while not stop.is_set():
            try:
                for value in prometheus('up{job="spark-native"}'): 
                    if value['value'][1]=='1':samples.append({'application':value['metric']['application'],'timestamp':value['value'][0]})
            except Exception:pass
            stop.wait(3)
    worker=threading.Thread(target=sample);worker.start()
    try:name=execute('regulatory_report',{'COUNTRY_CODES':'CI','REPORT_DATE':'2026-02-28'})
    finally:
        stop.set();worker.join()
        for name_,replicas in original.items():kube('scale','deployment/'+name_,'-n','ingestion','--replicas='+str(replicas))
        for name_,replicas in original.items():
            if replicas:wait_deployment(name_,'ingestion')
    samples=[x for x in samples if x['application']==name+'-driver']
    assert samples,'Native Spark executor metrics never scraped successfully for this driver'
    match=urllib.parse.urlencode({'match[]':'{job="spark-native",application="'+name+'-driver"}'})
    metric_names=json.load(urllib.request.urlopen('http://localhost:9090/api/v1/label/__name__/values?'+match,timeout=15))['data']
    native_names=[x for x in metric_names if x.startswith('metrics_executor_')]
    assert native_names,'No actual executor metrics collected'
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        rows=loki('{namespace="processing", pod="'+name+'-driver", event="regulatory_report_completed"} | json')
        durations=prometheus('waba_spark_job_duration_seconds{application="'+name+'",country_scope="CI",state="COMPLETED"}')
        if rows and durations:break
        time.sleep(5)
    else:raise TimeoutError('Real Spark completion event/duration not collected')
    report={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'spark_application':name,'country_scope':'CI','report_date':'2026-02-28','native_spark_metrics_scraped':True,'native_metrics_samples':len(samples),'native_executor_metric_names_count':len(native_names),'job_duration_seconds':float(durations[0]['value'][1]),'json_event_parsed_as_loki_label':True,'json_event':'regulatory_report_completed','data_writes':'idempotent CI regulatory report replay','ingestion_replicas_restored':original}
    (ROOT/'docs/observability-spark-verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print('Real CI Spark job, native metrics, duration and parsed JSON logs verified.')


if __name__=='__main__':main()
