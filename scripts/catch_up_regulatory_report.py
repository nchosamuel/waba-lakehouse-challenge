"""Recover today's scheduler-created regulatory run, preserving the missed deadline."""
import base64
from datetime import datetime, timezone
import fcntl
import json
import subprocess
import time
import urllib.parse
import urllib.request
from kubernetes import ROOT, KUBECTL, CONTEXT, kube, private_env, wait_deployment
from observability_api import prometheus, grafana

DAG='dag_regulatory_report'


def main():
    lock=(ROOT/'.tools/kubernetes.lock').open('w')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    now=datetime.now(timezone.utc);today=now.date()
    apps=json.loads(kube('get','sparkapplications','-n','processing','-o','json',capture=True))['items']
    if any(a.get('status',{}).get('applicationState',{}).get('state') not in ('COMPLETED','FAILED') for a in apps):
        raise RuntimeError('Wait for active Spark writers before regulatory recovery')
    before=prometheus('waba_regulatory_deadline_missed{scenario="live"}')
    report={'started_at_utc':now.isoformat(),'deadline_utc':today.isoformat()+'T06:00:00+00:00','schedule_unchanged':'30 0 * * *','missed_deadline_before_recovery':bool(before and float(before[0]['value'][1])==1),'run_created_by_scheduler':False}
    # Keep monitoring running; release ingestion memory for the scheduler and Spark.
    kube('scale','deployment/kafka','deployment/nifi','-n','ingestion','--replicas=0')
    kube('wait','--for=delete','pod','-n','ingestion','-l','app in (kafka,nifi)','--timeout=120s')
    kube('scale','deployment/airflow','deployment/spark-runner','-n','processing','--replicas=1')
    for name in ('spark-runner','airflow'):wait_deployment(name,'processing')
    values=private_env();auth=base64.b64encode((values['AIRFLOW_ADMIN_USER']+':'+values['AIRFLOW_ADMIN_PASSWORD']).encode()).decode()
    forward=subprocess.Popen([KUBECTL,'--context',CONTEXT,'port-forward','-n','processing','svc/airflow','18081:8081'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    def api(path,method='GET',body=None):
        if forward.poll() is not None:raise RuntimeError('Owned Airflow port-forward stopped')
        request=urllib.request.Request('http://localhost:18081/api/v1/'+path,method=method,data=json.dumps(body).encode() if body is not None else None,headers={'Authorization':'Basic '+auth,'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=20) as response:return json.load(response)
    try:
        deadline=time.monotonic()+120
        while time.monotonic()<deadline:
            try:api('dags/'+DAG);break
            except Exception:time.sleep(2)
        else:raise TimeoutError('Airflow API did not become ready')
        api('dags/dag_ingest_raw','PATCH',{'is_paused':True})
        api('dags/'+DAG,'PATCH',{'is_paused':False})
        deadline=time.monotonic()+1500;last=None
        while time.monotonic()<deadline:
            runs=api('dags/'+DAG+'/dagRuns?order_by=-execution_date&limit=20')['dag_runs']
            selected=[r for r in runs if r['run_type']=='scheduled' and datetime.fromisoformat(r['data_interval_end'].replace('Z','+00:00')).astimezone(timezone.utc).date()==today]
            if selected:
                current=selected[0]
                if current['state']!=last:
                    print(json.dumps({'event':'regulatory_recovery','run_id':current['dag_run_id'],'state':current['state']}),flush=True);last=current['state']
                if current['state']=='success':break
                if current['state']=='failed':raise RuntimeError('Scheduled recovery failed; task logs are preserved in Airflow')
            time.sleep(5)
        else:raise TimeoutError('Scheduler-created report did not succeed')
        report['run_created_by_scheduler']=True
        report['airflow_run']={k:current.get(k) for k in ('dag_id','dag_run_id','run_type','state','logical_date','data_interval_start','data_interval_end','start_date','end_date','conf')}
        report_date=datetime.fromisoformat(current['data_interval_start'].replace('Z','+00:00')).date().isoformat()
        report['report_date']=report_date
        # Read only aggregate artifacts; no source customer/account records printed.
        code="""import json
from waba.publisher import client
s3=client();date=REPORT_DATE
objects=s3.list_objects_v2(Bucket='archive',Prefix='regulatory/'+date+'/').get('Contents',[])
result=[]
for obj in objects:
 rows=[json.loads(line) for line in s3.get_object(Bucket='archive',Key=obj['Key'])['Body'].read().decode().splitlines() if line.strip()]
 result.append({'key':obj['Key'],'rows':len(rows),'report_dates':sorted({r.get('report_date') for r in rows}),'snapshot_dates':sorted({r['snapshot_date'] for r in rows if 'snapshot_date' in r})})
print(json.dumps(result))
""".replace('REPORT_DATE',repr(report_date))
        artifacts=json.loads(kube('exec','deployment/app','-n','serving','--','python','-c',code,capture=True))
        assert len(artifacts)==16,'Expected eight countries and two aggregate report files each'
        assert all(not x['rows'] or x['report_dates']==[report_date] for x in artifacts)
        report['artifacts']=artifacts
        report['data_coverage_note']='BCEAO uses the last available loan snapshot; CIMA can be empty when no modelled insurance operations exist in the reporting month. Run success does not certify data freshness or official regulatory compliance.'
    finally:
        forward.terminate();forward.wait(timeout=10)
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        metric=prometheus('waba_regulatory_deadline_missed{scenario="live"}')
        rules=grafana('/api/prometheus/grafana/api/v1/rules')['data']['groups']
        instances=[a for g in rules for r in g['rules'] if r.get('name')=='Rapport réglementaire manquant à 06 h UTC' for a in r.get('alerts',[]) if a.get('labels',{}).get('scenario')=='live']
        if metric and float(metric[0]['value'][1])==0 and instances and all(a['state']=='Normal' for a in instances):break
        time.sleep(5)
    else:raise TimeoutError('Live deadline alert did not resolve after successful scheduled recovery')
    report.update({'completed_at_utc':datetime.now(timezone.utc).isoformat(),'deadline_was_met':datetime.fromisoformat(current['end_date'].replace('Z','+00:00'))<=datetime.fromisoformat(report['deadline_utc']),'live_deadline_metric_after_recovery':0,'live_grafana_alert_after_recovery':'Normal','airflow_left_running':True,'regulatory_dag_left_unpaused':True,'ingestion_left_paused_for_memory':True})
    (ROOT/'docs/regulatory-catch-up-verification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print('Scheduled regulatory report recovered; artifacts checked; live alert resolved. Original lateness preserved.')


if __name__=='__main__':main()
