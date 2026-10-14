"""Read-only native telemetry. No records, customer IDs or SQL result rows collected."""
import concurrent.futures
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import ssl
import threading
import time
import urllib.parse
import urllib.request

NAMESPACES = ('ingestion', 'processing', 'serving', 'governance', 'monitoring')
STATE = {'body': '# Initializing\n'}
LOCK = threading.Lock()
SOURCE_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=5)
SOURCE_FUTURES = {}


def fetch(url, headers=None, data=None, context=None):
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}, data=data), context=context, timeout=5) as r:
        return json.load(r)


def kube(path):
    token = Path('/var/run/secrets/kubernetes.io/serviceaccount/token').read_text()
    return fetch('https://kubernetes.default.svc'+path, {'Authorization': 'Bearer '+token}, context=ssl.create_default_context(cafile='/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'))


class Metrics:
    def __init__(self): self.lines = []
    def add(self, name, value, **labels):
        suffix = '{'+','.join(f'{k}={json.dumps(str(v))}' for k,v in sorted(labels.items()))+'}' if labels else ''
        self.lines.append(f'{name}{suffix} {float(value)}')
    def render(self): return '\n'.join(self.lines)+'\n'


def spark_metrics(m):
    apps=kube('/apis/sparkoperator.k8s.io/v1beta2/namespaces/processing/sparkapplications')['items']
    failure=0;stream_present=0
    for app in apps:
        spec=app['spec'];status=app.get('status',{});state=status.get('applicationState',{}).get('state','UNKNOWN')
        job=spec['mainApplicationFile'].rsplit('/',1)[-1].removesuffix('.py')
        countries=next((e['value'] for e in spec['driver'].get('env',[]) if e['name']=='COUNTRY_CODES'),'ALL')
        # Whole-job duration associated with country scope; never invent per-country timings for shared runs.
        start=status.get('lastSubmissionAttemptTime');end=status.get('terminationTime')
        if start and end:
            duration=(datetime.fromisoformat(end.replace('Z','+00:00'))-datetime.fromisoformat(start.replace('Z','+00:00'))).total_seconds()
            m.add('waba_spark_job_duration_seconds',duration,job=job,country_scope=countries,application=app['metadata']['name'],state=state)
        if app['metadata']['name']=='waba-streaming':
            stream_present=1;failure=int(state in ('FAILED','FAILING','SUBMISSION_FAILED','INVALIDATING'))
    m.add('waba_spark_fraud_failed',failure,scenario='live')
    m.add('waba_stream_expected',stream_present)


def deployments(m):
    for ns in NAMESPACES:
        for d in kube('/apis/apps/v1/namespaces/'+ns+'/deployments')['items']:
            labels={'namespace':ns,'component':d['metadata']['name']}
            m.add('waba_component_desired_replicas',d['spec'].get('replicas',1),**labels)
            m.add('waba_component_ready_replicas',d.get('status',{}).get('readyReplicas',0),**labels)
            source={'kafka':'kafka','nifi':'nifi','airflow':'airflow'}.get(d['metadata']['name'])
            if source:
                m.add('waba_telemetry_expected',int(d['spec'].get('replicas',1)>0),source=source)
    # Coordinator native health endpoint. Disabled services are shown by desired replicas above.
    result=fetch('http://trino.serving:8080/v1/info')
    m.add('waba_trino_starting',int(result.get('starting',False)))


def committed_offsets(directory):
    commits=Path(directory)/'commits'
    batches=[int(p.name) for p in commits.glob('*') if p.name.isdigit()]
    if not batches: return {}
    lines=(Path(directory)/'offsets'/str(max(batches))).read_text().splitlines()
    result={}
    for line in lines[2:]:
        if line=='-':continue
        for topic,partitions in json.loads(line).items():
            for partition,offset in partitions.items():result[(topic,int(partition))]=int(offset)
    return result


def kafka_metrics(m):
    deployment=kube('/apis/apps/v1/namespaces/ingestion/deployments/kafka')
    if deployment['spec'].get('replicas',1)==0:
        raise RuntimeError('Kafka deliberately paused')
    from kafka import KafkaConsumer, TopicPartition
    health=json.loads(Path('/checkpoints/stream-health.json').read_text())
    heartbeat=datetime.fromisoformat(health['heartbeat_utc'])
    m.add('waba_stream_heartbeat_age_seconds',(datetime.now(timezone.utc)-heartbeat).total_seconds())
    consumer=KafkaConsumer(bootstrap_servers='kafka.ingestion:9092',group_id=None,enable_auto_commit=False,api_version=(3,9),request_timeout_ms=5000,api_version_auto_timeout_ms=3000,metadata_max_age_ms=10000)
    try:
        for query,directory,topics in [
            ('raw_to_silver','raw-to-silver',['raw-bank-transactions','raw-insurance-operations','raw-mobile-money-payments','raw-loan-repayments']),
            ('silver_to_gold','silver-to-gold',['silver-bank-transactions','silver-insurance-operations','silver-mobile-money','silver-loan-repayments'])]:
            actual=[topic for topic in topics if consumer.partitions_for_topic(topic) is not None]
            partitions=[TopicPartition(topic,p) for topic in actual for p in consumer.partitions_for_topic(topic)]
            if not partitions:raise RuntimeError('No Kafka source partitions')
            ends=consumer.end_offsets(partitions);begins=consumer.beginning_offsets(partitions)
            offsets=committed_offsets('/checkpoints/'+directory);total=0
            for p in partitions:
                lag=max(0,ends[p]-max(offsets.get((p.topic,p.partition),begins[p]),begins[p]))
                total+=lag
                m.add('waba_kafka_checkpoint_lag_messages',lag,query=query,topic=p.topic,partition=p.partition)
            m.add('waba_kafka_query_lag_messages',total,query=query,scenario='live')
            q=next((q for q in health['queries'] if q['name']==query),None)
            if q and q.get('recent_progress'):
                progress=q['recent_progress'][-1]
                m.add('waba_spark_stream_input_rows_per_second',progress.get('inputRowsPerSecond',0),query=query)
                m.add('waba_spark_stream_batch_duration_seconds',progress.get('durationMs',{}).get('triggerExecution',0)/1000,query=query)
    finally:consumer.close()


def nifi_metrics(m):
    # NiFi generates a self-signed certificate for the local installation only.
    ctx=ssl._create_unverified_context()
    data=urllib.parse.urlencode({'username':os.environ['NIFI_ADMIN_USER'],'password':os.environ['NIFI_ADMIN_PASSWORD']}).encode()
    req=urllib.request.Request('https://nifi.ingestion.svc.cluster.local:8443/nifi-api/access/token',data=data)
    with urllib.request.urlopen(req,context=ctx,timeout=5) as r:token=r.read().decode()
    status=fetch('https://nifi.ingestion.svc.cluster.local:8443/nifi-api/flow/process-groups/root/status',{'Authorization':'Bearer '+token},context=ctx)['processGroupStatus']['aggregateSnapshot']
    # These are NiFi's rolling five-minute statistics, not monotonic counters.
    m.add('waba_nifi_flowfiles_out_5m',status.get('flowFilesOut',0))
    m.add('waba_nifi_bytes_out_5m',status.get('bytesOut',0))
    m.add('waba_nifi_queued_flowfiles',status.get('flowFilesQueued',0))


def regulatory_missed(now, states):
    """states must describe the scheduled run whose interval ends on the UTC day."""
    now=now.astimezone(timezone.utc)
    return int(now.hour>=6 and not (states and states[0][0]=='success'))


def airflow_metrics(m):
    conn=sqlite3.connect('file:/airflow-state/airflow.db?mode=ro',uri=True,timeout=2)
    try:
        rows=conn.execute("SELECT dag_id,state,COUNT(*) FROM dag_run WHERE start_date >= datetime('now','-1 day') GROUP BY dag_id,state").fetchall()
        for dag,state,count in rows:m.add('waba_airflow_runs_24h',count,dag=dag,state=state)
        for (dag,) in conn.execute('SELECT DISTINCT dag_id FROM dag_run'):
            states=dict(conn.execute("SELECT state,COUNT(*) FROM dag_run WHERE dag_id=? AND start_date >= datetime('now','-1 day') GROUP BY state",(dag,)).fetchall())
            m.add('waba_airflow_error_ratio_24h',states.get('failed',0)/max(sum(states.values()),1),dag=dag)
        # The scheduled run whose interval ends today must succeed by today 06:00 UTC.
        now=datetime.now(timezone.utc);day=now.date().isoformat()
        rows=conn.execute("SELECT state FROM dag_run WHERE dag_id='dag_regulatory_report' AND run_type='scheduled' AND date(data_interval_end)=? ORDER BY execution_date DESC LIMIT 1",(day,)).fetchall()
        m.add('waba_regulatory_deadline_missed',regulatory_missed(now,rows),scenario='live')
    finally:conn.close()


def collect(source_functions=None, timeout_seconds=12, demo=None):
    m=Metrics()
    sources=source_functions if source_functions is not None else {'kubernetes':deployments,'spark':spark_metrics,'kafka':kafka_metrics,'nifi':nifi_metrics,'airflow':airflow_metrics}
    def one(item):
        name,fn=item;local=Metrics()
        try:fn(local);local.add('waba_telemetry_available',1,source=name)
        except Exception as e:
            # No exception message: remote responses may contain credentials or record values.
            local=Metrics();local.add('waba_telemetry_available',0,source=name)
            print(json.dumps({'event':'telemetry_unavailable','source':name,'error_type':type(e).__name__}),flush=True)
        return local.lines
    # A stalled SDK must not stop other sources or freeze the published snapshot.
    # Keep at most one in-flight task per source; never accumulate timed-out work.
    for name, fn in sources.items():
        previous = SOURCE_FUTURES.get(name)
        if previous is None or previous.done():
            SOURCE_FUTURES[name] = SOURCE_POOL.submit(one, (name, fn))
    concurrent.futures.wait(
        [SOURCE_FUTURES[name] for name in sources], timeout=timeout_seconds
    )
    for name in sources:
        future = SOURCE_FUTURES[name]
        if future.done():
            m.lines.extend(future.result())
        else:
            m.add('waba_telemetry_available',0,source=name)

    m.add('waba_exporter_collection_timestamp_seconds',time.time())
    # Separate demo namespace and labels: never overwrite observed live metrics.
    if demo is None:
        demo=json.loads(Path('/demo/signals.json').read_text())
    for name,value in demo.items():
        if name not in ('spark_failed','aml_lag','regulatory_missed'):continue
        metric={'spark_failed':'waba_demo_spark_fraud_failed','aml_lag':'waba_demo_kafka_aml_lag_messages','regulatory_missed':'waba_demo_regulatory_deadline_missed'}[name]
        m.add(metric,value,scenario='demo')
    return m.render()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ('/metrics','/health'):self.send_error(404);return
        with LOCK:body=STATE['body'].encode()
        self.send_response(200);self.send_header('Content-Type','text/plain; version=0.0.4');self.end_headers();self.wfile.write(body)
    def do_POST(self):
        if self.path!='/alerts':self.send_error(404);return
        length=int(self.headers.get('Content-Length','0'))
        if length>262144:self.send_error(413);return
        payload=json.loads(self.rfile.read(length))
        for alert in payload.get('alerts',[]):
            labels={k:v for k,v in alert.get('labels',{}).items() if k in ('alertname','scenario','severity','team','query')}
            event={'event':'alert_notification','timestamp':datetime.now(timezone.utc).isoformat(),'status':alert.get('status'),'labels':labels}
            with LOCK:
                with Path('/events/notifications.jsonl').open('a') as f:f.write(json.dumps(event)+'\n')
            print(json.dumps(event),flush=True)
        self.send_response(200);self.end_headers();self.wfile.write(b'OK')
    def log_message(self,*args):pass


def refresh():
    while True:
        try:
            body=collect()
            with LOCK:STATE['body']=body
        except Exception as e:print(json.dumps({'event':'collector_error','error_type':type(e).__name__}),flush=True)
        time.sleep(15)


if __name__=='__main__':
    threading.Thread(target=refresh,daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0',9108),Handler).serve_forever()
