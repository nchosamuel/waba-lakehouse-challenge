"""Small, pinned native observability profile; all credentials supplied separately."""
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=ROOT/'infra/observability'
OUT.mkdir(exist_ok=True)

def save(path,body):path.write_text(json.dumps(body,indent=2)+'\n')

prom={'global':{'scrape_interval':'15s','evaluation_interval':'15s'},'scrape_configs':[
 {'job_name':'prometheus','static_configs':[{'targets':['localhost:9090']}]},
 {'job_name':'waba','static_configs':[{'targets':['observability-exporter.processing:9108']}]},
 {'job_name':'loki','static_configs':[{'targets':['loki.monitoring:3100']}]},
 {'job_name':'alloy','static_configs':[{'targets':['alloy.processing:12345']}]},
 {'job_name':'spark-native','kubernetes_sd_configs':[{'role':'pod','namespaces':{'names':['processing']}}],
 'metrics_path':'/metrics/executors/prometheus','relabel_configs':[
 {'source_labels':['__meta_kubernetes_pod_label_spark_role'],'regex':'driver','action':'keep'},
 {'source_labels':['__meta_kubernetes_pod_phase'],'regex':'Running','action':'keep'},
 {'source_labels':['__meta_kubernetes_pod_ip'],'target_label':'__address__','replacement':'${1}:4040'},
 {'source_labels':['__meta_kubernetes_pod_name'],'target_label':'application'}]}]}
# JSON is a valid YAML configuration for Prometheus, Loki and Grafana provisioning.
save(OUT/'prometheus.json',prom)
loki={'auth_enabled':False,'server':{'http_listen_port':3100},'common':{'path_prefix':'/loki','replication_factor':1,'ring':{'kvstore':{'store':'inmemory'}},'storage':{'filesystem':{'chunks_directory':'/loki/chunks','rules_directory':'/loki/rules'}}},'schema_config':{'configs':[{'from':'2025-01-01','store':'tsdb','object_store':'filesystem','schema':'v13','index':{'prefix':'index_','period':'24h'}}]},'limits_config':{'retention_period':'72h','reject_old_samples':False},'compactor':{'working_directory':'/loki/compactor','retention_enabled':True,'delete_request_store':'filesystem'},'analytics':{'reporting_enabled':False}}
save(OUT/'loki.json',loki)
alloy='''discovery.kubernetes "pods" {
  role = "pod"
  namespaces { names = ["ingestion", "processing", "serving", "governance", "monitoring"] }
}
discovery.relabel "logs" {
  targets = discovery.kubernetes.pods.targets
  rule { source_labels = ["__meta_kubernetes_namespace"]
    target_label = "namespace" }
  rule { source_labels = ["__meta_kubernetes_pod_name"]
    target_label = "pod" }
  rule { source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label = "container" }
}
loki.source.kubernetes "pods" {
  targets = discovery.relabel.logs.output
  forward_to = [loki.process.json.receiver]
}
local.file_match "airflow" {
  path_targets = [{"__path__" = "/airflow-logs/dag_id=*/run_id=*/task_id=*/*.log", "namespace" = "processing", "component" = "airflow-task"}]
}
loki.source.file "airflow" {
  targets = local.file_match.airflow.targets
  forward_to = [loki.process.json.receiver]
}
loki.process "json" {
  stage.json { expressions = {"event" = "event", "job" = "job", "country_code" = "country_code", "level" = "level"} }
  stage.labels { values = {"event" = "", "job" = "", "country_code" = "", "level" = ""} }
  forward_to = [loki.write.local.receiver]
}
loki.write "local" {
  endpoint { url = "http://loki.monitoring:3100/loki/api/v1/push" }
}
'''
(OUT/'config.alloy').write_text(alloy)
save(OUT/'datasources.json',{'apiVersion':1,'datasources':[{'name':'Prometheus','uid':'waba-prometheus','type':'prometheus','url':'http://prometheus.monitoring:9090','access':'proxy','isDefault':True,'editable':False},{'name':'Loki','uid':'waba-loki','type':'loki','url':'http://loki.monitoring:3100','access':'proxy','editable':False}]})
save(OUT/'providers.json',{'apiVersion':1,'providers':[{'name':'WABA','folder':'WABA','type':'file','options':{'path':'/var/lib/grafana/dashboards'},'allowUiUpdates':False}]})
panels=[]
def panel(title,expr,kind='timeseries',unit='short',description=''):
 i=len(panels);panels.append({'id':i+1,'title':title,'description':description,'type':kind,'datasource':{'type':'prometheus','uid':'waba-prometheus'},'gridPos':{'x':(i%2)*12,'y':(i//2)*8,'w':12,'h':8},'fieldConfig':{'defaults':{'unit':unit,**({'min':0,'max':1} if unit=='percentunit' else {})},'overrides':[]},'options':{'legend':{'displayMode':'table','placement':'bottom'}},'targets':[{'refId':'A','expr':expr,'legendFormat':'{{component}}{{source}}{{exported_job}} {{country_scope}}{{query}}{{dag}}{{scenario}}','instant':kind in ('stat','table'),'format':'table' if kind=='table' else 'time_series'}]})
panel('Services actifs — réplicas prêts','waba_component_ready_replicas',description='Comparer avec les réplicas demandés : 0 demandé signifie un arrêt volontaire.')
panel('Services demandés — 0 = pause volontaire','waba_component_desired_replicas')
panel('Disponibilité des collecteurs','waba_telemetry_available','stat',description='0 signifie mesure indisponible ; aucune valeur réelle remplacée par zéro.')
panel('Durée des jobs Spark par périmètre pays','waba_spark_job_duration_seconds','table','s',description='Durée du job entier. CI indique un traitement CI ; ALL indique un traitement partagé, pas une mesure indépendante par pays.')
panels[-1]['transformations']=[{'id':'organize','options':{'excludeByName':{'Time':True,'__name__':True,'instance':True,'job':True},'indexByName':{'country_scope':0,'exported_job':1,'state':2,'Value':3,'application':4},'renameByName':{'country_scope':'Pays / périmètre','exported_job':'Traitement','state':'État','Value':'Durée','application':'Application'}}}]
panels[-1]['fieldConfig']['overrides']=[{'matcher':{'id':'byName','options':name},'properties':[{'id':'custom.width','value':width}]} for name,width in [('Pays / périmètre',120),('Traitement',170),('État',110),('Durée',90),('Application',180)]]
panel('Retard Kafka — fraude / AML','waba_kafka_query_lag_messages{scenario="live"}',unit='short',description='Offsets fin broker moins offsets du dernier micro-lot Spark committé. silver_to_gold exécute fraude, AML et liquidité ; pas un consumer group Kafka indépendant.')
panel('Durée du dernier micro-lot conservé','waba_spark_stream_batch_duration_seconds',unit='s')
panel('Débit NiFi — FlowFiles sortants / seconde','waba_nifi_flowfiles_out_5m / 300',description='Moyenne calculée depuis les statistiques NiFi sur cinq minutes ; pas un compteur cumulatif.')
panel('Taux d’erreur des DAGs — dernières 24 h','waba_airflow_error_ratio_24h',unit='percentunit')
panel('Rapport réglementaire — échéance 06 h UTC','waba_regulatory_deadline_missed{scenario="live"}','stat',description='1 si le run planifié dont l’intervalle se termine aujourd’hui n’est pas réussi après 06 h UTC. Un run manuel ne valide pas l’échéance.')
panel('Fraîcheur du heartbeat streaming','waba_stream_heartbeat_age_seconds',unit='s')
panel('Signaux de démonstration des alertes','{__name__=~"waba_demo_.*"}',description='Injection de test séparée des observations réelles. Retour à zéro après vérification.')
i=len(panels);panels.append({'id':i+1,'title':'Journaux centralisés — événements JSON Spark','type':'logs','datasource':{'type':'loki','uid':'waba-loki'},'gridPos':{'x':12,'y':40,'w':12,'h':8},'targets':[{'refId':'A','expr':'{namespace="processing", event=~".*batch_committed|spark_table_lineage|regulatory_report_completed"} | json'}],'options':{'showTime':True,'showLabels':False,'wrapLogMessage':True,'sortOrder':'Descending'}})
panel('Fraîcheur globale des mesures','time() - waba_exporter_collection_timestamp_seconds','stat','s',description='Âge du dernier cycle de collecte ; une valeur ancienne rend les mesures suspectes.')
panel('Streaming attendu — 0 = arrêté','waba_stream_expected','stat',description='Le fichier de santé et les checkpoints persistent même lorsque le traitement est arrêté.')
for item in panels:
    if item['title']=='Rapport réglementaire — échéance 06 h UTC':steps=[{'color':'green','value':None},{'color':'red','value':1}]
    elif item['title']=='Disponibilité des collecteurs':steps=[{'color':'red','value':None},{'color':'green','value':1}]
    elif item['title']=='Streaming attendu — 0 = arrêté':steps=[{'color':'gray','value':None},{'color':'green','value':1}]
    elif item['title']=='Fraîcheur globale des mesures':steps=[{'color':'green','value':None},{'color':'red','value':120}]
    else:continue
    item['fieldConfig']['defaults'].update({'color':{'mode':'thresholds'},'thresholds':{'mode':'absolute','steps':steps}})
save(OUT/'dashboard.json',{'uid':'waba-health','title':'WABA — Santé des pipelines','schemaVersion':41,'version':1,'refresh':'15s','timezone':'utc','time':{'from':'now-1h','to':'now'},'tags':['WABA','local','observability'],'panels':panels})
rules=[]
for uid,title,expr,threshold,pending,desc in [
 ('waba-spark-fraud','Fraude Spark en erreur > 5 minutes','waba_spark_fraud_failed or waba_demo_spark_fraud_failed',0,'5m','Échec continu du job streaming de fraude pendant cinq minutes. scenario=demo identifie les tests.'),
 ('waba-kafka-aml','Retard Kafka AML > 5 000 messages','waba_kafka_query_lag_messages{query="silver_to_gold"} or waba_demo_kafka_aml_lag_messages',5000,'0s','Retard réel du checkpoint partagé fraude/AML ; pas de groupe AML séparé. scenario=demo identifie les tests.'),
 ('waba-regulatory','Rapport réglementaire manquant à 06 h UTC','waba_regulatory_deadline_missed or waba_demo_regulatory_deadline_missed',0,'0s','Run planifié J+1 non réussi à l’échéance UTC. scenario=demo identifie les tests.')]:
 rules.append({'uid':uid,'title':title,'condition':'B','for':pending,'noDataState':'NoData','execErrState':'Error','isPaused':False,'annotations':{'summary':desc},'labels':{'team':'waba','severity':'warning'},'data':[{'refId':'A','relativeTimeRange':{'from':600,'to':0},'datasourceUid':'waba-prometheus','model':{'refId':'A','expr':expr,'instant':True,'intervalMs':15000,'maxDataPoints':43200}},{'refId':'B','relativeTimeRange':{'from':0,'to':0},'datasourceUid':'__expr__','model':{'refId':'B','type':'threshold','expression':'A','conditions':[{'type':'query','evaluator':{'type':'gt','params':[threshold]},'operator':{'type':'and'},'reducer':{'type':'last','params':[]}}]}}]})
save(OUT/'alerting.json',{'apiVersion':1,'groups':[{'orgId':1,'name':'WABA operational','folder':'WABA','interval':'30s','rules':rules}],'contactPoints':[{'orgId':1,'name':'waba-local','receivers':[{'uid':'waba-local-webhook','type':'webhook','settings':{'url':'http://observability-exporter.processing:9108/alerts','httpMethod':'POST'},'disableResolveMessage':False}]}],'policies':[{'orgId':1,'receiver':'waba-local','group_by':['alertname','scenario'],'group_wait':'5s','group_interval':'10s','repeat_interval':'1h'}]})
items=[]
def res(kind,name,ns='monitoring',api=None,**kw):
 api=api or ('apps/v1' if kind=='Deployment' else 'v1')
 r={'apiVersion':api,'kind':kind,'metadata':{'name':name,'namespace':ns,'labels':{'app.kubernetes.io/part-of':'waba'}},**kw};items.append(r);return r
for name,ns in [('observability','monitoring'),('observability','processing')]:res('ServiceAccount',name,ns)
for ns in ['ingestion','processing','serving','governance','monitoring']:
 res('Role','waba-observe',ns,api='rbac.authorization.k8s.io/v1',rules=[{'apiGroups':[''],'resources':['pods','pods/log'],'verbs':['get','list','watch']},{'apiGroups':['apps'],'resources':['deployments'],'verbs':['get','list']},{'apiGroups':['sparkoperator.k8s.io'],'resources':['sparkapplications'],'verbs':['get','list']}])
 res('RoleBinding','waba-observe',ns,api='rbac.authorization.k8s.io/v1',roleRef={'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':'waba-observe'},subjects=[{'kind':'ServiceAccount','name':'observability','namespace':s} for s in ['monitoring','processing']])
for name,ns in [('prometheus-data','monitoring'),('loki-data','monitoring'),('grafana-data','monitoring'),('observability-events','processing')]:
 res('PersistentVolumeClaim',name,ns,spec={'accessModes':['ReadWriteOnce'],'resources':{'requests':{'storage':'1Gi'}}})
res('ConfigMap','observability-config',data={p.name:p.read_text() for p in OUT.iterdir() if p.suffix in ('.json','.alloy')})
res('ConfigMap','alloy-config','processing',data={'config.alloy':alloy})
res('ConfigMap','observability-demo','processing',data={'signals.json':json.dumps({'spark_failed':0,'aml_lag':0,'regulatory_missed':0})})
def deploy(name,image,port,memory,args=None,ns='monitoring',mounts=None,volumes=None,env=None,envFrom=None,security=None):
 res('Service',name,ns,spec={'selector':{'app':name},'ports':[{'port':port,'targetPort':port}]})
 c={'name':name,'image':image,'ports':[{'containerPort':port}],'resources':{'requests':{'memory':'64Mi','cpu':'50m'},'limits':{'memory':memory}},'readinessProbe':{'tcpSocket':{'port':port},'periodSeconds':5},'livenessProbe':{'tcpSocket':{'port':port},'initialDelaySeconds':60,'periodSeconds':20}}
 for k,v in [('args',args),('volumeMounts',mounts),('env',env),('envFrom',envFrom)]:
  if v is not None:c[k]=v
 res('Deployment',name,ns,spec={'replicas':0,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name,'app.kubernetes.io/part-of':'waba'}},'spec':{'serviceAccountName':'observability','securityContext':security or {'runAsUser':1000,'fsGroup':1000},'containers':[c],'volumes':volumes or []}}})
def cmvol(name='config'):return {'name':name,'configMap':{'name':'observability-config'}}
def datavol(name):return {'name':'data','persistentVolumeClaim':{'claimName':name+'-data'}}
def mount(file,target):return {'name':'config','mountPath':target,'subPath':file,'readOnly':True}
deploy('prometheus','prom/prometheus:v3.5.0',9090,'384Mi',args=['--config.file=/etc/prometheus/prometheus.json','--storage.tsdb.path=/prometheus','--storage.tsdb.retention.time=3d','--storage.tsdb.retention.size=700MB'],mounts=[mount('prometheus.json','/etc/prometheus/prometheus.json'),{'name':'data','mountPath':'/prometheus'}],volumes=[cmvol(),datavol('prometheus')],security={'runAsUser':65534,'fsGroup':65534})
deploy('loki','grafana/loki:3.5.5',3100,'384Mi',args=['-config.file=/etc/loki/loki.json'],mounts=[mount('loki.json','/etc/loki/loki.json'),{'name':'data','mountPath':'/loki'}],volumes=[cmvol(),datavol('loki')],security={'runAsUser':10001,'fsGroup':10001})
deploy('grafana','grafana/grafana:12.2.0',3000,'384Mi',mounts=[mount('datasources.json','/etc/grafana/provisioning/datasources/waba.yaml'),mount('providers.json','/etc/grafana/provisioning/dashboards/waba.yaml'),mount('alerting.json','/etc/grafana/provisioning/alerting/waba.yaml'),mount('dashboard.json','/var/lib/grafana/dashboards/waba.json'),{'name':'data','mountPath':'/var/lib/grafana'}],volumes=[cmvol(),datavol('grafana')],env=[{'name':'GF_PLUGINS_PREINSTALL_DISABLED','value':'true'},{'name':'GF_USERS_ALLOW_SIGN_UP','value':'false'},{'name':'GF_ANALYTICS_REPORTING_ENABLED','value':'false'},{'name':'GF_ANALYTICS_CHECK_FOR_UPDATES','value':'false'},{'name':'GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES','value':'false'}],envFrom=[{'secretRef':{'name':'waba-grafana'}}],security={'runAsUser':472,'fsGroup':472})
deploy('alloy','grafana/alloy:v1.10.2',12345,'256Mi',ns='processing',args=['run','--server.http.listen-addr=0.0.0.0:12345','--storage.path=/alloy','/etc/alloy/config.alloy'],mounts=[{'name':'config','mountPath':'/etc/alloy','readOnly':True},{'name':'logs','mountPath':'/airflow-logs','readOnly':True},{'name':'state','mountPath':'/alloy'}],volumes=[{'name':'config','configMap':{'name':'alloy-config'}},{'name':'logs','persistentVolumeClaim':{'claimName':'airflow-logs'}},{'name':'state','emptyDir':{}}],security={'runAsUser':1000,'fsGroup':1000,'supplementalGroups':[0,185]})
deploy('observability-exporter','waba/observability:20261014',9108,'192Mi',ns='processing',mounts=[{'name':'airflow','mountPath':'/airflow-state','readOnly':True},{'name':'checkpoint','mountPath':'/checkpoints','readOnly':True},{'name':'demo','mountPath':'/demo','readOnly':True},{'name':'events','mountPath':'/events'}],volumes=[{'name':'airflow','persistentVolumeClaim':{'claimName':'airflow-state'}},{'name':'checkpoint','persistentVolumeClaim':{'claimName':'stream-checkpoints'}},{'name':'demo','configMap':{'name':'observability-demo'}},{'name':'events','persistentVolumeClaim':{'claimName':'observability-events'}}],envFrom=[{'secretRef':{'name':'waba-observability'}}],security={'runAsUser':1000,'fsGroup':1000,'supplementalGroups':[0,185]})
save(ROOT/'infra/k8s/60-observability.yaml',{'apiVersion':'v1','kind':'List','items':items})
if __name__=='__main__':print('Observability manifests rendered')
