"""Local verification client. Credentials stay private and are never printed."""
import base64
import json
import urllib.parse
import urllib.request
from kubernetes import private_env


def grafana(path):
    values=private_env()
    auth=base64.b64encode((values['GRAFANA_ADMIN_USER']+':'+values['GRAFANA_ADMIN_PASSWORD']).encode()).decode()
    return json.load(urllib.request.urlopen(urllib.request.Request('http://localhost:3000'+path,headers={'Authorization':'Basic '+auth}),timeout=15))


def prometheus(expr):
    result=json.load(urllib.request.urlopen('http://localhost:9090/api/v1/query?query='+urllib.parse.quote(expr),timeout=15))
    if result['status']!='success':raise RuntimeError('Prometheus query failed')
    return result['data']['result']


def loki(expr,lookback_seconds=86400):
    import time
    query=urllib.parse.urlencode({'query':expr,'start':str(int((time.time()-lookback_seconds)*1e9)),'end':str(int(time.time()*1e9)),'limit':'100'})
    result=json.load(urllib.request.urlopen('http://localhost:3100/loki/api/v1/query_range?'+query,timeout=15))
    if result['status']!='success':raise RuntimeError('Loki query failed')
    return result['data']['result']


if __name__=='__main__':
    print('Datasources:',[(x['name'],x['uid']) for x in grafana('/api/datasources')])
    print('Telemetry:',[(x['metric'].get('source'),x['value'][1]) for x in prometheus('waba_telemetry_available')])
    rules=grafana('/api/prometheus/grafana/api/v1/rules')
    for group in rules['data']['groups']:
        for rule in group['rules']:
            print(rule['name'],rule.get('state'),[(x.get('labels',{}).get('scenario'),x.get('state')) for x in rule.get('alerts',[])])
    print('JSON Spark log streams:',len(loki('{namespace="processing", event="spark_table_lineage"} | json')))
