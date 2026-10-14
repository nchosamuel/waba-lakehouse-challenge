"""Real collector authentication failure; restore the private Secret-backed env."""
import fcntl
import json
import time
from datetime import datetime, timezone
from kubernetes import kube, ROOT, wait_deployment
from observability_api import prometheus, grafana


def nifi_alert_state():
    for group in grafana('/api/prometheus/grafana/api/v1/rules')['data']['groups']:
        for rule in group['rules']:
            if rule['name'] == 'Collecteur attendu indisponible':
                for alert in rule.get('alerts', []):
                    if alert.get('labels', {}).get('source') == 'nifi':
                        return alert['state']
    return 'Absent'


def wait_for(predicate, timeout=210):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            if predicate():return
        except Exception:pass  # Port-forwards reconnect during a controlled rollout.
        time.sleep(3)
    raise TimeoutError('Telemetry regression condition not reached')


def metric(expr, expected):
    rows=prometheus(expr)
    return bool(rows and float(rows[0]['value'][1])==expected)


def main():
    lock=(ROOT/'.tools/kubernetes.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    deployment=json.loads(kube('get','deployment/observability-exporter','-n','processing','-o','json',capture=True))
    assert not any(e['name']=='NIFI_ADMIN_PASSWORD' for e in deployment['spec']['template']['spec']['containers'][0].get('env',[])), 'Refuse to overwrite a pre-existing env override'
    assert metric('waba_telemetry_expected{source="nifi"}',1), 'NiFi must be expected'
    report={'started_at_utc':datetime.now(timezone.utc).isoformat(),'fault':'invalid collector-only NiFi password; actual NiFi service unchanged','external_notifications_sent':False}
    try:
        kube('set','env','deployment/observability-exporter','-n','processing','NIFI_ADMIN_PASSWORD=deliberately-invalid-for-regression')
        wait_deployment('observability-exporter','processing')
        wait_for(lambda: metric('waba_telemetry_available{source="nifi"}',0))
        wait_for(lambda: nifi_alert_state()=='Alerting')
        report['actual_failure_alert_fired']=True
        report['other_sources_fresh']=all(metric('waba_telemetry_available{source="'+source+'"}',1) for source in ('spark','kubernetes','airflow'))
        assert report['other_sources_fresh']
    finally:
        kube('set','env','deployment/observability-exporter','-n','processing','NIFI_ADMIN_PASSWORD-')
        wait_deployment('observability-exporter','processing')
    wait_for(lambda: metric('waba_telemetry_available{source="nifi"}',1) and nifi_alert_state() in ('Normal','Absent'))
    report.update({'restored_secret_backed_credentials':True,'alert_resolved':True,'completed_at_utc':datetime.now(timezone.utc).isoformat()})
    (ROOT/'docs/telemetry-failure-verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print('Actual collector outage fired the live alert; other metrics stayed fresh; recovery verified.')


if __name__=='__main__':main()
