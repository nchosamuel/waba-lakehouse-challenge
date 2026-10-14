"""Trigger all three real Grafana rules using labeled demo metrics; reset in finally."""
import json
import time
from datetime import datetime,timezone
from kubernetes import kube,ROOT
from observability_api import grafana,prometheus,loki


def signals(spark=0,aml=0,regulatory=0):
    body={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'observability-demo','namespace':'processing'},'data':{'signals.json':json.dumps({'spark_failed':spark,'aml_lag':aml,'regulatory_missed':regulatory})}}
    kube('apply','-f','-',data=json.dumps(body))


def states():
    rules=grafana('/api/prometheus/grafana/api/v1/rules')['data']['groups']
    return {r['name'].removesuffix(' · démo'):next((a['state'] for a in r.get('alerts',[]) if a.get('labels',{}).get('scenario')=='demo'),'Absent') for g in rules for r in g['rules'] if r['name'].endswith(' · démo')}


def main():
    report={'started_at_utc':datetime.now(timezone.utc).isoformat(),'signals_are_demo_only':True,'external_notifications_sent':False,'spark_pending_period_seconds':300,'kafka_threshold_messages':5000}
    try:
        signals(aml=5000)
        deadline=time.monotonic()+150
        while time.monotonic()<deadline:
            sample=prometheus('waba_demo_kafka_aml_lag_messages')
            if sample and float(sample[0]['value'][1])==5000:break
            time.sleep(3)
        else:raise TimeoutError('Demo signals not scraped')
        time.sleep(35)  # One complete evaluation interval; threshold is strictly greater than 5000.
        before=states();assert before['Retard Kafka AML > 5 000 messages']=='Normal',before
        report['kafka_exact_threshold_does_not_fire']=True
        signals(1,5001,1);injected=time.monotonic();pending_seen=False
        deadline=time.monotonic()+540
        while time.monotonic()<deadline:
            current=states()
            spark=current['Fraude Spark en erreur > 5 minutes']
            if spark=='Pending':pending_seen=True
            if all(value=='Alerting' for value in current.values()):
                report['firing_states']=current;report['elapsed_since_signal_change_seconds']=round(time.monotonic()-injected,1);break
            print(json.dumps({'event':'alert_proof_wait','states':current,'elapsed_seconds':round(time.monotonic()-injected)}),flush=True)
            time.sleep(15)
        else:raise TimeoutError('Three labeled demo alert instances did not fire')
        assert pending_seen,'Spark pending state not observed'
        assert report['elapsed_since_signal_change_seconds']>=300
        report['spark_pending_then_firing_verified']=True
        # Verify actual local notification delivery, not only an evaluation state.
        deadline=time.monotonic()+90
        while time.monotonic()<deadline:
            rows=loki('{event="alert_notification"} | json')
            delivered={}
            for row in rows:
                for _,line in row['values']:
                    event=json.loads(line)
                    if (event.get('labels',{}).get('scenario')=='demo' and event.get('status')=='firing'
                            and event.get('timestamp','') >= report['started_at_utc']
                            and event.get('labels',{}).get('alertname','').endswith(' · démo')):
                        delivered[event['labels']['alertname']]=True
            if len(delivered)==3:report['local_firing_notifications']=sorted(delivered);break
            time.sleep(5)
        else:raise TimeoutError('Three demo notification deliveries not observed in Loki')
    finally:
        signals()
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        current=states()
        if len(current)==3 and all(value=='Normal' for value in current.values()):report['demo_resolved_states']=current;break
        time.sleep(5)
    else:raise TimeoutError('Demo alerts did not resolve after reset')
    report['demo_signals_reset']=True;report['completed_at_utc']=datetime.now(timezone.utc).isoformat()
    (ROOT/'docs/observability-alert-verification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print('Three Grafana alert rules fired, notified locally and returned to normal in demo scope.')


if __name__=='__main__':main()
