"""Exercise the requested default flow sizes across all eight countries."""
import csv
import io
import json
import subprocess
import uuid
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SIZES={'bank_transactions':10000,'insurance_operations':5000,'mobile_money_payments':20000,'loan_repayments':10000}
PK={'bank_transactions':'transaction_id','insurance_operations':'operation_id','mobile_money_payments':'payment_id','loan_repayments':'repayment_id'}


def run(*args):
    return subprocess.check_output(['docker','compose',*args],cwd=ROOT,text=True)


def query(sql):
    return list(csv.reader(io.StringIO(run('exec','-T','trino','trino','--output-format','CSV','--execute',sql))))


def producer(code):
    return run('run','--rm','--entrypoint','python3','spark','-c',code)


run('run','--rm','spark')
before={name:int(query(f'SELECT count(*) FROM iceberg.raw.{name}')[0][0]) for name in SIZES}
before_day={name: {row[0]:int(row[1]) for row in query(f"SELECT country_code,count(*) FROM iceberg.raw.{name} WHERE txn_date=DATE '2026-01-07' GROUP BY 1")} for name in SIZES}
batch='volume-'+uuid.uuid4().hex
reports=json.loads(producer(f'''
import json
from datetime import datetime
from waba.publisher import publish
from waba.generator import COUNTRIES
reports=[]
for name,size in {SIZES!r}.items():
    reports.append(publish(list(COUNTRIES),size,datetime(2026,1,7),datetime(2026,1,7),
                           batch_id={batch!r},seed=2026,dataset=name))
print(json.dumps(reports))
'''))
run('run','--rm','spark')
results={}
for name,size in SIZES.items():
    count,unique=map(int,query(f'SELECT count(*),count(DISTINCT {PK[name]}) FROM iceberg.raw.{name}')[0])
    assert count==unique==before[name]+size*8,(name,count,unique,before[name])
    day=query(f"SELECT country_code,count(*) FROM iceberg.raw.{name} WHERE txn_date=DATE '2026-01-07' GROUP BY 1")
    assert len(day)==8 and all(int(row[1])==before_day[name].get(row[0],0)+size for row in day), (name,day)
    results[name]={'new_rows':size*8,'countries':8,'rows_per_country':size,'total_rows':count}
# Replay a full-size 20 000-row Ghana mobile-money file.
replay_key=next(key for report in reports for key in report['files'] if key.startswith('GH/mobile_money_payments/'))
producer(f"from waba.publisher import client; client().copy_object(Bucket='raw-landing',Key={replay_key!r},CopySource={{'Bucket':'archive','Key':{replay_key!r}}})")
run('run','--rm','spark')
assert int(query('SELECT count(*) FROM iceberg.raw.mobile_money_payments')[0][0])==results['mobile_money_payments']['total_rows']
result={'default_flow_volumes_verified':True,'large_file_replay_verified':True,'batch_id':batch,'flows':results}
(ROOT/'docs'/'transaction-volume-verification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
