"""Exercise all eight raw tables and re-ingestion across CI/GH."""
import csv
import io
import json
import subprocess
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FLOW_NAMES = ('bank_transactions','insurance_operations','mobile_money_payments','loan_repayments')
REFERENCE_NAMES = ('customers','accounts','branches','products')
PRIMARY = dict(zip(FLOW_NAMES, ('transaction_id','operation_id','payment_id','repayment_id')))


def run(*args):
    return subprocess.check_output(['docker','compose',*args],cwd=ROOT,text=True)


def query(sql):
    return list(csv.reader(io.StringIO(run('exec','-T','trino','trino','--output-format','CSV','--execute',sql))))


def producer(code):
    return run('run','--rm','--entrypoint','python3','spark','-c',code)


run('run','--rm','spark')
tables = {r[0] for r in query('SHOW TABLES FROM iceberg.raw')}
before = {n: int(query(f'SELECT count(*) FROM iceberg.raw.{n}')[0][0]) if n in tables else 0 for n in FLOW_NAMES}
batch = 'raw-check-' + uuid.uuid4().hex
code = f'''
import json
from datetime import datetime
from waba.publisher import publish
reports=[]
for name in {FLOW_NAMES!r}:
    reports.append(publish(['CI','GH'], 10, datetime(2026,1,4), datetime(2026,1,4),
                           batch_id={batch!r}, seed=77, dataset=name))
print(json.dumps(reports))
'''
reports = json.loads(producer(code))
keys = [k for report in reports for k in report['files']]
results = []
for iteration in (1,2):
    if iteration == 2:
        producer(f'''
from waba.publisher import client
s3=client()
for key in {keys!r}:
    s3.copy_object(Bucket='raw-landing',Key=key,CopySource={{'Bucket':'archive','Key':key}})
''')
    run('run','--rm','spark')
    tables = {r[0] for r in query('SHOW TABLES FROM iceberg.raw')}
    assert tables == set(FLOW_NAMES + REFERENCE_NAMES), tables
    counts = {}
    for name in FLOW_NAMES:
        count, unique = map(int,query(f'SELECT count(*), count(DISTINCT {PRIMARY[name]}) FROM iceberg.raw.{name}')[0])
        assert count == before[name]+20, (name,count,before[name])
        assert count == unique, (name,count,unique)
        counts[name] = count
        for country,currency in [('CI','XOF'),('GH','GHS')]:
            assert int(query(f"SELECT count(*) FROM iceberg.raw.{name} WHERE country_code='{country}' AND currency <> '{currency}'")[0][0]) == 0
    results.append({'iteration':iteration,'counts':counts})
for name in REFERENCE_NAMES:
    for country in ('CI','GH'):
        assert int(query(f"SELECT count(*) FROM iceberg.raw.{name} WHERE country_code='{country}'")[0][0]) > 0
report = {'all_eight_tables_verified':True,'countries':['CI','GH'],'all_flow_replay_verified':True,'results':results}
(ROOT/'docs'/'raw-verification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))
