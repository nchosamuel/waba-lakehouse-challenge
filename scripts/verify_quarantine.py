"""Mix valid and rejected rows in all four flows, then replay without duplicates."""
import csv
import io
import json
import subprocess
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FLOWS = ('bank_transactions','insurance_operations','mobile_money_payments','loan_repayments')
PK = dict(zip(FLOWS, ('transaction_id','operation_id','payment_id','repayment_id')))


def run(*args):
    return subprocess.check_output(['docker','compose',*args],cwd=ROOT,text=True)


def query(sql):
    return next(csv.reader(io.StringIO(run('exec','-T','trino','trino','--output-format','CSV','--execute',sql))))


def producer(code):
    return run('run','--rm','--entrypoint','python3','spark','-c',code)


run('run','--rm','spark')
before = {name:int(query(f'SELECT count(*) FROM iceberg.raw.{name}')[0]) for name in FLOWS}
batch = 'quality-' + uuid.uuid4().hex
code = f'''
import csv, io, json, tempfile, uuid
from datetime import datetime
from waba.generator import generate
from waba.contracts import FLOW_PREFIX, KEYS
from waba.publisher import client
with tempfile.TemporaryDirectory() as folder:
    data=generate(folder,'CI',8,start=datetime(2026,1,6),end=datetime(2026,1,6),batch_id={batch!r})
s3=client()
keys=[]
for name in {FLOWS!r}:
    rows=data[name]
    amount='amount_due' if name=='loan_repayments' else 'amount'
    rows[2][amount]=-1
    orphan={{'bank_transactions':'account_id','insurance_operations':'account_id',
             'mobile_money_payments':'sender_id','loan_repayments':'loan_account_id'}}[name]
    rows[3][orphan]='DOES-NOT-EXIST'
    rows[4][amount]='not-a-number'
    rows[5][KEYS[name]]=rows[6][KEYS[name]] # Both duplicated source rows must be rejected.
    stream=io.StringIO()
    writer=csv.writer(stream)
    columns=list(rows[0])
    writer.writerow(columns)
    for index,row in enumerate(rows):
        values=[row[c] for c in columns]
        if index==7:
            values.append('unexpected-extra-column')
        writer.writerow(values)
    key=f'CI/{{name}}/2026-01-06/{{FLOW_PREFIX[name]}}_CI_20260106_{batch}.csv'
    s3.put_object(Bucket='raw-landing',Key=key,Body=stream.getvalue().encode())
    keys.append(key)
print(json.dumps(keys))
'''
keys = json.loads(producer(code))
reports = []
for iteration in (1,2):
    if iteration == 2:
        producer(f'''
from waba.publisher import client
s3=client()
for key in {keys!r}:
    s3.copy_object(Bucket='raw-landing',Key=key,CopySource={{'Bucket':'archive','Key':key}})
''')
    run('run','--rm','spark')
    check = f'''
import json
from waba.publisher import client
s3=client()
reports=[]
for key in {keys!r}:
    report=json.loads(s3.get_object(Bucket='archive',Key=f'reports/ingestion/{{key}}.json')['Body'].read())
    rejected=[json.loads(line) for line in s3.get_object(Bucket='archive',Key=f'quarantine/{{key}}.jsonl')['Body'].read().decode().splitlines()]
    assert report['accepted_rows']==2 and report['rejected_rows']==6, report
    assert len(rejected)==6
    assert all(r['reject_reason'] and r['source_file']==key and r['_raw_record'] for r in rejected)
    try:
        s3.head_object(Bucket='raw-landing',Key=key)
        raise AssertionError('Processed file still in landing')
    except s3.exceptions.ClientError as exc:
        assert exc.response['Error']['Code']=='404', exc
    reports.append(report)
print(json.dumps(reports))
'''
    reports = json.loads(producer(check))
    for name in FLOWS:
        count,unique=map(int, query(f'SELECT count(*),count(DISTINCT {PK[name]}) FROM iceberg.raw.{name}'))
        assert count==unique==before[name]+2, (name,count,unique,before[name])
result = {'mixed_files_verified':True,'all_flow_replay_verified':True,'accepted_rows':8,'rejected_rows':24,'files':reports}
(ROOT/'docs'/'quarantine-verification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
