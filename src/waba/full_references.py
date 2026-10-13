"""Generate and upload full references one file at a time, without large lists."""
import json
import csv
from botocore.exceptions import ClientError
import tempfile
import time
from pathlib import Path
from waba.reference_scale import TARGETS, write_reference, baseline, reference_row, count_for
from waba.generator import COUNTRIES
from waba.contracts import REFERENCES, SCHEMAS, KEYS
from waba.publisher import client


def compatible_existing(s3, key, name, country, details):
    """Allow additions, but never overwrite conflicting existing rows."""
    try:
        head = s3.head_object(Bucket='raw-landing', Key=key)
    except ClientError as exc:
        if exc.response['Error']['Code'] in ('404','NoSuchKey'):
            return False
        raise
    if head.get('Metadata', {}).get('sha256') == details['sha256']:
        return True
    base = {row[KEYS[name]]:row for row in baseline(country)[name]}
    prefixes = {'customers':f'WABA-{country}-SC-', 'accounts':f'WABA-{country}-SA-',
                'branches':f'WABA-{country}-SB-', 'products':f'{country}-SP-'}
    body = s3.get_object(Bucket='raw-landing', Key=key)['Body']
    try:
        reader = csv.DictReader(line.decode('utf-8') for line in body.iter_lines(keepends=True))
        if reader.fieldnames != [part.split(':')[0] for part in SCHEMAS[name].split()]:
            raise ValueError(f'Conflicting existing reference schema: {key}')
        seen = set()
        for row in reader:
            identity = row[KEYS[name]]
            if identity in seen:
                raise ValueError(f'Duplicate existing reference key: {key}')
            seen.add(identity)
            expected = base.get(identity)
            if expected is None and identity.startswith(prefixes[name]):
                try:
                    index = int(identity.rsplit('-',1)[1])
                    if 0 <= index < count_for(name,country):
                        expected = reference_row(name,country,index)
                except ValueError:
                    pass
            if expected is None or row != {k:str(v) for k,v in expected.items()}:
                raise ValueError(f'Conflicting existing reference row: {key}')
    finally:
        body.close()
    return False


def publish_full_references():
    started = time.monotonic()
    s3 = client()
    totals = {name: 0 for name in REFERENCES}
    files = []
    with tempfile.TemporaryDirectory() as folder:
        for country in COUNTRIES:
            for name in REFERENCES:
                path = Path(folder) / f'{name}.csv'
                details = write_reference(path, name, country)
                key = f'{country}/references/{name}.csv'
                unchanged = compatible_existing(s3,key,name,country,details)
                if not unchanged:
                    s3.upload_file(str(path), 'raw-landing', key, ExtraArgs={'Metadata': {
                        'profile': 'full-v1', 'rows': str(details['rows']), 'sha256': details['sha256']}})
                totals[name] += details['rows']
                files.append({'key': key, **details})
                print(json.dumps({'event': 'full_reference_uploaded', 'key': key, **details}), flush=True)
                path.unlink()
    assert totals == TARGETS, totals
    report = {'profile':'full-v1','totals':totals,'files':files,'elapsed_seconds':round(time.monotonic()-started,2)}
    s3.put_object(Bucket='archive', Key='reports/full-reference-generation.json', Body=json.dumps(report).encode())
    return report
