"""Validate exact reference volumes, uniqueness and referential integrity in Trino."""
import csv
import io
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {'customers':500000,'accounts':800000,'branches':200,'products':50}
KEYS = {'customers':'customer_id','accounts':'account_id','branches':'branch_id','products':'product_id'}


def query(sql):
    output = subprocess.check_output(['docker','compose','exec','-T','trino','trino',
                                     '--output-format','CSV','--execute',sql],cwd=ROOT,text=True)
    return list(csv.reader(io.StringIO(output)))


results = {}
for name, target in EXPECTED.items():
    rows, unique, countries = map(int, query(f'''SELECT count(*),
        count(DISTINCT ROW(country_code,{KEYS[name]})),count(DISTINCT country_code)
        FROM iceberg.raw.{name}''')[0])
    assert rows == unique == target and countries == 8, (name, rows, unique, countries)
    results[name] = {'rows':rows,'unique_keys':unique,'countries':countries}
orphans = int(query('''SELECT count(*) FROM iceberg.raw.accounts a LEFT JOIN iceberg.raw.customers c
    ON a.customer_id=c.customer_id AND a.country_code=c.country_code AND a.entity_type=c.entity_type
    WHERE c.customer_id IS NULL''')[0][0])
assert orphans == 0, orphans
result = {'full_reference_volumes_verified':True,'tables':results,'orphan_accounts':orphans}
(ROOT/'docs'/'volume-verification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
