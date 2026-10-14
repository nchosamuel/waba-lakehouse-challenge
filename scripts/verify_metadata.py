"""Verify live catalogue schema/owners/tags/lineage and automatic new-table discovery."""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import sys
import urllib.parse
import uuid
from metadata_api import MetadataApi
from kubernetes import kube,wait_job
from verification_runtime import query,output

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'infra/openmetadata'))
from curation import GOLD


def latest_report():
    code="""import boto3,json
r=boto3.client('s3',endpoint_url='http://minio:9000').get_object(Bucket='archive',Key='governance-reports/latest.json')['Body'].read()
print(r.decode())
"""
    return json.loads(output('app',['python','-c',code]))


def main():
    os.environ['WABA_RUNTIME']='kubernetes'
    api=MetadataApi();version=api.request('system/version')['version'];assert version=='1.9.0'
    checks=[]
    for name in GOLD:
        table=api.request('tables/name/waba-trino.iceberg.gold.'+name+'?fields=owners,tags,columns')
        assert table['description'] and table['owners'], name
        owner=table['owners'][0];assert owner['type']=='team' and owner['name'].startswith('WABA'), name
        assert any(t['source']=='Glossary' for t in table['tags']), name
        expected=GOLD[name][3]
        assert set(expected)<={t['tagFQN'] for t in table['tags']}, name
        checks.append({'table':name,'owner':owner['name'],'glossary_and_regulatory_tags_verified':True})
    accounts=api.request('tables/name/waba-trino.iceberg.raw.accounts?fields=columns,tags')
    for name in ('account_id','customer_id'):
        column=next(c for c in accounts['columns'] if c['name']==name)
        assert 'PII.Sensitive' in {t['tagFQN'] for t in column['tags']}
    report=latest_report();assert report['lineage_events_read']>=29 and len(report['lineage_edges'])>=42 and not report['lineage_unresolved']
    path=[('iceberg.raw.bank_transactions','iceberg.bronze.bank_transactions'),('iceberg.bronze.bank_transactions','iceberg.silver.bank_transactions'),('iceberg.silver.bank_transactions','iceberg.gold.daily_transaction_volume')]
    assert all(list(edge) in report['lineage_edges'] for edge in path)
    gold=api.request('tables/name/waba-trino.iceberg.gold.daily_transaction_volume')
    lineage=api.request('lineage/table/'+gold['id']+'?upstreamDepth=3&downstreamDepth=0')
    names={n['fullyQualifiedName'] for n in lineage['nodes']}|{lineage['entity']['fullyQualifiedName']}
    for schema in ('raw','bronze','silver'):
        assert 'waba-trino.iceberg.'+schema+'.bank_transactions' in names
    # Empty synthetic fixture proves that discovery and all required PII rules are dynamic.
    probe='governance_probe_'+uuid.uuid4().hex[:10];fqn='waba-trino.iceberg.gold.'+probe;probe_id=None
    try:
        query("CREATE TABLE iceberg.gold."+probe+" AS SELECT 'CI' country_code, 'TEST' customer_id, 'TEST' account_number, 'TEST' iban WHERE false")
        name='metadata-proof-'+uuid.uuid4().hex[:8]
        kube('create','job',name,'--from=cronjob/metadata-discovery','-n','governance');wait_job(name,'governance')
        detected=api.request('tables/name/'+fqn+'?fields=columns,tags');probe_id=detected['id']
        for column_name in ('customer_id','account_number','iban'):
            col=next(c for c in detected['columns'] if c['name']==column_name)
            assert 'PII.Sensitive' in {t['tagFQN'] for t in col['tags']}, column_name
    finally:
        query('DROP TABLE IF EXISTS iceberg.gold.'+probe)
        if probe_id:api.request('tables/'+probe_id+'?hardDelete=false&recursive=false',method='DELETE')
    final={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'openmetadata_version':version,'native_kubernetes_verified':True,
        'tables_discovered':report['tables_discovered'],'schemas':report['schemas'],'gold_curation':checks,'pii_columns_tagged':len(report['pii_columns']),
        'sensitive_identifiers_present':sorted({c['column'] for c in report['pii_columns']}),'account_number_and_iban_absent_from_original_tables':not ({'account_number','iban'} & {c['column'].lower() for c in report['pii_columns']}),
        'automatic_new_table_discovery_verified':True,'empty_probe_customer_account_number_iban_tagging_verified':True,'probe_source_dropped_and_catalog_soft_deleted':True,
        'lineage_events':report['lineage_events_read'],'lineage_edges':len(report['lineage_edges']),'raw_bronze_silver_gold_live_api_path_verified':True,
        'runtime_capture':'actual analyzed Spark DataSourceV2Relation leaves after successful CI writes','row_samples_collected':False,'external_cronjob':'metadata-discovery at 02:15 UTC, paused outside metadata mode'}
    (ROOT/'docs/openmetadata-verification.json').write_text(json.dumps(final,indent=2,ensure_ascii=False)+'\n')
    (ROOT/'docs/openmetadata-ingestion-verification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps(final,ensure_ascii=False))


if __name__=='__main__':main()
