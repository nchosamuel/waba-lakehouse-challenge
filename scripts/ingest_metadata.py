"""External Trino discovery, curation and runtime Spark lineage ingestion.

Runs as a Kubernetes Job/CronJob with private credentials. No data profiling,
row samples or financial values are collected; SQL reads information_schema.
"""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import sys
import urllib.parse
import urllib.error
import boto3
from waba.trino_client import query
from metadata_api import MetadataApi
from curation import GOLD,TEAMS,TERMS,PII_FIELDS

SERVICE='waba-trino'


def label(name,source='Classification'):
    return {'tagFQN':name,'source':source,'labelType':'Automated','state':'Confirmed'}


def data_type(value):
    root=value.split('(')[0].lower()
    return {'varchar':'STRING','char':'CHAR','bigint':'BIGINT','integer':'INT','smallint':'SMALLINT','tinyint':'TINYINT','double':'DOUBLE','real':'FLOAT','decimal':'DECIMAL','boolean':'BOOLEAN','date':'DATE','timestamp':'TIMESTAMP','timestamp with time zone':'TIMESTAMPZ','varbinary':'BINARY'}.get(root,'STRING')


def main():
    api=MetadataApi()
    for name,description in [('WABARegulatory','Périmètres réglementaires des agrégats de démonstration ; ne prouve pas une conformité officielle.'),('WABAPrivacy','Classification de sensibilité ; données simulées. Applicabilité RGPD/lois locales à évaluer pour un usage réel.')]:
        api.request('classifications',{'name':name,'description':description},'PUT')
    for name,description in [('BCEAO','Agrégat de démonstration avec périmètre BCEAO explicite.'),('CIMA','Agrégat de démonstration avec périmètre CIMA explicite.')]:
        api.request('tags',{'name':name,'classification':'WABARegulatory','description':description},'PUT')
    api.request('tags',{'name':'FinancialSensitive','classification':'WABAPrivacy','description':"Montant d'assurance individuel lié à un assuré (prime ou sinistre) ; accès restreint."},'PUT')
    # Built-in PII classification is used rather than a parallel invented standard.
    api.request('tags/name/PII.Sensitive')
    parent=api.request('teams',{'name':'WABA','displayName':'WABA Groupe','description':'Entités propriétaires des données du challenge simulé.','teamType':'BusinessUnit'},'PUT')
    owners={}
    for entity,title in TEAMS.items():
        team=api.request('teams',{'name':'WABA_'+entity,'displayName':title,'description':'Propriétaire métier '+title,'teamType':'Group','parents':[parent['id']]},'PUT')
        owners[entity]={'id':team['id'],'type':'team'}
    ownership={}
    for entities,term,description,tags in GOLD.values():
        if len(entities)==1:
            ownership[tuple(entities)]=[owners[entities[0]]]
        else:
            team=api.request('teams',{'name':'WABA_'+'_'.join(entities),'displayName':' / '.join(TEAMS[e] for e in entities),
                'description':'Équipe commune propriétaire des indicateurs de ces entités WABA.','teamType':'Group','parents':[parent['id']]},'PUT')
            ownership[tuple(entities)]=[{'id':team['id'],'type':'team'}]
    api.request('glossaries',{'name':'WABAFinance','displayName':'Glossaire financier WABA','description':'Définitions et hypothèses financières de démonstration.'},'PUT')
    for term,description in TERMS.items():
        api.request('glossaryTerms',{'name':term,'displayName':term,'glossary':'WABAFinance','description':description},'PUT')
    api.request('services/databaseServices',{'name':SERVICE,'serviceType':'Trino','description':'Découverte automatique externe de Trino sécurisé. Identité dédiée waba-metadata, HTTPS/CA vérifiée, secrets Kubernetes ; aucun échantillon de données collecté.'},'PUT')
    database=api.request('databases',{'name':'iceberg','service':SERVICE,'description':'Catalogue Iceberg du lac WABA.'},'PUT')
    rows=query("SELECT table_schema,table_name,column_name,data_type,ordinal_position FROM iceberg.information_schema.columns WHERE table_schema IN ('raw','bronze','silver','gold','bi') ORDER BY 1,2,5")[1]
    kinds={(s,t):kind for s,t,kind in query("SELECT table_schema,table_name,table_type FROM iceberg.information_schema.tables WHERE table_schema IN ('raw','bronze','silver','gold','bi')")[1]}
    grouped={}
    for schema,table,column,typ,ordinal in rows:
        grouped.setdefault((schema,table),[]).append((column,typ,ordinal))
    tables={};pii=[];curated=[]
    for schema in sorted({s for s,t in grouped}):
        api.request('databaseSchemas',{'name':schema,'database':database['fullyQualifiedName']},'PUT')
    for (schema,name),columns in sorted(grouped.items()):
        data={'name':name,'tableType':'View' if kinds.get((schema,name))=='VIEW' else 'Regular','databaseSchema':SERVICE+'.iceberg.'+schema,'columns':[]}
        for column,typ,ordinal in columns:
            entry={'name':column,'dataType':data_type(typ),'dataTypeDisplay':typ,'ordinalPosition':ordinal}
            tags=[]
            if column.lower() in PII_FIELDS:
                entry['description']='Identifiant personnel simulé : accès restreint, protection RGPD/lois locales à apprécier selon le périmètre réel.'
                tags.append(label('PII.Sensitive'));pii.append({'table':schema+'.'+name,'column':column})
            if schema in ('raw','bronze','silver') and name=='insurance_operations' and column in ('amount','amount_eur'):
                tags.append(label('WABAPrivacy.FinancialSensitive'))
            if tags:entry['tags']=tags
            if entry['dataType']=='DECIMAL':
                match=re.search(r'decimal\((\d+),\s*(\d+)\)',typ)
                if match:entry.update(precision=int(match[1]),scale=int(match[2]))
            data['columns'].append(entry)
        if schema=='gold' and name in GOLD:
            entities,term,description,tags=GOLD[name]
            data.update(description=description,owners=ownership[tuple(entities)],tags=[label(t) for t in tags]+[label('WABAFinance.'+term,'Glossary')])
            curated.append(name)
        table=api.request('tables',data,'PUT');tables['iceberg.'+schema+'.'+name]=table
    s3=boto3.client('s3',endpoint_url='http://minio.ingestion:9000')
    api.request('services/pipelineServices',{'name':'WABA_Spark','serviceType':'Spark','description':'Traçabilité réelle des traitements Spark Kubernetes, capturée après écriture réussie.'},'PUT')
    pipelines={}
    edges=set();events=0;missing=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket='archive',Prefix='governance-lineage/'):
        for obj in page.get('Contents',[]):
            event=json.loads(s3.get_object(Bucket='archive',Key=obj['Key'])['Body'].read());events+=1
            app_name=event['application_name']
            if app_name not in pipelines:
                pipelines[app_name]=api.request('pipelines',{'name':app_name,'service':'WABA_Spark','description':'Traitement Spark natif ; dépendances extraites du plan analysé après écriture réussie.'},'PUT')
            for source in event['inputs']:
                target=event['output']
                if source not in tables or target not in tables:
                    missing.append([source,target]);continue
                pair=(source,target)
                if pair in edges:continue
                api.request('lineage',{'edge':{'fromEntity':{'id':tables[source]['id'],'type':'table'},'toEntity':{'id':tables[target]['id'],'type':'table'},'lineageDetails':{'description':'Capture automatique du plan Spark analysé après écriture réussie : '+event['application_name'],'source':'PipelineLineage','pipeline':{'id':pipelines[app_name]['id'],'type':'pipeline'}}}},'PUT')
                edges.add(pair)
    report={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'source':'Trino information_schema via dedicated OAuth2 client and verified HTTPS','tables_discovered':len(tables),'schemas':sorted({s for s,t in grouped}),'gold_curated':curated,'pii_columns':pii,'lineage_events_read':events,'lineage_edges':sorted([list(e) for e in edges]),'lineage_unresolved':missing,'row_samples_collected':False}
    s3.put_object(Bucket='archive',Key='governance-reports/latest.json',Body=json.dumps(report,indent=2).encode(),ContentType='application/json')
    print(json.dumps(report))


if __name__=='__main__':main()
