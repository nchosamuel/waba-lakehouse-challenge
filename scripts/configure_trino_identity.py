"""Idempotently provision confidential Trino and machine OIDC clients via Admin API.

The startup import preserves realms; this updater changes only WABA-owned clients.
"""
import json
from pathlib import Path
import urllib.parse
import urllib.request
from kubernetes import private_env

BASE='http://localhost:18080'


def configure():
    values=private_env()
    form={'grant_type':'password','client_id':'admin-cli','username':values['KEYCLOAK_ADMIN_USER'],'password':values['KEYCLOAK_ADMIN_PASSWORD']}
    with urllib.request.urlopen(urllib.request.Request(BASE+'/realms/master/protocol/openid-connect/token',data=urllib.parse.urlencode(form).encode()),timeout=30) as response:
        token=json.load(response)['access_token']
    def api(path,data=None,method=None):
        req=urllib.request.Request(BASE+'/admin/realms/waba/'+path,data=json.dumps(data).encode() if data is not None else None,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'},method=method)
        with urllib.request.urlopen(req,timeout=30) as response:
            content=response.read()
            return json.loads(content) if content else None
    mapper={'name':'waba-roles','protocol':'openid-connect','protocolMapper':'oidc-usermodel-realm-role-mapper','config':{'claim.name':'waba_trino_roles','multivalued':'true','jsonType.label':'String','id.token.claim':'true','access.token.claim':'true','userinfo.token.claim':'true'}}
    audience={'name':'trino-audience','protocol':'openid-connect','protocolMapper':'oidc-audience-mapper','config':{'included.client.audience':'waba-trino','access.token.claim':'true','id.token.claim':'false'}}
    clients=[('waba-trino','KEYCLOAK_TRINO_SECRET',None),('waba-bi','KEYCLOAK_BI_SERVICE_SECRET','service_bi'),('waba-validation','KEYCLOAK_VALIDATION_SERVICE_SECRET','service_validation'),('waba-dashboard','KEYCLOAK_DASHBOARD_SERVICE_SECRET','service_dashboard'),('waba-metadata','KEYCLOAK_METADATA_SERVICE_SECRET','service_metadata')]
    for client_id,key,role in clients:
        definition={'clientId':client_id,'enabled':True,'protocol':'openid-connect','publicClient':False,'secret':values[key],
                    'standardFlowEnabled':not bool(role),'directAccessGrantsEnabled':False,'serviceAccountsEnabled':bool(role),
                    'redirectUris':['https://localhost:8843/oauth2/callback'] if not role else [],
                    'attributes':{'post.logout.redirect.uris':'https://localhost:8843/ui/logout/logout.html'},
                    'protocolMappers':[mapper,audience]}
        found=api('clients?clientId='+client_id)
        if found:
            identifier=found[0]['id'];api('clients/'+identifier,definition,'PUT')
        else:
            api('clients',definition,'POST');identifier=api('clients?clientId='+client_id)[0]['id']
        if role:
            all_roles={r['name']:r for r in api('roles')}
            if role not in all_roles:
                api('roles',{'name':role},'POST')
            role_definition=api('roles/'+role)
            user=api('clients/'+identifier+'/service-account-user')
            api('users/'+user['id']+'/role-mappings/realm',[role_definition],'POST')
    # Superset tokens carry their own audience and no Trino authorization groups.
    superset=api('clients?clientId=waba-superset')[0]
    own_audience={'name':'superset-audience','protocol':'openid-connect','protocolMapper':'oidc-audience-mapper','config':{'included.client.audience':'waba-superset','access.token.claim':'true','id.token.claim':'false'}}
    existing=api('clients/'+superset['id']+'/protocol-mappers/models')
    found=next((m for m in existing if m['name']=='superset-audience'),None)
    if found:
        api('clients/'+superset['id']+'/protocol-mappers/models/'+found['id'],dict(own_audience,id=found['id']),'PUT')
    else:
        api('clients/'+superset['id']+'/protocol-mappers/models',own_audience,'POST')
    print('Trino and scoped service clients configured; direct password grants disabled.')


if __name__=='__main__':
    configure()
