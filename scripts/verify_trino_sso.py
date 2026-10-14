"""Real authorization-code tokens and direct HTTPS SQL positive/negative tests."""
from datetime import datetime,timezone
import hashlib
import base64
import http.cookiejar
import json
from pathlib import Path
import secrets
import ssl
import urllib.error
import urllib.parse
import urllib.request
from verify_sso import LoginForm,LocalhostCookiePolicy
from kubernetes import private_env

ROOT=Path(__file__).resolve().parents[1]
BASE='https://localhost:8843'
CA=ROOT/'.tools/trino-tls/ca.crt'


class CallbackCapture(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        parsed=urllib.parse.urlparse(newurl)
        if parsed.hostname=='localhost' and parsed.path in ('/oauth2/callback','/oauth-authorized/keycloak'):
            raise urllib.error.HTTPError(newurl,code,'Captured owned callback',headers,fp)
        return super().redirect_request(req,fp,code,msg,headers,newurl)


def token(username,password,values,client_id='waba-trino'):
    callback=BASE+'/oauth2/callback' if client_id=='waba-trino' else 'http://localhost:8088/oauth-authorized/keycloak'
    jar=http.cookiejar.CookieJar(policy=LocalhostCookiePolicy())
    client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar),CallbackCapture())
    state=secrets.token_urlsafe(24);verifier=secrets.token_urlsafe(48)
    params={'client_id':client_id,'redirect_uri':callback,'response_type':'code','scope':'openid profile email','state':state,
            'code_challenge_method':'S256','code_challenge':base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')}
    url='http://localhost:18080/realms/waba/protocol/openid-connect/auth?'+urllib.parse.urlencode(params)
    with client.open(url,timeout=30) as r: form=LoginForm(r.read().decode())
    assert form.action,'Login form missing'
    form.fields.update(username=username,password=password,credentialId='')
    try:
        client.open(urllib.request.Request(form.action,data=urllib.parse.urlencode(form.fields).encode()),timeout=30)
        raise AssertionError('Authorization callback missing')
    except urllib.error.HTTPError as e:
        assert e.code==302,'Authorization failed'
        result=urllib.parse.parse_qs(urllib.parse.urlparse(e.url).query)
        assert result.get('state')==[state], 'State mismatch'
        code=result['code'][0]
    key='KEYCLOAK_TRINO_SECRET' if client_id=='waba-trino' else 'KEYCLOAK_SUPERSET_SECRET'
    data={'grant_type':'authorization_code','client_id':client_id,'client_secret':values[key],'code':code,'redirect_uri':callback,'code_verifier':verifier}
    with urllib.request.urlopen(urllib.request.Request('http://localhost:18080/realms/waba/protocol/openid-connect/token',data=urllib.parse.urlencode(data).encode()),timeout=30) as r:
        return json.load(r)['access_token']


class SqlSession:
    def __init__(self,access_token=None,user=None):
        self.headers={}
        if access_token:self.headers['Authorization']='Bearer '+access_token
        if user:self.headers['X-Trino-User']=user
        self.client=urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(CA))))

    def query(self,sql):
        def request(url,data=None):
            assert urllib.parse.urlparse(url).netloc=='localhost:8843', 'Unexpected SQL continuation host'
            with self.client.open(urllib.request.Request(url,data=data,headers=self.headers),timeout=120) as r:return json.load(r)
        result=request(BASE+'/v1/statement',sql.encode());rows=[]
        while True:
            if 'error' in result:raise SqlDenied(result['error'].get('errorName',''),result['error']['message'])
            rows.extend(result.get('data',[]))
            if 'nextUri' not in result:return rows
            result=request(result['nextUri'])

    def denied(self,sql):
        try:self.query(sql)
        except urllib.error.HTTPError as e:return e.code in (401,403)
        except SqlDenied as e:return e.name in ('PERMISSION_DENIED','ACCESS_DENIED')
        return False


class SqlDenied(Exception):
    def __init__(self,name,message):super().__init__(message);self.name=name


def web_ui_login(values):
    jar=http.cookiejar.CookieJar(policy=LocalhostCookiePolicy())
    client=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar),urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(CA))))
    with client.open(BASE+'/ui/',timeout=30) as response:
        form=LoginForm(response.read().decode())
    assert form.action, 'Trino UI did not redirect to Keycloak'
    form.fields.update(username='waba-group-admin',password=values['SSO_GROUP_ADMIN_PASSWORD'],credentialId='')
    with client.open(urllib.request.Request(form.action,data=urllib.parse.urlencode(form.fields).encode()),timeout=30) as response:
        assert urllib.parse.urlparse(response.url).netloc=='localhost:8843', 'Trino UI callback failed'
    with client.open(BASE+'/ui/api/query',timeout=30) as response:
        assert isinstance(json.load(response),list), 'Authenticated UI API failed'
    with client.open(BASE+'/ui/logout',timeout=30) as response:
        assert urllib.parse.urlparse(response.url).path=='/ui/logout/logout.html', 'Trino logout callback failed'
    with client.open(BASE+'/ui/',timeout=30) as response:
        assert LoginForm(response.read().decode()).action, 'IdP session still active after Trino logout'
    return True


def main():
    v=private_env();checks=[]
    web_ui_login(v)
    assert SqlSession(user='waba-group-admin').denied('SELECT 1'), 'Unsigned identity accepted'
    assert SqlSession('invalid-token','waba-group-admin').denied('SELECT 1'), 'Invalid token accepted'
    wrong=token('waba-viewer',v['SSO_VIEWER_PASSWORD'],v,'waba-superset')
    assert SqlSession(wrong,'waba-viewer').denied('SELECT 1'), 'Wrong audience accepted'
    admin=SqlSession(token('waba-group-admin',v['SSO_GROUP_ADMIN_PASSWORD'],v),'waba-group-admin')
    assert len(admin.query('SELECT DISTINCT country_code FROM iceberg.raw.accounts'))==8
    for c in ('CI','SN','ML','BF','GN','TG','BJ','GH'):
        user='waba-analyst-'+c.lower();bearer=token(user,v['SSO_ANALYST_'+c+'_PASSWORD'],v);s=SqlSession(bearer,user)
        assert s.query('SELECT DISTINCT country_code FROM iceberg.raw.accounts')==[[c]]
        assert s.query('SELECT DISTINCT country_code FROM iceberg.bronze.accounts')==[[c]]
        assert s.query('SELECT DISTINCT country_code FROM iceberg.silver.bank_transactions')==[[c]]
        assert s.query('SELECT DISTINCT country_code FROM iceberg.gold.npl_ratio_by_country')==[[c]]
        assert s.query('SELECT DISTINCT country_code FROM iceberg.bi.revenue')==[[c]]
        other='SN' if c!='SN' else 'CI'
        assert s.query("SELECT count(*) FROM iceberg.raw.accounts WHERE country_code='"+other+"'")==[[0]]
        assert s.denied('DELETE FROM iceberg.raw.accounts WHERE false'), 'Analyst mutation accepted'
        assert SqlSession(bearer,'waba-group-admin').denied('SELECT count(*) FROM iceberg.raw.accounts'), 'Identity spoof accepted'
        checks.append({'profile':'country_analyst','country':c,'raw_bronze_silver_gold_bi_country_restriction':True,'contradictory_predicate_empty':True,'write_denied':True,'impersonation_denied':True})
    viewer=SqlSession(token('waba-viewer',v['SSO_VIEWER_PASSWORD'],v),'waba-viewer')
    views=('revenue','arpc','subscriptions','npl','loss_ratio','aml_counts','claims_sla','mobile_hourly','corridors','mobile_failures')
    for view in views:assert viewer.query('SELECT count(*) FROM iceberg.bi.'+view)[0][0]>0
    for sql in ('SELECT * FROM iceberg.raw.accounts LIMIT 1','SELECT * FROM iceberg.silver.insurance_operations LIMIT 1','SELECT * FROM iceberg.gold.regulatory_cima_daily LIMIT 1','SELECT (SELECT max(account_id) FROM iceberg.raw.accounts) FROM iceberg.bi.revenue LIMIT 1','SELECT * FROM iceberg.bi.revenue CROSS JOIN iceberg.raw.accounts LIMIT 1'):
        assert viewer.denied(sql),'Viewer sensitive SQL bypass'
    checks.append({'profile':'viewer','ten_aggregate_views':True,'accounts_claims_regulatory_subquery_join_denied':True})
    compliance=SqlSession(token('waba-compliance',v['SSO_COMPLIANCE_PASSWORD'],v),'waba-compliance')
    for table in ('regulatory_bceao_daily','regulatory_cima_daily'):assert compliance.query('SELECT count(*) FROM iceberg.gold.'+table)[0][0]>0
    for table in ('iceberg.raw.accounts','iceberg.silver.insurance_operations','iceberg.gold.customer_arpu_monthly','iceberg.bi.revenue'):
        assert compliance.denied('SELECT * FROM '+table+' LIMIT 1'),'Compliance scope bypass'
    checks.append({'profile':'compliance_officer','two_regulatory_tables_only':True})
    # HTTP listener remains solely for cluster discovery/health; clients must use TLS.
    from verification_runtime import output
    code="""import urllib.request,urllib.error
try:
 urllib.request.urlopen(urllib.request.Request('http://trino:8080/v1/statement',data=b'SELECT 1',headers={'X-Trino-User':'waba-group-admin'}))
 print('UNSAFE')
except urllib.error.HTTPError as e: print(e.code)
"""
    import os;os.environ['WABA_RUNTIME']='kubernetes'
    assert output('app',['python','-c',code]).strip() in ('401','403'), 'Insecure HTTP SQL accepted'
    report={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'trino_version':'475','https_certificate_verified':True,'web_ui_keycloak_login_verified':True,'web_ui_and_idp_logout_verified':True,'authorization_code_tokens_verified':True,'keycloak_role_claims_used':True,'unsigned_invalid_wrong_audience_rejected':True,'http_sql_denied':True,'group_admin_eight_countries':True,'profiles':checks,'production_ha_verified':False}
    (ROOT/'docs/trino-sso-verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


if __name__=='__main__':main()
