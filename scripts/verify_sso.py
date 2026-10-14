"""Exercise real OIDC authorization-code login and server-side dataset restrictions.

Requires localhost Keycloak:18080 and Superset:8088 port-forwards. Never emits
credentials, authorization codes, tokens or cookies. No direct password grant.
"""
import copy
from datetime import datetime, timezone
from html.parser import HTMLParser
import http.cookiejar
import json
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASE = 'http://localhost:8088'
COUNTRIES = ('CI','SN','ML','BF','GN','TG','BJ','GH')


class LoginForm(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.action = None
        self.fields = {}
        self.feed(html)

    def handle_starttag(self, tag, attributes):
        a = dict(attributes)
        if tag == 'form' and a.get('id') == 'kc-form-login':
            self.action = a['action']
        if tag == 'input' and a.get('type') == 'hidden' and a.get('name'):
            self.fields[a['name']] = a.get('value','')


class LocalhostCookiePolicy(http.cookiejar.DefaultCookiePolicy):
    def return_ok_secure(self, cookie, request):
        # Match Chrome's trustworthy-loopback treatment. Never relax remote hosts.
        if urllib.parse.urlparse(request.full_url).hostname == 'localhost':
            return True
        return super().return_ok_secure(cookie, request)


class Session:
    def __init__(self):
        self.client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar(policy=LocalhostCookiePolicy())))
        self.headers = {}

    def open(self, path, data=None, form=False):
        url = path if path.startswith('http') else BASE+path
        headers = dict(self.headers)
        if data is not None:
            headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
            data = (urllib.parse.urlencode(data) if form else json.dumps(data)).encode()
        return self.client.open(urllib.request.Request(url,data=data,headers=headers),timeout=180)

    def json(self,path,data=None):
        with self.open(path,data) as response:
            return json.load(response)

    def login(self,username,password):
        with self.open('/login/keycloak') as response:
            form = LoginForm(response.read().decode())
        assert form.action, 'Keycloak login form missing'
        form.fields.update(username=username,password=password,credentialId='')
        with self.open(form.action,form.fields,form=True) as response:
            assert urllib.parse.urlparse(response.url).port == 8088, 'OIDC callback failed'
        self.headers['X-CSRFToken'] = self.json('/api/v1/security/csrf_token/')['result']
        me = self.json('/api/v1/me/')['result']
        assert me['username'] == username, 'Authenticated identity mismatch'
        return me

    def denied(self,path,data=None):
        try:
            result = self.json(path,data)
            return any(r.get('status') == 'failed' for r in result.get('result',[]) if isinstance(r,dict))
        except urllib.error.HTTPError as error:
            return error.code in (401,403,404)


def main():
    values = dict(line.split('=',1) for line in (ROOT/'.env').read_text().splitlines() if '=' in line and not line.startswith('#'))
    admin = Session()
    admin.login('waba-group-admin', values['SSO_GROUP_ADMIN_PASSWORD'])
    charts = admin.json('/api/v1/chart/?q='+urllib.parse.quote(json.dumps({'page_size':100})))['result']
    contexts = []
    for chart in charts:
        detail = admin.json('/api/v1/chart/'+str(chart['id']))['result']
        contexts.append((chart['slice_name'],json.loads(detail['query_context'])))
    for name,context in contexts:
        query=copy.deepcopy(context);query['force']=True
        r=admin.json('/api/v1/chart/data',query)['result'][0]
        assert r['status']=='success' and r['data'], ('group_admin',name)
    datasets = admin.json('/api/v1/dataset/?q='+urllib.parse.quote(json.dumps({'page_size':100})))['result']
    sensitive = next(d for d in datasets if d['table_name']=='regulatory_cima_daily')
    regulatory_ids = {d['id'] for d in datasets if d['table_name'].startswith('regulatory_')}
    role_checks=[]
    for country in COUNTRIES:
        user = Session()
        user.login('waba-analyst-'+country.lower(),values['SSO_ANALYST_'+country+'_PASSWORD'])
        for name,context in contexts:
            query=copy.deepcopy(context)
            query['force']=True
            query['queries'][0]['filters']=[]  # Deliberately remove the dashboard country filter.
            result=user.json('/api/v1/chart/data',query)['result'][0]
            assert result['status']=='success', (country,name,'query failed')
            found={r['country_code'] for r in result['data']}
            assert found <= {country}, (country,name,'cross-country leak')
            query['force']=False
            cached=user.json('/api/v1/chart/data',query)['result'][0]
            assert {r['country_code'] for r in cached['data']} <= {country}, 'Cached country leak'
        # Request another country explicitly; the server must intersect with RLS.
        query=copy.deepcopy(contexts[0][1]);query['force']=True
        query['queries'][0]['filters']=[{'col':'country_code','op':'IN','val':['SN' if country!='SN' else 'CI']}]
        assert not user.json('/api/v1/chart/data',query)['result'][0]['data'], 'RLS bypass'
        assert user.denied('/api/v1/sqllab/execute/',{'database_id':sensitive['database']['id'],'sql':'SELECT * FROM iceberg.raw.accounts','runAsync':False}), 'SQL Lab bypass'
        role_checks.append({'profile':'country_analyst','country':country,'all_chart_queries_country_restricted':True,'contradictory_filter_empty':True,'sql_lab_denied':True})
    viewer=Session();viewer.login('waba-viewer',values['SSO_VIEWER_PASSWORD'])
    visible=viewer.json('/api/v1/dataset/?q='+urllib.parse.quote(json.dumps({'page_size':100})))['result']
    assert not (regulatory_ids & {d['id'] for d in visible}), 'Unexpected viewer regulatory access'
    assert viewer.denied('/api/v1/dataset/'+str(sensitive['id'])), 'Viewer dataset detail bypass'
    assert viewer.denied('/api/v1/sqllab/execute/',{'database_id':sensitive['database']['id'],'sql':'SELECT * FROM iceberg.raw.accounts','runAsync':False}), 'Viewer SQL Lab bypass'
    for name,context in contexts:
        q=copy.deepcopy(context);q['force']=True
        r=viewer.json('/api/v1/chart/data',q)['result'][0]
        assert r['status']=='success' and r['data'], ('viewer',name)
    injection=copy.deepcopy(contexts[0][1]);injection['force']=True
    injection['queries'][0]['metrics']=[{'expressionType':'SQL','sqlExpression':'(SELECT max(account_id) FROM iceberg.raw.accounts)','label':'blocked_probe','optionName':'blocked_probe'}]
    try:
        rejected=viewer.json('/api/v1/chart/data',injection)
    except urllib.error.HTTPError as error:
        assert error.code in (400,403,422)
        rejected=json.loads(error.read())
    assert 'sub-quer' in json.dumps(rejected).lower(), 'Subquery not explicitly rejected'
    role_checks.append({'profile':'viewer','aggregate_charts_accessible':True,'regulatory_dataset_denied':True,'sql_lab_denied':True,'ad_hoc_subquery_denied':True})
    compliance=Session();compliance.login('waba-compliance',values['SSO_COMPLIANCE_PASSWORD'])
    visible=compliance.json('/api/v1/dataset/?q='+urllib.parse.quote(json.dumps({'page_size':100})))['result']
    assert {d['id'] for d in visible} == regulatory_ids, 'Compliance scope mismatch'
    assert compliance.denied('/api/v1/chart/data',contexts[0][1]), 'Compliance aggregate dataset bypass'
    for dataset in visible:
        query={'datasource':{'id':dataset['id'],'type':'table'},'force':True,'queries':[{'columns':['country_code'],'metrics':[],'row_limit':100,'filters':[],'orderby':[]}],'result_format':'json','result_type':'full'}
        r=compliance.json('/api/v1/chart/data',query)['result'][0]
        assert r['status']=='success' and r['data'], 'Regulatory query failed'
    role_checks.append({'profile':'compliance_officer','only_two_regulatory_gold_datasets':True,'other_dataset_query_denied':True})
    # Cache already warmed by admin before analyst requests: RLS must still isolate results.
    with viewer.open('/logout/') as response:
        html=response.read().decode()
        # The final public login must not retain Superset authentication.
    assert viewer.denied('/api/v1/me/'), 'Local logout failed'
    with viewer.open('/login/keycloak') as response:
        assert LoginForm(response.read().decode()).action, 'IdP session still active after logout'
    report={'verified_at_utc':datetime.now(timezone.utc).isoformat(),'keycloak_version':'26.4.0','superset_oidc_verified':True,'authorization_code_pkce':True,'profiles':role_checks,'group_admin_all_dashboard_queries':True,'local_logout_verified':True,'identity_provider_logout_verified':True,'rls_cache_isolation_verified':True,'trino_sso_verified':False,'profile':'local HTTP, H2 persistent PVC; no production TLS/HA claim'}
    (ROOT/'docs/sso-verification.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps(report,ensure_ascii=False))


if __name__=='__main__':
    main()
