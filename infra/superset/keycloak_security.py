"""Accept only explicit WABA realm roles from validated OIDC user info."""
from flask import redirect, session
from urllib.parse import urlencode
from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthOAuthView
from superset.security import SupersetSecurityManager

COUNTRIES = ('CI','SN','ML','BF','GN','TG','BJ','GH')


class WabaOAuthView(AuthOAuthView):
    @expose('/logout/')
    def logout(self):
        token = session.get('waba_id_token')
        super().logout()
        session.clear()
        parameters = {'client_id':'waba-superset','post_logout_redirect_uri':'http://localhost:8088/login/'}
        if token:
            parameters['id_token_hint'] = token
        return redirect('http://localhost:18080/realms/waba/protocol/openid-connect/logout?'+urlencode(parameters))



class WabaSecurityManager(SupersetSecurityManager):
    authoauthview = WabaOAuthView

    def oauth_user_info(self, provider, response=None):
        if provider != 'keycloak':
            return {}
        if response and response.get('id_token'):
            session['waba_id_token'] = response['id_token']
        info = self.oauth_remotes[provider].get('userinfo').json()
        roles = set(info.get('waba_roles', []))
        functional = roles & {'group_admin','country_analyst','compliance_officer','viewer'}
        countries = roles & {'country_'+c for c in COUNTRIES}
        # Refuse ambiguous assignments and analysts without exactly one country.
        if len(functional) != 1 or ('country_analyst' in functional and len(countries) != 1):
            return {}
        role_keys = list(functional)
        if 'country_analyst' in functional:
            role_keys = list(countries)
        return {'username':info['preferred_username'], 'email':info.get('email',''),
                'first_name':info.get('given_name','WABA'),'last_name':info.get('family_name',''),
                'role_keys':role_keys}
