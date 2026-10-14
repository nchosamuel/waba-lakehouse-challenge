"""Build a private realm import; credentials are supplied through environment."""
COUNTRIES = ('CI','SN','ML','BF','GN','TG','BJ','GH')


def realm(values):
    roles = ['group_admin','country_analyst','compliance_officer','viewer'] + ['country_'+c for c in COUNTRIES]
    users = [('waba-group-admin','SSO_GROUP_ADMIN_PASSWORD',['group_admin']),
             ('waba-viewer','SSO_VIEWER_PASSWORD',['viewer']),
             ('waba-compliance','SSO_COMPLIANCE_PASSWORD',['compliance_officer'])]
    users += [('waba-analyst-'+c.lower(),'SSO_ANALYST_'+c+'_PASSWORD',['country_analyst','country_'+c]) for c in COUNTRIES]
    return {
        'realm':'waba','enabled':True,'sslRequired':'none',
        'registrationAllowed':False,'resetPasswordAllowed':False,
        'bruteForceProtected':True,'loginWithEmailAllowed':False,
        'roles':{'realm':[{'name':r} for r in roles]},
        'clients':[{
            'clientId':'waba-superset','name':'WABA Superset','enabled':True,
            'protocol':'openid-connect','publicClient':False,
            'secret':values['KEYCLOAK_SUPERSET_SECRET'],
            'standardFlowEnabled':True,'directAccessGrantsEnabled':False,
            'redirectUris':['http://localhost:8088/oauth-authorized/keycloak'],
            'webOrigins':['http://localhost:8088'],
            'attributes':{'pkce.code.challenge.method':'S256', 'post.logout.redirect.uris':'http://localhost:8088/login/'},
            'protocolMappers':[{'name':'waba-realm-roles','protocol':'openid-connect',
                'protocolMapper':'oidc-usermodel-realm-role-mapper',
                'config':{'claim.name':'waba_roles','multivalued':'true','jsonType.label':'String',
                          'id.token.claim':'true','access.token.claim':'true','userinfo.token.claim':'true'}}],
        }],
        'users':[{'username':u,'enabled':True,'emailVerified':True,
                  'firstName':'WABA','lastName':u.removeprefix('waba-'),
                  'email':u+'@example.invalid','realmRoles':rs,
                  'credentials':[{'type':'password','value':values[key],'temporary':False}]} for u,key,rs in users],
    }
