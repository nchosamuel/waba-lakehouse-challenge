"""Default-deny SQL policy using groups from validated Keycloak token claims."""
COUNTRIES = ('CI','SN','ML','BF','GN','TG','BJ','GH')
VIEWS = {'revenue':'Revenus consolidés','arpc':'ARPC mensuel','subscriptions':'Souscriptions simulées',
         'npl':'NPL','loss_ratio':'Sinistres et primes','aml_counts':'AML · 30 jours',
         'claims_sla':'Sinistres et SLA simulé','mobile_hourly':'Mobile par heure',
         'corridors':'Corridors transfrontaliers','mobile_failures':'Échecs mobile'}


def policy():
    admin = 'group_admin|service_validation'
    tables=[{'group':admin,'privileges':['SELECT','INSERT','DELETE','UPDATE','OWNERSHIP','GRANT_SELECT']},
            {'user':'service-account-waba-validation','catalog':'iceberg','schema':'silver|gold|bi','privileges':['SELECT','GRANT_SELECT']},
            {'user':'service-account-waba-validation','catalog':'kafka','schema':'default','table':'silver_bank_transactions','privileges':['SELECT','GRANT_SELECT']},
            {'group':'service_bi','catalog':'iceberg','schema':'silver|gold|bi','privileges':['SELECT']},
            {'group':'service_dashboard','catalog':'iceberg','schema':'gold','privileges':['SELECT']},
            {'group':'compliance_officer','catalog':'iceberg','schema':'gold','table':'regulatory_bceao_daily|regulatory_cima_daily','privileges':['SELECT']},
            {'group':'viewer','catalog':'iceberg','schema':'bi','table':'|'.join(VIEWS),'privileges':['SELECT']}]
    tables += [{'group':'country_'+c,'catalog':'iceberg','schema':'raw|bronze|silver|gold|bi','privileges':['SELECT'],'filter':"country_code = '"+c+"'"} for c in COUNTRIES]
    tables += [{'privileges':[]}]
    return {'catalogs':[{'group':admin,'allow':'all'}, {'user':'service-account-waba-validation','catalog':'iceberg|kafka','allow':'all'},
                        {'group':'service_bi|service_dashboard|compliance_officer|viewer|country_('+ '|'.join(COUNTRIES)+')','catalog':'iceberg','allow':'read-only'},
                        {'allow':'none'}],
            'schemas':[{'group':admin,'owner':True},{'owner':False}],
            'tables':tables, 'impersonation':[],
            'queries':[{'group':admin,'allow':['execute','view','kill']},{'allow':['execute']}],
            'system_information':[{'user':'waba-group-admin|service-account-waba-validation','allow':['read']},{'allow':[]}],
            'system_session_properties':[{'group':admin,'allow':True},{'allow':False}]}


if __name__=='__main__':
    import json
    from pathlib import Path
    Path(__file__).with_name('rules.json').write_text(json.dumps(policy(),indent=2)+'\n')
