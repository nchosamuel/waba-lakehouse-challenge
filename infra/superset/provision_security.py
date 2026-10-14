"""Least privilege datasets and server-side country row security, idempotently."""
from superset.app import create_app
from superset import db
from datasets import DATASETS

app = create_app()
with app.app_context():
    from superset.connectors.sqla.models import SqlaTable, RowLevelSecurityFilter
    from superset.models.core import Database
    from superset.utils.core import RowLevelSecurityFilterType
    sm = app.appbuilder.sm
    database = db.session.query(Database).filter_by(database_name='WABA Trino').one()
    aggregates = db.session.query(SqlaTable).filter_by(database_id=database.id).filter(SqlaTable.table_name.in_(list(DATASETS))).all()
    regulatory = []
    for name in ('regulatory_bceao_daily','regulatory_cima_daily'):
        table = db.session.query(SqlaTable).filter_by(database_id=database.id,table_name=name).one_or_none()
        if not table:
            table = SqlaTable(table_name=name,database=database,schema='gold')
            db.session.add(table)
            db.session.flush()
        table.fetch_metadata()
        regulatory.append(table)
    db.session.commit()
    for name,tables in [('WABA Viewer',aggregates),('WABA Compliance',regulatory)] + [('WABA Analyst '+c,aggregates+regulatory) for c in ('CI','SN','ML','BF','GN','TG','BJ','GH')]:
        role = sm.find_role(name) or sm.add_role(name)
        role.permissions = [sm.add_permission_view_menu('datasource_access', t.perm) for t in tables]
        if name.startswith('WABA Analyst '):
            country = name.rsplit(' ',1)[1]
            rule = db.session.query(RowLevelSecurityFilter).filter_by(name='WABA country '+country).one_or_none()
            if not rule:
                rule = RowLevelSecurityFilter(name='WABA country '+country)
                db.session.add(rule)
            rule.filter_type = RowLevelSecurityFilterType.REGULAR
            rule.clause = "country_code = '"+country+"'"
            rule.roles = [role]
            rule.tables = aggregates+regulatory
            rule.group_key = 'waba-country'
    db.session.commit()
    print('WABA dataset roles and eight country restrictions provisioned.')
