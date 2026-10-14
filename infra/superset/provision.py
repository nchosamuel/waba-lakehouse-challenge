"""Idempotent creation of three country-filtered dashboards from Trino datasets."""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from datasets import DATASETS
from superset.app import create_app
from superset import db


def metric(column, label=None, expression=None):
    return {
        "expressionType": "SQL",
        "sqlExpression": expression or "SUM(" + column + ")",
        "label": label or column,
        "hasCustomLabel": True,
        "optionName": "metric_" + column,
    }


# Tables keep threshold/SLA columns explicit; no raw identifiers are exposed.
CHARTS = [
    (
        1,
        "Revenus par pays et activité",
        "Revenus consolidés",
        "echarts_timeseries_bar",
        ["country_code", "entity_type"],
        [metric("revenue_eur", "Revenus (€)")],
        {"x_axis": "country_code", "groupby": ["entity_type"], "stack": False},
    ),
    (
        1,
        "ARPC mensuel par pays",
        "ARPC mensuel",
        "echarts_timeseries_line",
        ["month", "country_code"],
        [
            metric(
                "arpc_eur",
                "ARPC (€)",
                "SUM(revenue_eur)/NULLIF(SUM(active_customers),0)",
            )
        ],
        {
            "x_axis": "month",
            "groupby": ["country_code"],
            "markerEnabled": True,
            "markerSize": 7,
        },
    ),
    (
        1,
        "Contribution des pays aux revenus",
        "Revenus consolidés",
        "world_map",
        ["country_code"],
        [metric("revenue_eur", "Revenus (€)")],
        {
            "entity": "country_code",
            "country_fieldtype": "cca2",
            "show_bubbles": False,
            "color_by": "metric",
            "linear_color_scheme": "blue_white_yellow",
        },
    ),
    (
        1,
        "Top produits · souscriptions simulées",
        "Souscriptions simulées",
        "table",
        ["country_code", "entity_type", "product_name"],
        [metric("subscription_count", "Souscriptions")],
        {},
    ),
    (
        2,
        "NPL · vert <3 %, orange 3–5 %, rouge >5 %",
        "NPL",
        "echarts_timeseries_bar",
        ["country_code", "risk_status"],
        [
            metric(
                "npl_percent",
                "NPL (%)",
                "100*SUM(default_outstanding_eur)/NULLIF(SUM(total_outstanding_eur),0)",
            )
        ],
        {
            "x_axis": "country_code",
            "groupby": ["risk_status"],
            "label_colors": {
                "Vert · < 3 %": "#2ca02c",
                "Orange · 3–5 %": "#ff9900",
                "Rouge · > 5 %": "#d62728",
            },
        },
    ),
    (
        2,
        "Sinistres / primes · seuil du sujet 70 %",
        "Sinistres et primes",
        "table",
        ["country_code", "product_line", "risk_status", "challenge_threshold_percent"],
        [
            metric(
                "loss_percent",
                "Sinistres / primes (%)",
                "100*SUM(paid_claims_eur)/NULLIF(SUM(earned_premiums_eur),0)",
            )
        ],
        {},
    ),
    (
        2,
        "Alertes AML · 30 derniers jours",
        "AML · 30 jours",
        "echarts_timeseries_bar",
        ["alert_day", "country_code"],
        [metric("alert_count", "Alertes")],
        {"x_axis": "alert_day", "groupby": ["country_code"]},
    ),
    (
        2,
        "Délais des sinistres et SLA simulé",
        "Sinistres et SLA simulé",
        "table",
        ["country_code", "insurance_line", "sla_business_days"],
        [
            metric(
                "avg_business_processing_days",
                "Délai (jours ouvrés)",
                "SUM(avg_business_processing_days*paid_claim_count)/NULLIF(SUM(paid_claim_count),0)",
            )
        ],
        {},
    ),
    (
        3,
        "Paiements mobiles par heure et pays",
        "Mobile par heure",
        "heatmap_v2",
        ["country_code", "payment_hour"],
        [metric("payment_count", "Paiements")],
        {
            "x_axis": "payment_hour",
            "groupby": ["country_code"],
            "sort_x_axis": "alpha_asc",
            "sort_y_axis": "alpha_asc",
            "normalize_across": "heatmap",
            "linear_color_scheme": "blue_white_yellow",
            "show_values": False,
        },
    ),
    (
        3,
        "Top 5 corridors par pays",
        "Corridors transfrontaliers",
        "table",
        ["country_code", "corridor"],
        [metric("amount_eur", "Montant (€)"), metric("payment_count", "Paiements")],
        {},
    ),
    (
        3,
        "Échecs mobile par opérateur et pays",
        "Échecs mobile",
        "table",
        ["country_code", "operator"],
        [
            metric(
                "failure_percent",
                "Échecs (%)",
                "100.0*SUM(failed_count)/NULLIF(SUM(attempted_count),0)",
            )
        ],
        {},
    ),
]
DASHBOARDS = {
    1: ("Performance Commerciale Groupe", "waba-performance"),
    2: ("Risque et Conformité Réglementaire", "waba-risk"),
    3: ("Mobile Money et Transferts", "waba-mobile"),
}
app = create_app()
with app.app_context():
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard

    database = (
        db.session.query(Database).filter_by(database_name="WABA Trino").one_or_none()
    )
    if not database:
        database = Database(
            database_name="WABA Trino",
            sqlalchemy_uri="trino://waba-bi@trino:8080/iceberg/gold",
            expose_in_sqllab=True,
        )
        db.session.add(database)
        db.session.flush()
    if os.environ.get("WABA_TRINO_SSO_ENABLED") == "true":
        database.sqlalchemy_uri = "trino://service-account-waba-bi@trino:8443/iceberg/gold"
    tables = {}
    for name, sql in DATASETS.items():
        table = (
            db.session.query(SqlaTable)
            .filter_by(database_id=database.id, table_name=name)
            .one_or_none()
        )
        if not table:
            table = SqlaTable(
                table_name=name, database=database, schema="gold", sql=sql
            )
            db.session.add(table)
            db.session.flush()
        table.sql = sql
        table.description = "Données WABA simulées issues de Trino. Souscriptions et SLA sont des hypothèses explicitement marquées."
        table.fetch_metadata()
        labels = {
            "country_code": "Pays",
            "entity_type": "Activité",
            "product_name": "Produit",
            "product_line": "Produit assurance",
            "insurance_line": "Activité assurance",
            "sla_business_days": "SLA (jours ouvrés)",
            "challenge_threshold_percent": "Seuil du sujet (%)",
            "risk_status": "Situation",
            "operator": "Opérateur",
            "corridor": "Corridor",
        }
        for column in table.columns:
            column.verbose_name = labels.get(column.column_name, column.verbose_name)
        tables[name] = table
    db.session.commit()
    slices = {}
    for group, title, dataset, viz, columns, metrics, options in CHARTS:
        table = tables[dataset]
        params = {
            "viz_type": viz,
            "datasource": str(table.id) + "__table",
            "adhoc_filters": [],
            "time_range": "No filter",
            "row_limit": 10000,
            "metrics": metrics,
            "metric": metrics[0],
            "groupby": columns,
            "all_columns": [],
            "query_mode": "aggregate",
            "show_legend": True,
            "y_axis_format": ",.2f",
            **options,
        }
        if viz == "table":
            params.update(
                order_desc=True,
                server_pagination=False,
                include_search=True,
                page_length=10,
            )
        query = {
            "columns": columns,
            "metrics": metrics,
            "filters": [],
            "extras": {"having": "", "where": ""},
            "row_limit": 10000,
            "orderby": [[metrics[0], False]],
            "time_range": "No filter",
            "is_timeseries": False,
        }
        query_context = {
            "datasource": {"id": table.id, "type": "table"},
            "queries": [query],
            "form_data": params,
            "result_format": "json",
            "result_type": "full",
        }
        chart = db.session.query(Slice).filter_by(slice_name=title).one_or_none()
        if not chart:
            chart = Slice(
                slice_name=title,
                datasource_id=table.id,
                datasource_type="table",
                viz_type=viz,
            )
            db.session.add(chart)
        chart.params = json.dumps(params)
        chart.query_context = json.dumps(query_context)
        chart.viz_type = viz
        db.session.flush()
        slices.setdefault(group, []).append(chart)
    for group, (title, slug) in DASHBOARDS.items():
        dashboard = db.session.query(Dashboard).filter_by(slug=slug).one_or_none()
        if not dashboard:
            dashboard = Dashboard(dashboard_title=title, slug=slug)
            db.session.add(dashboard)
        dashboard.published = True
        dashboard.slices = slices[group]
        layout = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
            "GRID_ID": {
                "id": "GRID_ID",
                "type": "GRID",
                "children": [],
                "parents": ["ROOT_ID"],
            },
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
        }
        for i in range(0, len(slices[group]), 2):
            row = "ROW-" + str(i)
            layout["GRID_ID"]["children"].append(row)
            layout[row] = {
                "id": row,
                "type": "ROW",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            for chart in slices[group][i : i + 2]:
                identifier = "CHART-" + str(chart.id)
                layout[row]["children"].append(identifier)
                layout[identifier] = {
                    "id": identifier,
                    "type": "CHART",
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row],
                    "meta": {
                        "chartId": chart.id,
                        "sliceName": chart.slice_name,
                        "width": 6,
                        "height": 55,
                    },
                }
        dashboard.position_json = json.dumps(layout)
        targets = [
            {"datasetId": tables[name].id, "column": {"name": "country_code"}}
            for name in dict.fromkeys(c[2] for c in CHARTS if c[0] == group)
        ]
        dashboard.json_metadata = json.dumps(
            {
                "native_filter_configuration": [
                    {
                        "id": "NATIVE_FILTER-country",
                        "name": "Pays",
                        "filterType": "filter_select",
                        "targets": targets,
                        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                        "controlValues": {
                            "enableEmptyFilter": False,
                            "multiSelect": True,
                            "searchAllOptions": True,
                        },
                        "defaultDataMask": {"extraFormData": {}, "filterState": {}},
                        "cascadeParentIds": [],
                        "type": "NATIVE_FILTER",
                    }
                ],
                "color_scheme": "supersetColors",
                "label_colors": {
                    "Vert · < 3 %": "#2ca02c",
                    "Orange · 3–5 %": "#ff9900",
                    "Rouge · > 5 %": "#d62728",
                },
            }
        )
    db.session.commit()
    print(
        json.dumps(
            {
                "dashboards": list(DASHBOARDS.values()),
                "charts": len(CHARTS),
                "datasets": len(tables),
            }
        )
    )
