"""Read-only Trino queries for the demonstration interface."""

import json
import urllib.request
from waba.generator import COUNTRIES

TABLES = {
    "Activité bancaire": "daily_transaction_volume",
    "Risque crédit · NPL": "npl_ratio_by_country",
    "Revenu par client": "customer_arpu_monthly",
    "Sinistres / primes": "loss_ratio_by_product",
    "Délais des sinistres": "claims_processing_time",
    "Paiements mobiles": "mobile_money_daily_flow",
    "Transferts entre pays": "cross_border_transfers",
    "Couverture comptable assurance": "insurance_accounting_coverage",
    "Fraude · alertes": "fraud_alerts",
    "AML · alertes": "aml_events",
    "Liquidité · alertes": "liquidity_alerts",
    "Activité bancaire · batch + temps réel": "lambda_daily_transaction_volume",
}


def query(table, countries):
    if (
        table not in TABLES.values()
        or not countries
        or any(c not in COUNTRIES for c in countries)
    ):
        raise ValueError("Unsupported analytics query")
    literals = ",".join("'" + country + "'" for country in countries)
    sql = f"SELECT * FROM iceberg.gold.{table} WHERE country_code IN ({literals}) ORDER BY country_code LIMIT 500"
    from waba.trino_client import query as sql_query
    columns,rows = sql_query(sql)
    return [dict(zip([c['name'] for c in columns], row)) for row in rows]



LABELS = {
    "country_code": "Pays",
    "alert_id": "Identifiant de l’alerte",
    "rule": "Règle",
    "subject_id": "Compte ou client",
    "window_start": "Début de fenêtre",
    "window_end": "Fin de fenêtre",
    "event_count": "Événements",
    "detected_at": "Détection",
    "entity_type": "Ligne métier",
    "txn_date": "Date",
    "month": "Mois",
    "snapshot_date": "Date des encours",
    "transaction_type": "Type de transaction",
    "attempted_count": "Tentatives",
    "transaction_count": "Transactions réussies",
    "successful_count": "Paiements réussis",
    "failed_count": "Échecs",
    "total_amount_eur": "Montant total (€)",
    "fee_revenue_eur": "Commissions (€)",
    "total_outstanding_eur": "Encours total (€)",
    "default_outstanding_eur": "Encours en défaut (€)",
    "loan_count": "Nombre de prêts",
    "loan_type": "Type de prêt",
    "npl_ratio": "NPL (%)",
    "above_challenge_threshold": "Alerte selon le sujet",
    "customer_segment": "Segment client",
    "revenue_eur": "Revenus observés (€)",
    "active_customers": "Clients actifs",
    "unknown_interest_events": "Opérations sans intérêts renseignés",
    "arpc_eur": "Revenu par client (€)",
    "revenue_scope": "Périmètre du revenu",
    "product_id": "Produit",
    "product_line": "Ligne assurance",
    "earned_premiums_eur": "Primes acquises (€)",
    "paid_claims_eur": "Sinistres payés (€)",
    "modelled_events": "Opérations modélisées",
    "loss_ratio": "Sinistres / primes (%)",
    "insurance_line": "Activité assurance",
    "paid_claim_count": "Sinistres payés",
    "avg_business_processing_days": "Délai moyen (jours ouvrés)",
    "active_users": "Utilisateurs actifs",
    "failure_rate": "Taux d’échec (%)",
    "sender_country": "Pays expéditeur",
    "receiver_country": "Pays destinataire",
    "week": "Semaine",
    "average_amount_eur": "Montant moyen (€)",
    "previous_week": "Semaine précédente",
    "previous_week_amount_eur": "Montant semaine précédente (€)",
    "weekly_growth": "Évolution hebdomadaire (%)",
    "total_events": "Opérations totales",
    "unmodelled_events": "Opérations exclues faute de contexte comptable",
}


def display_rows(rows):
    result = []
    for row in rows:
        translated = {}
        for key, value in row.items():
            if (
                key in ("npl_ratio", "loss_ratio", "failure_rate", "weekly_growth")
                and value is not None
            ):
                value = round(float(value) * 100, 3)
            elif key.endswith("_eur") and value is not None:
                value = round(float(value), 2)
            elif key == "above_challenge_threshold":
                value = "Oui" if value else "Non"
            elif key == "rule":
                value = {
                    "MULTIPLE_HIGH_VALUE_TRANSFERS": "Transactions répétées > 500 000 XOF",
                    "UNUSUAL_COUNTRY_FOR_PROFILE": "Pays inhabituel pour le profil",
                    "CLAIM_OVER_THREE_ANNUAL_PREMIUMS": "Sinistre > 3 primes annuelles",
                    "CHALLENGE_AML_THRESHOLD": "Seuil AML du sujet",
                    "DEMO_LIQUIDITY_COVERAGE_BELOW_10_PERCENT": "Couverture de liquidité simulée < 10 %",
                }.get(value, value)
            elif key == "revenue_scope":
                value = "Commissions observées + intérêts documentés"
            translated[LABELS.get(key, key)] = value
        result.append(translated)
    return result
