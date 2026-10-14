"""Business datasets use actual Trino records and declared simulation context."""

REVENUE = """SELECT country_code,entity_type,sum(revenue_eur) revenue_eur FROM (
 SELECT country_code,entity_type,revenue_eur FROM iceberg.gold.customer_arpu_monthly
 UNION ALL SELECT country_code,entity_type,earned_premiums_eur FROM iceberg.gold.loss_ratio_by_product
 UNION ALL SELECT country_code,entity_type,CAST(fee_amount_eur AS DOUBLE) FROM iceberg.silver.mobile_money_payments WHERE status='SUCCESS'
) GROUP BY 1,2"""
DATASETS = {
    "Revenus consolidés": REVENUE,
    "ARPC mensuel": "SELECT country_code,entity_type,month,revenue_eur,active_customers,arpc_eur FROM iceberg.gold.customer_arpu_monthly",
    "Souscriptions simulées": """SELECT * FROM (SELECT *,row_number() OVER (PARTITION BY country_code ORDER BY subscription_count DESC,product_id) product_rank FROM iceberg.gold.product_subscriptions) WHERE product_rank<=10""",
    "NPL": """SELECT country_code,entity_type,snapshot_date,total_outstanding_eur,default_outstanding_eur,100.0*default_outstanding_eur/NULLIF(total_outstanding_eur,0) npl_percent,
 CASE WHEN default_outstanding_eur/NULLIF(total_outstanding_eur,0)<0.03 THEN 'Vert · < 3 %' WHEN default_outstanding_eur/NULLIF(total_outstanding_eur,0)<=0.05 THEN 'Orange · 3–5 %' ELSE 'Rouge · > 5 %' END risk_status
 FROM (SELECT country_code,entity_type,snapshot_date,sum(total_outstanding_eur) total_outstanding_eur,sum(default_outstanding_eur) default_outstanding_eur FROM iceberg.gold.npl_ratio_by_country GROUP BY 1,2,3)""",
    "Sinistres et primes": """SELECT country_code,entity_type,month,product_id,product_line,earned_premiums_eur,paid_claims_eur,loss_ratio*100 loss_percent,70 challenge_threshold_percent,CASE WHEN loss_ratio>0.70 THEN 'Au-dessus du seuil du sujet' ELSE 'Sans dépassement du seuil' END risk_status FROM iceberg.gold.loss_ratio_by_product""",
    "AML · 30 jours": """SELECT country_code,entity_type,CAST(detected_at AS DATE) alert_day,count(*) alert_count FROM iceberg.gold.aml_events WHERE detected_at >= CAST(current_date-INTERVAL '30' DAY AS TIMESTAMP) GROUP BY 1,2,3""",
    "Sinistres et SLA simulé": "SELECT country_code,entity_type,month,insurance_line,paid_claim_count,avg_business_processing_days,sla_business_days,within_demo_sla,sla_model FROM iceberg.gold.claims_sla",
    "Mobile par heure": """SELECT country_code,entity_type,lpad(CAST(hour(timestamp) AS VARCHAR),2,'0') payment_hour,count(*) payment_count,sum(CASE WHEN status='SUCCESS' THEN CAST(amount_eur AS DOUBLE) ELSE 0 END) amount_eur FROM iceberg.silver.mobile_money_payments GROUP BY 1,2,3""",
    "Corridors transfrontaliers": """SELECT * FROM (SELECT country_code,entity_type,sender_country||' → '||receiver_country corridor,count(*) payment_count,sum(CAST(amount_eur AS DOUBLE)) amount_eur,row_number() OVER (PARTITION BY country_code ORDER BY sum(CAST(amount_eur AS DOUBLE)) DESC) corridor_rank FROM iceberg.silver.mobile_money_payments WHERE sender_country<>receiver_country AND status='SUCCESS' GROUP BY 1,2,3) WHERE corridor_rank<=5""",
    "Échecs mobile": """SELECT country_code,entity_type,operator,count(*) attempted_count,sum(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) failed_count,100.0*sum(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/count(*) failure_percent FROM iceberg.silver.mobile_money_payments GROUP BY 1,2,3""",
}
