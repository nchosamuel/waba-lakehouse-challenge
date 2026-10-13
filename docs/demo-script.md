# Démonstration de 5 à 10 minutes — niveaux 1 et 2

Statut : scénario de capture préparé ; vidéo à enregistrer. Aucun niveau 3 ou 4 n'est présenté comme implémenté.

| Temps indicatif | À montrer | À expliquer |
|---|---|---|
| 0:00–0:45 | README et architecture | Données fictives sur huit pays, MinIO/Iceberg/Trino, chaîne batch médaillon. |
| 0:45–2:00 | Streamlit Génération | Choisir CI et GH, Banque, 100 lignes, une journée ; lancer. Montrer devises et chemins MinIO. Choix Microfinance disponible pour transactions. |
| 2:00–3:00 | Génération continue | Régler dix secondes, démarrer deux lots puis arrêter. Le navigateur doit rester ouvert. |
| 3:00–4:30 | Airflow | Déclencher dag_ingest_raw et montrer Bronze → Silver → Gold. Montrer les quatre DAGs et le reporting à 00h30 UTC. Préparer les résultats avant la capture si les jobs prennent plus de temps. |
| 4:30–6:00 | Trino et Indicateurs | Interroger les huit tables ; montrer NPL, ratio assurance, jours ouvrés et corridors avec filtre pays. Expliquer les encours et primes acquises en un exemple. |
| 6:00–7:00 | Preuves | Rapports de quarantaine, absence de doublons au rejeu, backfill CI sans changement des autres pays, résultats des tests. |
| 7:00–8:00 | Limites et suites | FX simulé, couverture comptable partielle, démonstrateur local. NiFi/Kafka et Kubernetes sont les prochaines étapes. |

Ne pas filmer `.env`, les paramètres secrets, les en-têtes d'authentification ni les clés de connexion. Le login Airflow peut être effectué avant la capture.

## Trois requêtes lisibles

```sql
SELECT country_code, entity_type, COUNT(*) AS events
FROM iceberg.raw.bank_transactions GROUP BY 1, 2 ORDER BY 1, 2;

SELECT country_code, SUM(default_outstanding_eur) / SUM(total_outstanding_eur) AS npl
FROM iceberg.gold.npl_ratio_by_country GROUP BY 1 ORDER BY 1;

SELECT country_code, product_line, loss_ratio
FROM iceberg.gold.loss_ratio_by_product WHERE month = DATE '2026-02-01'
ORDER BY country_code, product_line;
```

Après capture, vérifier la durée, la lisibilité des écrans et l'absence de secrets avant de partager la vidéo. Ajouter son lien au dépôt ; le script seul ne remplace pas la vidéo demandée.
