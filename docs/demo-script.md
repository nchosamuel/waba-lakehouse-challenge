# Démonstration finale — 5 à 10 minutes

Statut : capture réalisée, montage de **9 min 14 s**, sous-titres français intégrés ; fichier vérifié visuellement. Voir `demo/WABA_Demo_FR.mp4` et `demo/WABA_Demo_FR.srt`. Les preuves des composants sont disponibles dans le dépôt. Le tournage doit montrer les résultats réellement visibles ; les signaux d’alertes techniques de démonstration sont identifiés comme tels.

| Temps | À montrer | Explication |
|---|---|---|
| 0:00–0:40 | README et architecture | Huit pays, quatre métiers, données fictives ; modes locaux pour tenir dans la mémoire du Mac. |
| 0:40–1:35 | Streamlit | Générer CI et GH ; montrer Banque/Microfinance, assurance/mobile et noms de fichiers. Démarrer deux lots continus puis arrêter. |
| 1:35–2:35 | Airflow et Spark Operator | Montrer un DAG réussi et driver/executor distincts, puis Raw → Bronze → Silver → Gold. Présenter un traitement préparé pour éviter une longue attente. |
| 2:35–3:30 | Trino | Huit tables raw, NPL et loss ratio ; expliquer les encours et primes acquises **simulés**, ainsi que les FX déclarés. Montrer un rejeu sans doublons. |
| 3:30–4:35 | NiFi/Kafka et fraude | Montrer une cohorte de fraude, les règles déclenchées et la vue Lambda ; les événements bruts restent traçables. Préparer cette démonstration dans le mode streaming. |
| 4:35–5:35 | Superset et Keycloak | Trois dashboards et filtre CI ; connexion analyste pays, lecteur sans lignes sensibles. Les souscriptions et SLA sont modélisés et déclarés. |
| 5:35–6:35 | OpenMetadata | Table Gold documentée, propriétaire et glossaire ; chemin Raw → Bronze → Silver → Gold ; PII customer_id. account_number/iban ne sont pas inventés dans les sources. |
| 6:35–7:45 | Grafana / Loki | Mesures techniques, JSON d’un vrai job CI, trois règles. Montrer les instances scenario=demo et leur notification locale ; l’alerte Spark conserve cinq minutes de délai. |
| 7:45–8:30 | Preuves et réserves | Tests unitaires/CI, rapports de contrôle, lien GitHub et archive. Le profil local ne valide ni déploiement intégral simultané, ni haute disponibilité/sécurité de production. |

## Préparer la capture

Les profils batch, streaming, BI, catalogue et supervision sont successifs, pas tous actifs en même temps. Préparer les écrans et résultats pour chaque séquence ; assembler les séquences en une vidéo continue de moins de dix minutes et annoncer le changement de mode. Ne pas présenter des captures anciennes comme une exécution en direct.

La cohorte de **fraude métier** du streaming et l’injection des **alertes techniques** de supervision sont distinctes : ne pas confondre un comportement client suspect avec une panne du traitement. Pour Grafana, lancer `verify_observability.py` en amont : il faut attendre réellement cinq minutes. Si la capture montre un retour à Normal, préciser qu’il concerne le scénario de test ; une alerte réelle peut rester active.

Ne pas filmer `.env`, les mots de passe, les entêtes HTTP ni les clés de connexion. Effectuer les logins avant capture. Les notifications Grafana restent dans le cluster ; aucun message externe n’est envoyé.

## Requêtes lisibles

```sql
SELECT country_code, entity_type, COUNT(*) AS events
FROM iceberg.raw.bank_transactions GROUP BY 1, 2 ORDER BY 1, 2;

SELECT country_code, SUM(default_outstanding_eur) / SUM(total_outstanding_eur) AS npl
FROM iceberg.gold.npl_ratio_by_country GROUP BY 1 ORDER BY 1;

SELECT country_code, product_line, loss_ratio
FROM iceberg.gold.loss_ratio_by_product WHERE month = DATE '2026-02-01'
ORDER BY country_code, product_line;
```

Après capture, vérifier durée, lisibilité et absence de secrets. Ajouter le lien de la vidéo au dépôt ; ce script et les captures ne remplacent pas la vidéo demandée.

## Capture livrée

La vidéo livrée dure 9 min 14 s, sans voix, avec sous-titres français intégrés. Elle montre la génération CI/GH ponctuelle et continue, des exécutions Airflow réussies, les indicateurs, le flux NiFi, les résultats de véritables requêtes Trino et des alertes de la cohorte, les trois dashboards Superset avec filtre CI, puis la supervision. L’écran de requêtes est explicitement un rapport en lecture seule construit depuis les résultats réellement obtenus ; il ne simule pas un éditeur SQL.

Les séquences sont montées par profils locaux ; les attentes sont écourtées. La capture vérifiée du résultat du filtre CI est maintenue 17 secondes pour faciliter la lecture. Les écrans d’authentification et les secrets ne sont pas filmés. Les preuves détaillées de SSO, OpenMetadata et déclenchement des alertes techniques restent dans les rapports et captures associés ; elles ne sont pas toutes rejouées dans ce montage.
