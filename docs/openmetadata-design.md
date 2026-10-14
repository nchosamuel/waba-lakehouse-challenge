# Catalogue et lineage OpenMetadata

OpenMetadata 1.9.0, PostgreSQL 15.12 et OpenSearch 2.19.1 tournent en ARM64 sur Kubernetes, dans `governance`. Les deux bases ont leurs PVC. Le mode catalogue met Superset, NiFi, Kafka et Airflow en pause, conserve les volumes et garde Trino, MinIO, le catalogue Iceberg et Keycloak actifs. L'observation du mode catalogue est d'environ 5,2 GiB sur les 7,65 GiB accordés à Docker ; ce chiffre ne prouve pas une charge de production.

## Exécution

Prérequis : socle et données Kubernetes, puis SSO Superset et Trino déjà activés. Conserver le port-forward Keycloak localhost:18080.

```sh
python3 scripts/metadata_mode.py
python3 scripts/metadata_ingestion.py
python3 scripts/capture_lineage.py
python3 scripts/metadata_mode.py
python3 scripts/metadata_ingestion.py
.tools/kubectl --context kind-waba -n governance port-forward svc/openmetadata 8585:8585
python3 scripts/verify_metadata.py
```

La capture rejoue les traitements raw→Bronze, Bronze→Silver et Silver→Gold pour CI, avec leurs écritures idempotentes et leur périmètre pays. Les autres pays sont conservés. Le catalogue est mis en pause pendant ces jobs pour laisser la mémoire à Spark. Le retour au mode BI ou streaming suspend la découverte planifiée et met les services de métadonnées en pause. Avant de changer de mode, laisser terminer une découverte active. Les migrations et les premiers essais échoués sont conservés pour diagnostic, dans la limite d'historique configurée des Jobs.

Ouvrir http://localhost:8585. Compte `waba-catalog-admin@example.invalid`, mot de passe privé `OPENMETADATA_ADMIN_PASSWORD` dans `.env`. Le mot de passe initial est remplacé automatiquement ; la réinitialisation utilise le mode SELF, sans email. Inscription publique désactivée. Clés RSA et clé Fernet générées et privées. Cette UI utilise une authentification locale Basic ; le SSO demandé pour Superset et Trino est traité séparément.

## Découverte et curation

Le connecteur externe du projet s'exécute en Job/CronJob Kubernetes, sans installer un second Airflow. Il lit `iceberg.information_schema` via le client Keycloak dédié `waba-metadata`, en lecture seule et HTTPS avec vérification de la CA. Il publie les services, schémas, tables, colonnes et types dans l'API OpenMetadata. Aucun échantillon de lignes ni profilage des valeurs n'est collecté. Les secrets sont uniquement dans des Secrets Kubernetes ; le service déclaré dans OpenMetadata ne stocke pas les identifiants SQL. L'orchestration se fait par le CronJob externe, et non par le bouton d'ingestion Airflow de l'UI.

La découverte est planifiée à 02h15 UTC, avec une seule exécution à la fois. Elle renouvelle ses jetons à chaque exécution. Les mises à jour sont des upserts ; une nouvelle table est détectée automatiquement. Le test crée une table vide, la découvre et vérifie ses tags, puis supprime cette table source et marque son entrée de catalogue supprimée. Le connecteur ne revendique pas une synchronisation générale des suppressions de sources ; cette suppression de fixture est explicite.

Résultat vérifié : **59 tables/vues**, cinq schémas, **neuf tables Gold** avec description métier, équipe propriétaire et terme du glossaire financier. Huit termes définissent NPL, ARPC, sinistralité, primes acquises, délais, volumes, transferts et reporting de démonstration. Les tags BCEAO/CIMA décrivent l'usage des agrégats et leur périmètre déclaré ; ils ne certifient pas une conformité officielle.

OpenMetadata impose une équipe de type Group et une seule équipe propriétaire. Les données mixtes Banque/Microfinance appartiennent donc à une équipe commune dédiée ; l'assurance et le mobile ont leur équipe respective. Les descriptions et hypothèses sont déclarées dans `infra/openmetadata/curation.py`, modifiables et rejouables.

## PII et limites des fichiers

**38 colonnes** présentes sont marquées `PII.Sensitive`, notamment `customer_id`, `account_id`, `loan_account_id` et `subject_id`. Les montants d'assurance individuels reçoivent également un tag de sensibilité financière. La classification est fondée sur des règles de noms/ contexte, sans inspection des valeurs ; elle n'est pas une détection DLP exhaustive ni une preuve juridique.

Les tables d'origine **ne contiennent pas les colonnes `account_number` ou `iban`**. Nous ne les inventons pas. Leurs règles de tagging sont implémentées et vérifiées sur la table de test vide, puis cette fixture est retirée. Les tags rappellent les besoins de protection RGPD/lois locales à apprécier selon un périmètre réel. Les données du challenge restent simulées.

## Lineage automatique issu de Spark

Après chaque écriture réussie par `merge` ou `replace_countries`, le code inspecte les feuilles `DataSourceV2Relation` du plan Spark analysé. Il enregistre les tables Iceberg effectivement lues, la table écrite, le nom et l'identifiant de l'application et l'heure dans `archive/governance-lineage/`. Il ne journalise ni lignes ni valeurs de données. La capture est activée par `WABA_LINEAGE_ENABLED` dans le profil Kubernetes.

La découverte importe ces événements et lie les tables à trois pipelines Spark dans OpenMetadata. La preuve couvre **29 événements et 42 dépendances**, avec un chemin live API et UI : `raw.bank_transactions → bronze.bank_transactions → silver.bank_transactions → gold.daily_transaction_volume`. Les jointures de référentiels sont visibles. L'affichage utilise une profondeur amont de trois.

Les contextes de simulation lus depuis JSON et les constantes FX ne sont pas des tables Iceberg d'entrée : leurs événements peuvent avoir une liste vide, sans faux lien inventé. La capture est au niveau table, pas colonne, et couvre les écritures batch passant par les helpers instrumentés ; aucune capture universelle Kafka/NiFi ou SQL ad hoc n'est revendiquée. Elle intervient après le commit : une panne de journalisation fait échouer le job après l'écriture ; le rejeu idempotent rétablit la provenance. Il n'y a pas de transaction distribuée données/journal.

## Preuves et production

`openmetadata-verification.json` contrôle descriptions/propriétaires/termes/tags, les dépendances réelles, le nouveau schéma de test et le nettoyage. `openmetadata-ingestion-verification.json` contient la découverte finale des 59 tables, sans la fixture. Les captures montrent la chaîne et les tags sur `raw.accounts`. Les 23 tests unitaires et les six tests Streamlit repassent.

Cette étape est une démonstration locale persistante. API/UI catalogue et services de stockage internes en HTTP, sécurité OpenSearch désactivée dans ce namespace local, ingestion avec un compte catalogue privilégié, un seul nœud. Une cible de production requiert TLS, politiques réseau, authentification du moteur de recherche, compte bot à privilèges adaptés, rotation des secrets et sauvegardes testées. Les liens profonds de cette image peuvent répondre HTTP 404 puis être rendus correctement par la SPA ; les captures/API sont vérifiées, mais le routage d'entrée doit être durci pour une publication. Prometheus/Grafana/Loki et les alertes restent à réaliser.

Références : [release OpenMetadata 1.9.0](https://github.com/open-metadata/OpenMetadata/releases/tag/1.9.0-release), [déploiement PostgreSQL de référence](https://github.com/open-metadata/OpenMetadata/blob/1.9.0-release/docker/development/docker-compose-postgres.yml), [création de tables par API](https://github.com/open-metadata/OpenMetadata/blob/1.9.0-release/openmetadata-spec/src/main/resources/json/schema/api/data/createTable.json), [modèle des pipelines](https://github.com/open-metadata/OpenMetadata/blob/1.9.0-release/openmetadata-spec/src/main/resources/json/schema/api/data/createPipeline.json).
