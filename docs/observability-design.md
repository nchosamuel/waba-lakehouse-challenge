# Supervision locale des pipelines

## Architecture et démarrage

Le profil Kubernetes déploie Prometheus 3.5.0, Grafana 12.2.0, Loki 3.5.5, Alloy 1.10.2 et un exporter Python dédié. Les images sont ARM64 natives. Les trois serveurs sont dans `monitoring` ; Alloy et l’exporter sont dans `processing` pour accéder en lecture seule aux PVC des logs Airflow, du catalogue SQLite Airflow et des checkpoints Spark. Prometheus, Grafana, Loki et le journal des notifications ont leurs propres PVC.

```sh
python3 scripts/observability_mode.py --ingestion
.tools/kubectl --context kind-waba -n monitoring port-forward svc/grafana 3000:3000
.tools/kubectl --context kind-waba -n monitoring port-forward svc/prometheus 9090:9090
.tools/kubectl --context kind-waba -n monitoring port-forward svc/loki 3100:3100
```

Ouvrir http://localhost:3000/d/waba-health. Compte et mot de passe : `GRAFANA_ADMIN_USER` et `GRAFANA_ADMIN_PASSWORD` dans `.env`, générés et envoyés aux Secrets sur stdin. Le dashboard et les trois alertes sont provisionnés depuis les fichiers versionnés. Les identifiants ne sont pas dans ces fichiers.

Le profil suspend le catalogue OpenMetadata et arrête Superset/Airflow ; `--ingestion` démarre Kafka et NiFi pour leurs mesures. Le scheduler Airflow reste arrêté : son historique est réellement lu dans SQLite, sans faire tourner tous les traitements. Les mesures absentes apparaissent comme indisponibles, pas comme un zéro inventé. `waba_component_desired_replicas=0` indique une pause volontaire ; `waba_component_ready_replicas` indique la disponibilité réelle. Les autres modes locaux arrêtent la supervision pour libérer la mémoire ; les PVC restent conservés.

Ce démarrage est séquentiel et nécessite le socle Kubernetes déjà initialisé. Le challenge demande aussi un lancement de toute la plateforme en une seule commande : la version locale utilise plusieurs profils pour respecter la RAM disponible, et ne revendique pas encore ce critère de déploiement intégral simultané.

## Mesures et sémantique

| Source | Mesure réelle | Limite et interprétation |
|---|---|---|
| Spark Operator | État des SparkApplications et durée entre dernière soumission et terminaison | Durée du job entier, incluant le démarrage. `country_scope=CI` est un job CI ; `ALL` est un job partagé, pas huit durées individuelles. |
| Spark natif | `/metrics/executors/prometheus` du driver découvert dynamiquement par Prometheus | Existe uniquement pendant que le driver tourne ; un scrape natif réussi a été vérifié pendant le job CI. |
| Kafka / Spark | Offsets de fin du broker moins offsets du dernier micro-lot **committé** dans le checkpoint | Structured Streaming ne valide pas ses offsets comme un consumer group classique. `silver_to_gold` regroupe fraude, AML et liquidité ; il n’existe pas de groupe AML séparé. Les offsets des lots non committés ne sont jamais considérés traités. |
| Streaming | Heartbeat, durée du dernier micro-lot et débit d’entrée depuis le fichier de santé | Le fichier persiste à l’arrêt du streaming : afficher son âge et `waba_stream_expected` avant d’interpréter le débit comme actuel. |
| NiFi | FlowFiles sortants, octets sortants et file d’attente depuis l’API de statut authentifiée | Statistiques glissantes sur cinq minutes ; débit moyen = FlowFiles sortants / 300, sans les présenter comme des compteurs cumulatifs. |
| Airflow | Répartition des DAG runs sur 24 h et proportion en échec | Lecture SQLite seule ; zéro run n’est pas une preuve que le scheduler est actif. |
| Trino | Santé native `/v1/info`, état du coordinator, réplicas prêts | Pas une instrumentation JMX détaillée des requêtes Trino. |

L’exporter ne consomme aucun message Kafka et ne lit aucune ligne métier via SQL. Il observe les offsets, métadonnées de traitements et statistiques techniques. Chaque source expose `waba_telemetry_available` ; `waba_exporter_collection_timestamp_seconds` permet de contrôler la fraîcheur globale. La lecture du checkpoint et du broker n’est pas atomique : un traitement en cours peut créer un retard légèrement surévalué le temps d’un scrape. Les données retenues par Kafka sont prises en compte via les offsets de début.

## Journaux Loki

Alloy collecte les logs des Pods des cinq namespaces WABA via l’API Kubernetes, sans conteneur privilégié ni accès au disque de l’hôte. Il lit aussi les fichiers de tâches Airflow du PVC de logs. `loki.process` parse les lignes JSON et extrait les labels `event`, `job`, `country_code` et `level`. Les identifiants clients, numéros de compte et batch IDs ne sont pas utilisés comme labels.

Le challenge cite Promtail ou Fluentd. Ici Alloy remplace Promtail, arrivé en fin de maintenance le 2 mars 2026, selon la [documentation Grafana](https://grafana.com/docs/loki/latest/send-data/promtail/). C’est une adaptation explicite du collecteur, avec Loki et le parsing JSON conservés. Les logs antérieurs au démarrage du collecteur ne sont pas supposés tous présents : un nouveau job Spark réel a été exécuté pour vérifier la chaîne complète.

Loki utilise un stockage local avec rétention de 72 heures, une seule réplique et aucune authentification interne. Alloy conserve ses positions dans un volume temporaire ; après redémarrage, certains logs de fichiers peuvent être rejoués. Ce profil n’offre ni haute disponibilité, ni archivage durable externe, ni garantie d’absence de doublons dans les logs.

## Trois alertes Grafana et notifications locales

| Règle | Condition réelle | Test |
|---|---|---|
| Fraude Spark | SparkApplication `waba-streaming` en erreur de manière continue pendant **5 minutes** | Signal de démonstration tenu jusqu’au passage Pending → Alerting, sans raccourcir le délai. |
| Kafka AML | Retard du checkpoint `silver_to_gold` **strictement supérieur à 5 000** messages | 5 000 ne déclenche pas ; 5 001 déclenche. |
| Rapport réglementaire | À partir de **06:00 UTC**, le run **planifié** de `dag_regulatory_report` dont l’intervalle se termine aujourd’hui doit être réussi | Le signal de test déclenche la même règle ; les tests unitaires vérifient l’échéance exacte, UTC et l’absence de run. Un run manuel ancien ne satisfait pas l’échéance. |

L’évaluation se fait toutes les 30 secondes. Les règles ont des instances `scenario=live` et `scenario=demo` ; l’exporter conserve les observations réelles et fournit les signaux de test sous des noms de métriques **distincts** `waba_demo_*`. Les règles de production ne sont ni réécrites ni mises en pause pour la vérification.

Les notifications sont exclusivement envoyées à un webhook du cluster. Celui-ci conserve seulement date, statut et quelques labels techniques dans un PVC, puis écrit un événement JSON recueilli par Loki. Aucun email, message externe ou notification à un tiers n’est envoyé. Les alertes de démonstration sont remises à zéro et leur retour à Normal est vérifié. Une vraie alerte peut rester active : dans cette démo, aucun run planifié du jour n’a réussi avant l’échéance ; le job de contrôle manuel pour février ne la fait pas disparaître.

```sh
python3 scripts/verify_observability.py
python3 scripts/verify_observability_spark.py
```

Ces commandes nécessitent les trois port-forwards. La première dure environ huit à douze minutes, incluant propagation des ConfigMaps, scrape et attente de cinq minutes. Elle remet les signaux à zéro même en cas d’échec. La seconde arrête temporairement Kafka/NiFi, rejoue de manière idempotente le rapport CI du 28 février 2026, vérifie les métriques Spark natives et son événement JSON dans Loki, puis restaure les réplicas d’ingestion.

## Réserves de production et références

Le service account d’observation a uniquement lecture des Pods/logs, déploiements et SparkApplications dans les cinq namespaces WABA ; il ne lit pas les Secrets et ne peut pas modifier les traitements. L’exporter possède les identifiants NiFi du profil local, sans séparation d’un rôle NiFi de supervision. Le certificat NiFi auto-signé n’est pas validé pour cet accès interne de démonstration. Grafana utilise un compte local, pas le SSO Keycloak des applications métier. Les UIs sont exposées par port-forward localhost ; l’Ingress fourni est un exemple non vérifié avec contrôleur et TLS.

Avant production : authentifier Loki et le webhook, TLS/NetworkPolicies, rôles NiFi dédiés, catalogue Airflow sur PostgreSQL, stockage/redondance externes, positions Alloy persistantes, supervision détaillée JMX et ressources Kubernetes, stratégie d’alertes sur absence/fraîcheur des mesures, versions maintenues et validation sur charge réaliste. Les métriques actuelles vérifient le fonctionnement local ; elles ne prouvent pas un niveau de service en production.

Sources techniques : [collecte Kubernetes Alloy](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.kubernetes/), [parsing JSON](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.process/), [provisionnement des alertes Grafana](https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/), [métriques Spark 3.5.5](https://spark.apache.org/docs/3.5.5/monitoring.html), [API de statut NiFi](https://nifi.apache.org/docs/nifi-docs/rest-api/index.html).
