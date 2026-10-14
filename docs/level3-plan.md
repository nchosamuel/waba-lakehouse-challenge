# Prochain bloc : niveau 3 — streaming

Statut : séquence réalisée et vérifiée dans le profil streaming Docker local. Voir `level3-design.md` et les rapports `streaming-*-verification.json`. Le socle est aussi vérifié sur Kubernetes kind avec Spark Operator ; Superset démarre le niveau 4. Le SSO Superset est vérifié ; SSO Trino, catalogue et observabilité restent à réaliser.

## Séquence de construction et de validation

1. Ajouter Kafka en KRaft et NiFi avec volumes persistants, limites mémoire et secrets générés. Séparer le profil streaming du profil Airflow pour rester dans la mémoire Docker disponible.
2. Configurer dans NiFi ListS3 → FetchS3Object → lecture des CSV → événements JSON avec pays, source et date d'ingestion → quatre topics Kafka. Garder un état de découverte persistant, des files avec back-pressure et une reprise testée.
3. Construire le premier job Spark Structured Streaming : schémas explicites, contrôles de référentiels/devises, DLQ, watermark et déduplication dix minutes, conversion EUR ; écriture Kafka et Iceberg avec reprise vérifiée.
4. Construire les règles de fraude/AML/liquidité depuis Silver ; fenêtres de cinq minutes glissant toutes les minutes, identifiants d'alertes stables et cohortes de test positives/négatives.
5. Configurer Trino pour les topics Kafka et vérifier une requête Lambda qui ne recompte pas des événements déjà présents en batch. La requête indicative du sujet doit être adaptée pour contrôler ce chevauchement.
6. Préparer un test de bout en bout, un test de reprise, une DLQ visible et une démonstration de règles déclenchées. Ne pas présenter le niveau 3 comme complet avant ces preuves.

## Contrôles décisifs

- Une ligne valide apparaît dans le bon topic brut avec sa provenance.
- Une ligne invalide rejoint la DLQ et ne produit pas d'alerte métier valide.
- Un même identifiant rejoué ne duplique pas la table Iceberg.
- Les trois cas de fraude et les seuils AML du sujet ont des tests positifs et négatifs.
- Un redémarrage conserve offsets, checkpoints et données.
- La requête batch/streaming donne le même total que la déduplication globale des événements.

## Sources techniques étudiées

- [Image officielle Kafka 3.9 et mode Docker](https://kafka.apache.org/39/getting-started/docker/).
- [Images NiFi officielles, dont ARM64](https://hub.docker.com/r/apache/nifi/tags).
- [Intégration Kafka et garanties du sink Spark Structured Streaming](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html). Les artefacts devront correspondre à notre Spark 3.5.5, et les garanties du double sink devront être testées explicitement.
