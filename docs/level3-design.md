# Niveau 3 — streaming local et Lambda

## Parcours des événements

NiFi 2.4.0 surveille `raw-landing` avec ListS3, filtre les quatre flux CSV, conserve leur clé source, les récupère via FetchS3Object, convertit les enregistrements en JSON, les sépare, ajoute la provenance et publie dans Kafka 3.9.1. Kafka utilise KRaft, un broker et trois partitions par topic. Les ports Kafka restent internes au réseau Docker.

Les files NiFi sont persistantes et limitées à 1 000 FlowFiles / 16 MB chacune. Fetch/Publish en échec restent en reprise ; les erreurs de conversion rejoignent `archive/nifi-failures/`. Les originaux ne sont pas supprimés par NiFi. Les services S3 utilisent des credentials injectés lors de la configuration, jamais enregistrés dans le code ou dans le rapport exporté.

Deux requêtes Spark Structured Streaming partagent une JVM locale :

1. **Raw → Silver** : JSON explicite, types, références, devises, provenance et FX daté ; watermark et déduplication dix minutes ; écriture dans les tables Silver existantes et les quatre topics Silver. Les rejets conservent leurs offsets, données brutes et motifs dans `dlq-financial-events`.
2. **Silver → Gold** : consommation Kafka et déduplication ; journal durable des événements, puis règles sur fenêtres de cinq minutes glissant chaque minute. Le journal Iceberg conserve l'état nécessaire à une reprise et empêche de recompter un mouvement. Les calculs relisent la période récente de dix minutes pour les fenêtres ; les alertes historiques restent conservées.

Ce deuxième traitement utilise un journal durable et des agrégations Spark de fenêtres dans `foreachBatch`, plutôt qu'un opérateur d'agrégation streaming distinct pour chaque règle. Cela permet de sérialiser les écritures dans la même JVM et de reconstruire les fenêtres après un redémarrage. Ce compromis local demande une optimisation et une rétention adaptées à un volume de production.

## Garanties et reprise

Les checkpoints sont dans un volume persistant, distinct pour chaque requête. Les tables sont fusionnées par identifiant/pays. Un outbox Iceberg enregistre les messages à publier ; Kafka est acquitté avant le marquage `published`. Un crash entre acquittement et marquage peut produire une répétition Kafka : **la publication est au moins une fois**, les consommateurs et les lectures Lambda dédupliquent par identifiant. Le démarrage reprend également les messages d'outbox non publiés.

Les deux callbacks partagent un verrou d'écriture ; ils ne manipulent pas simultanément les vues temporaires et le journal de publication. Les métadonnées Iceberg sont rafraîchies avant la lecture de l'outbox et du journal Gold après chaque écriture. Le test de plusieurs lots sur K8s a révélé une lecture obsolète du journal après une première cohorte : ce rafraîchissement garantit que le second lot participe aux règles. Les assertions de reprise contrôlent les valeurs métier et les identifiants, pas seulement le démarrage d'un processus.

Les événements plus anciens que le watermark peuvent être écartés par Spark ; leurs fichiers CSV restent disponibles et doivent être pris en charge en batch/backfill. Utiliser la journée UTC courante pour une démonstration streaming. `failOnDataLoss=true` empêche de reprendre silencieusement un checkpoint dont les offsets ont expiré. Kafka conserve les messages sept jours dans ce profil local.

## Règles et données de risque

| Règle | Définition implémentée |
|---|---|
| Transactions répétées | Au moins deux transactions réussies, chacune > 500 000 XOF, même compte, dans une fenêtre de cinq minutes glissant d'une minute. |
| Pays inhabituel | Pays d'émission différent du pays du référentiel client. Un client référencé globalement peut payer depuis un autre pays ; cela crée un signal de contrôle, pas une clé orpheline. |
| Sinistre élevé | Sinistre payé > trois primes annuelles du contrat de risque simulé. Les contrats couvrent 100 polices par pays et donnent explicitement leur prime annuelle pour la simulation ; absence de contrat = règle non évaluable. |
| AML | Virement réussi > 1 000 000 XOF ou > 5 000 GHS. Ces valeurs sont les seuils du sujet, pas une validation de la réglementation actuelle. |
| Liquidité | Solde d'ouverture simulé des comptes bancaires CURRENT/SAVINGS actifs + dépôts − retraits observés en streaming ; alerte si inférieur à 10 % de ce solde initial. Les transferts internes ne réduisent pas la liquidité pays. Le seuil de 10 % est une politique de démonstration, pas une norme BCEAO revendiquée. |

Les contrats annuels sont des entrées supplémentaires de risque fictives, cohérentes avec le tarif mensuel du scénario de niveau 2. Le pipeline n'infère pas une prime annuelle réellement payée à partir d'un événement isolé. Ce modèle simplifié ne remplace pas le calendrier des primes effectivement encaissées d'un système d'assurance réel.

Les lignes sans contexte comptable restent exclues des ratios assurance batch. Les métadonnées d'audit de rejets peuvent contenir un pays inconnu et une entité nulle : elles ne sont pas des événements financiers acceptés.

## Lambda sans chevauchement

Trino lit Iceberg et Kafka JSON. `gold.lambda_batch_transactions` contient les transactions batch et leurs montants/statuts dans un seul snapshot atomique Iceberg. La vue `gold.lambda_daily_transaction_volume` joint ces faits aux messages Kafka dédupliqués, donne priorité au fait batch puis agrège les succès. Une interruption du job avant l’écriture de `daily_transaction_volume` ne modifie pas la cohérence de cette vue. L’ancienne table `batch_transaction_membership` reste un artefact de compatibilité, sans rôle dans le calcul Lambda.

Une recomposition Gold met aussi à jour les identifiants couverts. Les modes batch et streaming sont exclusifs pour les écritures dans la démo locale ; cette discipline évite les lectures au milieu d'une mise à jour multi-table. En production, la publication d'une version atomique d'agrégats et de sa frontière de couverture serait nécessaire.

## Ressources et limites de déploiement

Airflow et le runner batch sont arrêtés en mode streaming. Kafka a un heap de 384 MB, NiFi 768 MB et Spark 1 GB ; Trino et MinIO restent accessibles. Le profil Docker est testé sur Apple Silicon. Le mode local garde un seul broker, une réplication de un et des checkpoints sur volume local. Le socle est également vérifié sur Kubernetes kind avec Spark Operator : voir `kubernetes-design.md` et les rapports `kubernetes-*.json`. La haute disponibilité, le SSO, la gouvernance et l’observabilité restent à construire.

NiFi utilise HTTPS avec certificat auto-signé et mot de passe généré. Le helper local accepte ce certificat uniquement sur localhost ; une chaîne de confiance réelle et TLS/SASL Kafka sont des travaux de production ultérieurs.

## Preuves et sources

- `streaming-verification.json` : quatre flux, trois règles de fraude, seuils AML positifs/négatifs, liquidité, DLQ et total Lambda.
- `streaming-recovery-verification.json` : rejeux, redémarrages et conservation des identifiants.
- `streaming-batch-compatibility.json` : reprise batch et conservation du total Lambda sans chevauchement.
- [Kafka : image officielle Docker](https://kafka.apache.org/39/getting-started/docker/).
- [Spark : Kafka, checkpoints et publication au moins une fois](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html).
- [NiFi : ListS3](https://nifi.apache.org/components/org.apache.nifi.processors.aws.s3.ListS3/).

## Corrections issues de la revue

Les règles individuelles examinent chaque événement accepté du micro-lot. La règle multi-transactions recalcule les fenêtres affectées à partir du registre durable, sans tronquer toutes les règles au maximum temporel global. `review-fixes-verification.json` vérifie les événements séparés d’une heure et les totaux Trino avant publication, après interruption injectée et après rejeu.
