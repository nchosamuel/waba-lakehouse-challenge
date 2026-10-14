# Kubernetes local — socle et exécution native de Spark

## Périmètre

Le profil cible un cluster kind mono-nœud sur Apple Silicon. Les composants du socle sont des Deployments et Services Kubernetes : MinIO, catalogue REST Iceberg, Trino, Streamlit, Kafka, NiFi et Airflow. Les traitements batch et streaming utilisent des SparkApplications Kubeflow, avec un driver et un executor distincts. Il ne s'agit pas d'une JVM Spark `local[2]` emballée dans un Pod.

Cinq namespaces sont déclarés : ingestion, processing, serving, governance et monitoring. Les deux derniers sont réservés aux composants de gouvernance et d'observabilité restant à installer. Des Services ExternalName gardent les noms courts du code existant et routent les échanges entre domaines.

## Installation et persistance

`python3 scripts/kubernetes.py start` prépare le cluster, construit et importe les images natives, initialise les secrets et buckets, applique les ressources du socle et installe Spark Operator 2.2.0. Le bootstrap des données est une étape distincte, reproductible, décrite dans le README.

Le stockage Kubernetes est indépendant des volumes Docker Compose. Les PVC conservent MinIO, le catalogue SQLite, les métadonnées et logs Airflow, Kafka, les cinq dépôts NiFi et les checkpoints Spark. La commande `stop` arrête les workloads sans supprimer les PVC ou le cluster. Supprimer le cluster kind détruirait son stockage local ; aucune commande du helper ne le fait.

Les checkpoints du streaming sont montés dans le driver **et** l'executor : les opérateurs de déduplication sauvegardent leur état côté executor. Le PVC ReadWriteOnce est adapté au nœud unique de cette démonstration. Un déploiement multi-nœud exige un stockage partagé compatible, un placement cohérent ou une stratégie de checkpoints distante validée.

## Correction du passage au calcul distribué

L'ingestion normalise les CSV sur le driver pour conserver les numéros de ligne, les enregistrements bruts et les erreurs de structure. La version locale lisait ensuite ce fichier depuis `/tmp`. Un executor dans un autre Pod ne peut pas accéder à ce chemin.

En mode Kubernetes, chaque tentative publie un objet temporaire unique dans `archive/processing-staging/` ; tous les executors le lisent via S3A. L'objet temporaire de la tentative est supprimé uniquement après ingestion, archivage et libération des caches réussis. Un échec conserve source et staging pour diagnostic. Hadoop AWS 3.3.4 et AWS SDK bundle 1.12.262 correspondent au Hadoop du runtime Spark. Les données Iceberg continuent d'utiliser S3FileIO d'Iceberg.

Les templates de probes du driver sont présents dans l'image applicative **et** montés dans l'opérateur : c'est l'opérateur qui lit le template lors de la soumission. Les premières tentatives ont révélé ces deux différences avec Compose ; les ressources d'échec sont conservées pour diagnostic, distinctes des exécutions réussies.

## Accès et santé

Les valeurs privées sont générées dans `.env`, puis injectées sur stdin dans des Secrets. Aucun manifeste versionné ne contient ces valeurs. Le Secret Spark contient seulement les variables S3 et Python nécessaires ; il ne contient pas les identifiants Airflow ou Superset. Le runner Airflow utilise un ServiceAccount autorisé à créer/lire/lister les SparkApplications dans processing. Son API exige également le token du runner et refuse les écritures concurrentes.

MinIO, le catalogue, Trino, Streamlit et Airflow ont des probes HTTP. Kafka et NiFi ont des probes TCP. Le driver Spark possède des probes de démarrage, readiness et vie sur son UI. La santé applicative des deux requêtes streaming est contrôlée séparément via leur heartbeat. Une readiness bloquée sur le démarrage des requêtes couperait le DNS du Service du driver avant que les executors puissent le rejoindre ; la probe de transport évite cette dépendance circulaire. L’allocateur attend jusqu’à 30 secondes que le driver soit prêt. Les images applicatives incluent le code ; aucun montage depuis le Mac n'est nécessaire dans les Pods.

Les fichiers Ingress fournis sont des exemples de routage avec `ingressClassName: nginx`. Le profil de vérification local utilise des port-forwards liés à localhost ; aucun contrôleur Ingress ni TLS d'entrée n'est revendiqué comme validé. Kafka reste interne au cluster. Ces mécanismes ne constituent pas encore le SSO ni l'autorisation fine du niveau 4.

## Capacité locale et limites

Docker dispose d'environ 8 Go. Les modes seed/batch, streaming et BI sont exécutés successivement ; Airflow et le streaming ne doivent pas écrire simultanément. Superset est mis en pause pendant les essais lourds du streaming. Les drivers et executors Spark sont limités à un cœur et un heap de 1 Go chacun. Le driver et l’executor reçoivent chacun 1 Go supplémentaire pour Python, mémoire native et métadonnées. Une première réserve de 384 Mo a provoqué des OOM pendant l’ingestion complète ; la réserve est corrigée, et l’historique des exécutions Spark UI est borné pour limiter la rétention de plans.

Un seul broker Kafka, réplication de un, bases SQLite et stockage local : ce profil valide le déploiement et les traitements du challenge, sans prétention de haute disponibilité. Une cible de production nécessite entre autres PostgreSQL, plusieurs brokers, gestion de secrets externe/chiffrement, TLS, contrôle d'accès et ressources dimensionnées.

## Références

- [SparkApplications Kubeflow](https://spark.kubeflow.org/en/latest/user-guide/writing-sparkapplication.html)
- [Images multi-plateforme : problème connu de kind](https://kind.sigs.k8s.io/docs/user/known-issues/)
- [Hadoop 3.3.4 et S3A](https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html)
