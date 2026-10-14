# WABA — architecture, choix et limites

## 1. Périmètre livré

La plateforme réalise les niveaux batch 1–2 et le parcours streaming du niveau 3. Le socle est vérifié en Docker Compose et sur Kubernetes kind ARM64. Superset apporte les trois dashboards métier du niveau 4. Le SSO Superset/Trino et les politiques d'accès par rôle/pays, OpenMetadata avec lineage, ainsi que la supervision et ses alertes sont implémentés et vérifiés par profils Kubernetes locaux. Le lancement intégral simultané et l'exposition par Ingress restent à valider ; la haute disponibilité et la sécurité de production restent hors du périmètre validé.

Le dépôt contient l'application de génération, les traitements PySpark, quatre DAGs, la configuration automatisée de NiFi, les ressources Kubernetes et des scripts de validation indépendants. Les rapports JSON documentent des exécutions réelles ; leurs comptages peuvent évoluer à chaque nouvelle génération. La [vidéo de démonstration](demo/WABA_Demo_FR.mp4) de 9 min 14 s est livrée avec sous-titres français intégrés. Elle présente des captures réelles montées par profils locaux ; les résultats Trino et les alertes de la cohorte sont présentés dans un rapport en lecture seule.

## 2. Socle et données

Streamlit publie des CSV dans MinIO, organisés par pays et flux. Le référentiel complet comporte 500 000 clients, 800 000 comptes, 200 agences et 50 produits. Les fichiers par pays bornent la mémoire utilisée pendant la génération ; les tables logiques consolident les huit pays. Les identifiants sont fictifs et pseudonymes, sans IBAN ou numéro bancaire réel.

MinIO porte les fichiers entrants, les tables Iceberg et les archives. Le catalogue REST Iceberg conserve ses métadonnées dans une base SQLite persistante pour la démonstration. Trino expose les couches raw, Bronze, Silver et Gold. Iceberg permet de fusionner par identifiant/pays plutôt que d'appendre aveuglément les mêmes événements.

L'ingestion impose les contrats de schéma, contrôle les références, la devise, les montants et les doublons internes au fichier. Les rejets conservent données brutes, numéro de ligne et motifs dans une quarantaine ; une référence invalide bloque le chargement. L'original est archivé avant suppression du fichier événement entrant. Les référentiels restent disponibles et leur empreinte évite une relecture inutile. Bronze, Silver et Gold sont des matérialisations physiques distinctes.

## 3. Modèles métier et orchestration

Silver enrichit les événements avec les clients, comptes, agences et produits, et normalise les montants en EUR avec un FX simulé daté. Les taux 2026 utilisés sont 655,957 XOF/EUR et 12,5 GHS/EUR ; ils ne sont pas des cours de marché. Le traitement conserve le XOF pour la Guinée selon le sujet, mais ne la classe pas dans le périmètre UEMOA réel utilisé pour les corridors et agrégats concernés.

Les fichiers ne donnent pas les encours de crédit ni les primes acquises. Un scénario versionné apporte des positions de prêts et un contexte comptable explicite. Le NPL utilise l'encours en défaut divisé par l'encours total, jamais les échéances. Le ratio assurance utilise les primes acquises et les sinistres payés liés. Les opérations sans contexte comptable restent signalées et exclues des ratios concernés. Les seuils BCEAO/CIMA mentionnés dans les graphiques sont ceux du challenge ; aucun formulaire officiel ni conformité réglementaire actuelle n'est revendiqué.

Airflow orchestre ingestion → Bronze → Silver → Gold, avec dépendances, pool d'un traitement, retries et journal d'échec. Un quatrième DAG produit le reporting J+1 à 00h30 UTC, paramétrable par pays et période. Le scheduler et les jobs ont été exécutés réellement. Sur Kubernetes, le runner authentifié soumet des SparkApplications via l'API du cluster et refuse les écritures concurrentes ; ses permissions restent limitées à ces ressources dans processing.

## 4. Streaming et Lambda

NiFi surveille les fichiers événements, convertit les CSV en JSON, ajoute leur provenance et publie dans Kafka. Les files sont persistantes et bornées ; les erreurs de conversion sont archivées, et les échecs d'accès/publication restent en reprise. Kafka utilise un broker KRaft local, trois partitions par topic et sept jours de rétention.

Deux requêtes Structured Streaming partagent un processus de pilotage : Raw → Silver, puis Silver → Gold. Une validation commune aux deux modes évite des règles incompatibles. Les rejets streaming rejoignent une DLQ avec payload, topic, partition, offset et motifs. Le watermark et la déduplication portent sur dix minutes. Un timestamp futur invalide ne peut pas faire avancer arbitrairement ce watermark.

Un journal Iceberg durable fournit les événements des fenêtres de cinq minutes glissant d'une minute. Les règles couvrent transactions bancaires répétées de forte valeur, pays d'émission inhabituel et sinistre supérieur à trois primes annuelles du contrat simulé ; AML et liquidité ont leurs propres alertes. L'entrée annuelle assurance et la politique de liquidité sont explicitement fictives. Le journal est rafraîchi après chaque commit pour éviter qu'un second lot soit évalué depuis une ancienne vue du catalogue.

L'outbox est acquitté par Kafka avant d'être marqué publié. Un crash dans cet intervalle peut répéter un message : la publication est au moins une fois. Les consommateurs et lectures Lambda dédupliquent les identifiants. Chaque événement accepté est examiné par les règles individuelles AML, pays inhabituel et sinistre excessif ; les fenêtres glissantes sont recalculées autour des événements acceptés, même anciens. Les événements trop anciens peuvent être écartés par le watermark ; leurs originaux restent disponibles pour un backfill batch.

La vue Lambda agrège une photographie transactionnelle Gold (`lambda_batch_transactions`) et les messages Silver Kafka, dédupliqués par pays/identifiant. La transaction batch prévaut en cas de chevauchement. Montants, statuts et identifiants batch sont publiés dans un seul commit Iceberg ; les totaux Lambda ne dépendent plus de deux tables publiées séparément. Les autres indicateurs Gold restent des publications indépendantes. Les modes d’écriture sont exclusifs dans ce profil.

## 5. Kubernetes, consommation et validation

Le déploiement utilise cinq namespaces par domaine. MinIO, catalogue, Kafka, NiFi, Airflow et checkpoints ont des PVC. Le code est inclus dans les images, les configurations dans des ConfigMaps et les valeurs privées dans des Secrets injectés au démarrage. Les helpers ciblent explicitement kind-waba et ne suppriment pas le cluster ou ses volumes.

Spark Operator lance un driver et un executor distincts. La normalisation CSV passe par un staging MinIO unique par tentative : le `/tmp` du driver n'est pas lisible dans un autre Pod. Les checkpoints sont partagés entre driver et executor, car l'état de déduplication est écrit côté executor. Le PVC ReadWriteOnce convient au nœud unique ; une cible multi-nœud exige une stratégie de stockage partagé validée. Les ressources, probes et rétention des historiques Spark ont été ajustées après les premiers essais, dont les échecs sont documentés.

Superset interroge Trino pour performance commerciale, risque et mobile money, avec filtre pays. Le NPL est consolidé par pays avant la coloration, les ratios sont pondérés par leurs dénominateurs. Les associations compte–produit et les SLA manquants sont ajoutés comme scénarios identifiés. Le catalogue peut comporter moins de dix produits par pays, et le scénario initial ne fournit qu'un mois d'ARPC : le rendu conserve ces limites au lieu d'inventer une tendance.

Les vérifications couvrent les 1,3 million de lignes de référence, l'unicité et les trois matérialisations, les résultats comptables attendus, la génération ponctuelle/continue, quatre flux streaming, cas positifs/négatifs de risque, DLQ, plusieurs lots, rejeux et redémarrages Kafka/NiFi/Spark, ainsi que les totaux Lambda. Superset est contrôlé par API et inspection visuelle ; une requête correcte ne suffit pas à garantir un graphique correctement enregistré.

Le Mac M3 Pro dispose de 18 Go et Docker d'environ 8 Go. Les essais batch, streaming et BI se succèdent pour respecter ce budget. Réplication Kafka de un, SQLite, stockage local, port-forwards et base Keycloak H2 restent des compromis de démonstration. Les métriques, journaux et alertes sont vérifiés dans le profil de supervision décrit ci-dessous. Le lancement intégral simultané, l'Ingress et son TLS, ainsi que la haute disponibilité restent à valider.

## Preuves détaillées

Consulter `submission-status.md`, `kubernetes-*-verification.json`, `streaming-*-verification.json`, `superset-verification.json` et les documents de conception associés. Le README fournit la reproduction et les modes d'exécution.


### Complément : SSO Superset

Keycloak 26.4.0 est déployé sur Kubernetes avec PVC. Superset 5.0.0 utilise un client OIDC confidentiel, authorization code avec PKCE S256 et des URI de retour exactes. Les rôles group_admin, country_analyst, compliance_officer et viewer sont distincts. Huit règles de lignes serveur limitent chaque analyste à un seul pays ; les lecteurs accèdent à une liste explicite de datasets agrégés, sans SQL Lab ; la conformité uniquement à regulatory_bceao_daily et regulatory_cima_daily. Les profils ambigus ou sans pays unique sont refusés. Les secrets restent privés. Le script verify_sso.py vérifie les connexions, les requêtes autorisées et interdites, les résultats en cache et la déconnexion locale et IdP ; une inspection du navigateur complète la preuve. Ce contrôle protège Superset : l'intégration OIDC et les restrictions de Trino sont maintenant vérifiées dans un rapport distinct. Le profil HTTP local et H2 persistante ne démontre ni TLS ni haute disponibilité. Voir sso-design.md pour le détail et les références.


### Complément : sécurité SQL Trino

Trino 475 impose HTTPS et utilise Keycloak pour la Web UI et les clients SQL. Les rôles proviennent d'une claim signée réservée aux clients Trino. Les analystes sont filtrés par pays sur raw/Bronze/Silver/Gold/BI ; le lecteur uniquement sur dix vues agrégées SECURITY DEFINER ; la conformité uniquement sur les deux tables Gold réglementaires. Les connexions techniques BI, indicateurs et vérification sont distinctes. Les noms d'utilisateur fournis sans jeton et toute usurpation sont refusés. La Web UI, la déconnexion, les huit pays, les permissions négatives et les graphiques ont été contrôlés. Le certificat local est vérifié par les clients. Trino 475 accepte l'absence d'audience : les clients actuels en reçoivent une explicite et les droits SQL ne sont pas transmis dans les jetons des autres applications. Le compte propriétaire interne des vues a les seuls droits de lecture/délégation nécessaires hors de son jeton. Les preuves et limites sont dans trino-sso-design.md et trino-sso-verification.json. Ce profil local ne protège pas à lui seul le réseau et tous les services du stockage.


### Complément : catalogue et traçabilité

OpenMetadata 1.9.0, PostgreSQL et OpenSearch ARM64 sont déployés avec stockage persistant. Un Job/CronJob externe découvre 59 tables/vues Trino par HTTPS et une identité dédiée en lecture seule ; aucun échantillon n’est collecté. Neuf tables Gold reçoivent descriptions, équipe WABA propriétaire, glossaire financier et tags réglementaires pertinents. Les noms de champs et leur contexte permettent de taguer 38 colonnes sensibles. Les colonnes account_number/iban absentes des sources ne sont pas inventées ; leurs règles sont testées sur une fixture vide retirée. Les helpers Spark capturent leurs entrées Iceberg réelles après écriture et archivent 29 événements ; 42 liens sont importés avec leurs pipelines. Le chemin raw→Bronze→Silver→Gold est vérifié par API et dans l’UI. La capture est au niveau table et ne couvre pas toutes les écritures streaming/SQL ; elle n’est pas transactionnelle avec le commit. Le mode catalogue et le mode traitements sont séparés en mémoire. Les preuves et limites de sécurité/routage du profil local sont dans openmetadata-design.md.


### Complément : supervision et alertes

Prometheus collecte un exporter de métadonnées techniques et les métriques natives des drivers Spark actifs. Grafana affiche la santé des services, durées des jobs par périmètre pays, retard réel entre broker Kafka et checkpoint committé, statistiques NiFi et proportion de DAG runs Airflow en échec. Les sources indisponibles et services volontairement arrêtés sont distingués. Alloy collecte les logs Kubernetes et fichiers des tâches Airflow ; les événements JSON Spark sont parsés puis interrogés dans Loki. Un rapport CI rejoué avec succès en 32 secondes a permis de vérifier les scrapes Spark natifs et la collecte de son événement de fin.

Trois règles Grafana gardent les seuils du challenge : fraude en erreur cinq minutes, retard AML supérieur à 5 000 messages et rapport planifié non réussi à 06 h UTC. Leur test utilise des métriques séparées et l’étiquette scenario=demo, sans modifier les observations réelles ; les notifications vont à un webhook local, sans message externe. Kafka n’a pas un groupe AML séparé : le retard du checkpoint silver_to_gold partagé fraude/AML/liquidité est mesuré. La supervision utilise un profil mémoire séparé ; les services critiques ne sont pas tous actifs simultanément. Voir observability-design.md et les rapports de vérification pour le périmètre et les réserves de production.

Sur le moteur Docker local (7,65 GiB), le contrôle complet des quatre DAGs a révélé des arrêts mémoire d’Airflow et Trino en concurrence avec les grands traitements. Airflow dispose maintenant de 2 GiB ; le mode `batch --lean` suspend Trino pendant ces écritures. La chaîne planifiée ingestion → Silver → Gold et un rapport de février ont ensuite réussi sur Spark Operator. Les essais interrompus et leur retard ne sont pas effacés. Ce profil séquentiel ne constitue pas une validation de déploiement intégral simultané.
