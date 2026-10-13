# Décisions et hypothèses

## Périmètre actuel
Première tranche : Côte d'Ivoire, transactions bancaires, petits référentiels.
Ce prototype ne constitue pas encore un Level 1 complet.

## Contrat de données
- Ajouter `entity_type` aux comptes et `customer_id` aux transactions pour satisfaire les contraintes générales.
- Schéma products défini explicitement : le document ne le fournit pas.
- Génération déterministe pour pouvoir reproduire les validations ; aucune donnée personnelle réelle.
- GN désigne la Guinée, hors UEMOA : l'association GN/XOF du document sera traitée comme une convention de simulation, pas comme une vérité géographique.
- Les seuils réglementaires du sujet sont des paramètres de simulation à vérifier avant toute affirmation réglementaire réelle.

## À résoudre avant Level 2
Encours et défauts de prêts pour NPL ; intérêts pour ARPC ; primes acquises et identifiants de police/sinistre ; jours ouvrés et calendrier ; conversion EUR avec taux datés.

## Sources techniques
- https://iceberg.apache.org/spark-quickstart/
- https://trino.io/docs/current/object-storage/metastores.html

## Limites
Dates de démo fixes au 1 janvier 2026 ; volumes réduits ; un processus d'ingestion à la fois.

## Construction MinIO
Les images officielles historiques ne sont plus accessibles anonymement (constaté le 11 octobre 2026). Image construite depuis le code source upstream, tag RELEASE.2025-04-22T22-12-26Z, pour la démo locale. Version ancienne, pas une recommandation de déploiement en production. Source : https://github.com/minio/minio

## Première architecture
MinIO stocke les CSV et les fichiers de tables. Le catalogue REST conserve les métadonnées nécessaires à Spark et Trino. Spark traite et écrit ; Trino interroge les mêmes tables sans recopier les données. Fusion sur transaction_id pour rendre la réingestion idempotente. Archivage après succès seulement.

## Génération séparée et lots
Streamlit publie les lots ; Spark découvre les fichiers dans raw-landing. Un nouveau lot reçoit un UUID, inclus dans les identifiants déterministes des transactions et les noms de fichiers. Une copie du même fichier conserve ses transaction_id pour le contrôle d'idempotence.
Les référentiels sont stables et comparés avant réutilisation ; un conflit arrête la publication. Chaque fichier correspond à un pays et une journée. Dates de début et de fin inclusives.

## Mode continu
Le timer Streamlit repose sur les fragments, pendant la session active. Aucun job Spark n'est démarré par l'interface. La génération continue sera séparée en worker autonome si une exécution sans navigateur est nécessaire.
Source : https://docs.streamlit.io/1.46.0/develop/api-reference/execution-flow/st.fragment

## Extension des quatre flux
Référentiels étendus par ajout de clés stables, sans modification des lignes bancaires existantes. Migration de contenu CSV contrôlée : toute modification d’une clé existante bloque la publication.
Les opérations assurance utilisent operation_id (le schéma annexe ne définit pas policy_id). Comptes de police liés à leur client ; remboursements liés à des comptes LOAN ; émetteurs/destinataires mobile money liés aux clients du même métier.
Les tables sont fusionnées sur clé métier + country_code. Les référentiels ont ingestion_date ; les flux ont txn_date. Une copie des référentiels est archivée mais leur source reste disponible pour la génération.
Le champ country_code ajouté aux paiements mobiles correspond au pays de l’expéditeur. Paiements transfrontaliers et données économiquement corrélées pour les KPIs restent à compléter.

## Volumes et migration des référentiels
Les volumes du sujet sont interprétés comme totaux groupe : 500 000 clients, 800 000 comptes, 200 agences, 50 produits. Répartition en huit fichiers par référentiel ; les noms historiques sont conservés sous le préfixe pays. Les clés des premiers exemples sont préservées. Les nouvelles clés utilisent un index stable, et l’extension vérifie les lignes existantes avant remplacement. Les lots ultérieurs échantillonnent les références complètes, sans listes de 800 000 comptes en mémoire.

## Rejets par ligne
CSV normalisé sur disque pour conserver les données brutes et détecter les colonnes en trop/manquantes. Lecture typée en PERMISSIVE, sans pruning CSV, puis validations métier et clés étrangères. Une ligne rejetée conserve sa source, son numéro de ligne logique, les valeurs brutes et les motifs. Toutes les occurrences d’un ID répété dans un même fichier sont rejetées. Les lignes valides sont fusionnées, puis le fichier original est archivé. Un en-tête incohérent, une erreur de parsing irrécupérable ou un référentiel invalide bloque le traitement et reste dans le landing.
Source technique : https://downloads.apache.org/spark/docs/3.5.0/sql-data-sources-csv.html

## Limite d’exécution Spark
Lecture des CSV depuis un disque temporaire partagé par le driver et ses exécuteurs local[2]. Intermédiaires persistés sur disque. Pour un déploiement Spark distribué sur K8s, ajouter un accès direct au stockage objet pour la lecture CSV.

## Niveau 2 — modèle comptable et orchestration locale
- Sources d'encours et de comptabilité ajoutées explicitement : ne pas assimiler une échéance à un encours ni une prime encaissée à une prime acquise.
- Historique assurance sans contexte exclu des ratios, avec mesure de couverture. ARPC limité aux revenus observés et intérêts documentés.
- FX simulé avec source et validité ; aucune revendication de taux de marché actuel.
- GN/XOF conservé selon le contrat du sujet, mais GN exclu des corridors UEMOA et périmètres BCEAO/CIMA réels de la démo ; sources officielles dans `level2-design.md`.
- Airflow SequentialExecutor/SQLite, pool d'un slot et endpoint privé Spark authentifié pour tenir dans environ 8 Go Docker. Aucun socket Docker exposé à Airflow.
- MERGE sur clés métier pour Bronze/Silver ; reconstruction atomique de tout l'historique Gold des pays sélectionnés, afin de préserver les agrégats mensuels et les autres pays.
- Reporting J+1 : photographie antérieure seulement, sinistres effectivement payés jusqu'à D et primes acquises au prorata. Exports pédagogiques, sans format officiel revendiqué.
- Dates du scénario initial de remboursements corrigées via un job borné à ses 800 IDs, sauvegarde des lignes originales dans `archive/reports/repairs/`, sans changement des montants ni des référentiels. Les nouvelles générations produisent directement les dates cohérentes.
