# Suivi — 11 octobre 2026

## Vérifié
- Première tranche : 1 000 transactions CI, sans doublons après réingestion et persistantes après redémarrage du catalogue.
- Génération séparée de l'ingestion ; lots par pays et journée, référentiels stables.
- Trois tests du générateur : cohérence CI/GH, reproductibilité, dates et distinction des lots.
- Nouvelle intégration : 50 transactions CI/GH ajoutées, 1 050 lignes et 1 050 IDs distincts après deux ingestions.
- Ingestion sans fichier : table inchangée.
- Fichier invalide : traitement échoué, fichier conservé, table inchangée.

## Interface vérifiée
Streamlit accessible sur http://localhost:8501. Deux tests fonctionnels réussis : génération ponctuelle avec validation de sélection ; démarrage, répétition et arrêt du mode continu. Affichage inspecté dans le navigateur.

## Étape suivante
Compléter les flux assurance, mobile money et remboursements ; charger les référentiels dans Iceberg ; vérifier les huit tables et les volumes cibles.

## Quatre flux et huit tables — étape validée
- Banque, assurance, mobile money et remboursements disponibles dans Streamlit ; génération des référentiels disponible séparément.
- Quatre tests du générateur réussis, couvrant les huit pays et les schémas explicites.
- Trois tests d’interface réussis, incluant les nouveaux types de données.
- Huit tables Iceberg interrogées via Trino ; quatre flux CI/GH rejoués sans doublons.
- Contrôles SQL des relations comptes/clients, assurance/polices, remboursements/prêts, expéditeurs/destinataires : zéro clé orpheline.
- Rapports : raw-verification.json et raw-integrity.json.
- Volumes complets, quarantaine des lignes et cohérence économique des données pour les KPIs restent à compléter.

## Volumes complets et quarantaine — étape validée
- Référentiels : exactement 500 000 clients, 800 000 comptes, 200 agences et 50 produits dans Trino, huit pays, clés uniques, zéro compte orphelin.
- Volumes de transactions : 360 000 nouveaux événements sur huit pays ; rejeu d’un fichier mobile money de 20 000 lignes sans doublons.
- Fichiers mixtes sur quatre flux : 8 lignes valides ingérées, 24 rejets conservés avec leurs motifs et données brutes ; rejeu sans doublons.
- Profil complet accessible dans Streamlit ; conservation automatique des références complètes et échantillonnage de leurs clés.
- Contrôle de conflits de référentiels ajouté et testé.
- Suite complète : 15 tests réussis (générateur, grands référentiels, protection des références existantes et interface).

## Étape prévue après le socle
Préparer le niveau 2 : définir les règles métier et les données nécessaires aux KPIs, enrichir la cohérence économique des flux, puis construire les transformations Silver/Gold et leur orchestration.

## Niveau 2 — implémenté et vérifié
- Tables physiques Bronze et Silver pour les huit datasets historiques ; types monétaires décimaux, conversion EUR datée et jointures avec les référentiels.
- Sept indicateurs Gold calculés et interrogés via Trino, plus une table de couverture comptable de l'assurance.
- Scénario comptable déclaré : 800 positions de prêt, primes acquises, sinistres liés et corridors transfrontaliers ; NPL de 3 à 8 %, ratios assurance de 50 à 85 % et délais de dix jours ouvrés contrôlés.
- Quatre DAGs exécutés avec succès par le scheduler Airflow ; chaîne ingestion → Bronze → Silver → Gold et reporting à 00h30 UTC, retries et paramètres par pays.
- Rejeu CI/février : empreintes des sept tables Gold identiques et autres pays conservés.
- Reporting au 10 février : acquisition au prorata des primes (3 571,43 EUR), zéro sinistre futur, aucune photographie de prêt future utilisée.
- Page Streamlit Indicateurs : huit vues rendues, filtre pays contrôlé, libellés français.
- Vingt tests de la suite complète réussis, puis six tests métier réussis après ajout du contrôle des dates de publication : 21 tests distincts vérifiés. Correction auditée des dates des 800 remboursements du scénario initial ; clés, montants et référentiels conservés.
- Preuves : `level2-verification.json`, `airflow-verification.json`, `backfill-verification.json` ; hypothèses dans `level2-design.md`.

## Prochaine étape actuelle
Niveau 3 : NiFi/Kafka, validation et déduplication en streaming, détection de fraude/AML et accès Lambda dans Trino. Le niveau 2 reste un démonstrateur local : taux FX simulés, périmètre comptable explicite et reporting non officiel.

## Préparation du dépôt et reproduction
- Dépôt privé créé : https://github.com/nchosamuel/waba-lakehouse-challenge ; première version poussée, CI GitHub verte (18 tests indépendants de Docker).
- Sélection Banque/Microfinance ajoutée aux transactions Streamlit, avec références correspondantes ; trois transactions Microfinance vérifiées dans Trino. Suite locale : 23 tests réussis.
- Initialisation `seed_demo.py` ajoutée pour alimenter les quatre flux sur un environnement neuf avant Gold.
- Reproduction sur volumes isolés et trois buckets initialement vides : référentiels complets, quatre DAGs réussis, sept KPIs contrôlés et huit vues Streamlit testées. Rapport : `fresh-install-verification.json`.
- Le premier reporting planifié a été activé trop tôt pendant le test et a échoué faute de Gold ; README et helper de vérification corrigés pour l'activer après le premier calcul Gold. Ce constat reste explicite dans le rapport.
- Explication métier et déroulé de capture ajoutés. Vidéo toujours à enregistrer ; accès des évaluateurs au dépôt privé à organiser.
- Environnement de reproduction arrêté sans suppression des volumes ; environnement habituel restauré.
