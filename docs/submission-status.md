# Périmètre et preuves de la version locale

Cette version implémente les niveaux 1 et 2 du challenge. Le streaming de niveau 3 est disponible et testé dans un profil Docker local. Le niveau 4 et le déploiement Kubernetes restent à construire.

| Exigence | État | Preuve / réserve |
|---|---|---|
| Streamlit, génération ponctuelle et continue | Vérifié | Tests d'interface ; le mode continu nécessite une session navigateur ouverte. |
| Pays et ligne métier | Vérifié avec périmètre explicite | Huit pays ; transactions Banque/Microfinance ; assurance, mobile et remboursements sur leurs entités compatibles. Portefeuille de prêts simulé Microfinance uniquement. |
| Référentiels complets et cohérence | Vérifié | 500 000 clients, 800 000 comptes, 200 agences, 50 produits ; clés uniques et absence d'orphelins testées. |
| Nomenclature des référentiels | Adaptation documentée | Fichiers par pays pour publication bornée en mémoire ; tables logiques communes au groupe. |
| Huit tables raw et Trino | Vérifié | `volume-verification.json`, `raw-verification.json`, `level2-verification.json`. |
| Validation et rejets | Vérifié | Quarantaine par ligne, données brutes et motifs conservés ; erreurs irrécupérables bloquantes. |
| Idempotence | Vérifié | Réingestion par identifiant/pays ; rejeu des lots sans doublons. |
| Bronze / Silver / Gold distincts | Vérifié | Comptages et chemins physiques contrôlés dans Trino. |
| Silver : enrichissement et EUR | Vérifié avec hypothèses | Types décimaux, jointures, FX simulé et daté. |
| Sept indicateurs Gold | Vérifié avec couverture déclarée | NPL 3–8 %, loss ratio 50–85 %, délais de dix jours ouvrés pour le scénario ; opérations sans contexte signalées et exclues des ratios assurance. |
| Quatre DAGs Airflow | Vérifié | Exécutions réelles du scheduler, dépendances, retries, paramètres pays/période. |
| Reporting J+1 | Vérifié pour des agrégats de démonstration | 00h30 UTC ; données futures exclues ; primes acquises au prorata. Aucun formulaire officiel revendiqué. |
| Déploiement vierge | Vérifié, avec correction du démarrage documentée | Volumes séparés initialement vides, quatre DAGs réussis et KPIs contrôlés ; `fresh-install-verification.json`. |
| Git et CI | Publié, CI verte | Dépôt privé GitHub ; source sans `.env` ; 20 tests indépendants de Docker ; statut de cette branche visible dans GitHub Actions. |
| Document technique | Disponible | `level2-design.md` : architecture, compromis, hypothèses et limites. |
| Vidéo de 5–10 minutes | Reste à enregistrer | `demo-script.md` fournit le déroulé ; ce document ne remplace pas une vidéo. |
| NiFi/Kafka/streaming/fraude/AML | Vérifié en Docker local | NiFi/Kafka, deux traitements Spark, DLQ, trois règles de fraude, AML et liquidité ; vue Lambda sans chevauchement. Kubernetes reste à déployer. |
| Kubernetes/Superset/SSO/gouvernance/observabilité | Non implémenté dans cette version | Niveau 4 restant à construire. |

Les rapports JSON existants témoignent des exécutions dont ils sont issus ; ils ne remplacent pas l'exécution des scripts de validation. Les données étant appendues par les générations et les tests, les comptages peuvent augmenter sans perte d'idempotence.
