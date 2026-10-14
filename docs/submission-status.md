# Périmètre et preuves de la version locale

Cette version implémente les niveaux 1 et 2 du challenge. Le niveau 3 est testé en Docker et sur Kubernetes local, avec Spark Operator. Les composants du niveau 4 sont vérifiés par profils locaux : Superset/SSO, catalogue/lineage et supervision. Le lancement intégral simultané, l’Ingress TLS et la sécurité de production ne sont pas validés ; la vidéo reste à enregistrer.

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
| Git et CI | Publié, CI verte | Dépôt privé GitHub ; source sans `.env` ; 28 tests indépendants de Docker ; statut de cette branche visible dans GitHub Actions. |
| Document technique | Disponible | `technical-writeup.md` : synthèse des niveaux livrés ; documents de conception détaillés associés. |
| Vidéo de 5–10 minutes | Reste à enregistrer | `demo-script.md` fournit le déroulé ; ce document ne remplace pas une vidéo. |
| NiFi/Kafka/streaming/fraude/AML | Vérifié en Docker et Kubernetes local | NiFi/Kafka, deux traitements Spark, DLQ, trois règles de fraude, AML et liquidité ; vue Lambda sans chevauchement ; reprise et rejeux vérifiés. |
| Kubernetes du socle | Vérifié en kind ARM64 | Sept composants, Spark Operator, driver/executor distincts, PVC, probes ; namespaces séparés ; un DAG exécuté par le scheduler sur K8s et quatre chargés. Ingress fourni comme exemple, contrôleur/TLS non testé. |
| Superset | Vérifié | Trois dashboards, onze graphiques depuis Trino ; filtre CI testé par API et affichage inspecté. Souscriptions et SLA explicitement simulés. |
| Keycloak / SSO Superset | Vérifié localement | Quatre profils ; restrictions serveur des huit analystes, lecteur limité aux agrégats, conformité aux deux tables réglementaires, déconnexion IdP ; `sso-verification.json`. |
| SSO Trino et restrictions SQL | Vérifié localement | HTTPS, connexion/déconnexion Keycloak ; quatre profils, huit analystes restreints, dix vues lecteur, deux tables conformité ; accès HTTP SQL et usurpation refusés ; `trino-sso-verification.json`. |
| OpenMetadata / lineage / PII | Vérifié localement avec périmètre déclaré | 59 tables/vues découvertes, neuf Gold documentées, 38 colonnes PII ; 29 événements Spark, 42 liens et chemin raw→Bronze→Silver→Gold vérifié API/UI. Règles account_number/iban testées sur fixture vide, colonnes absentes des données d’origine. |
| Observabilité / alertes | Vérifié localement avec adaptations déclarées | Prometheus/Grafana/Loki/Alloy ; cinq sources de télémétrie ; scrape Spark natif et logs JSON sur un vrai job CI ; trois règles Grafana testées via signaux demo distincts, notifications locales et retour à Normal. Lag du checkpoint partagé fraude/AML ; durées par périmètre pays, pas par pays d’un job commun. |

Les rapports JSON existants témoignent des exécutions dont ils sont issus ; ils ne remplacent pas l'exécution des scripts de validation. Les données étant appendues par les générations et les tests, les comptages peuvent augmenter sans perte d'idempotence.
