# Superset — premier périmètre métier du niveau 4

Superset 5.0.0 est déployé dans serving, avec un PVC pour sa base de métadonnées locale. Son image inclut le connecteur Trino 0.336.0. Le démarrage crée idempotemment la connexion, dix datasets virtuels et onze graphiques répartis dans les trois dashboards du sujet. Chaque dashboard expose un filtre pays commun aux datasets concernés.

La connexion Trino reste celle du prototype local, sans SSO : aucun rôle viewer ou country_analyst sécurisé n'est revendiqué. La session locale est administrateur ; Keycloak, politiques SQL et tests d'accès négatifs restent nécessaires pour achever le volet sécurité du niveau 4. SQLite, cache mémoire et HTTP localhost sont des choix de démonstration, à remplacer dans une cible de production.

## Sources et interprétation

- **Performance commerciale** : commissions et intérêts documentés de Gold, primes acquises modélisées, frais mobile de Silver ; ARPC pondéré par les clients ; contribution des huit pays aux revenus ; produits associés aux comptes dans le scénario.
- **Risque** : NPL pondéré par encours, consolidé par pays avant le classement couleur ; ratios sinistres/primes calculés à partir des sommes, seuil de 70 % fourni par le sujet ; alertes AML sur trente jours ; délais moyens pondérés par le nombre de sinistres, comparés au SLA simulé.
- **Mobile** : comptages par heure UTC et pays, cinq principaux corridors par pays et montants EUR, échecs par opérateur/pays calculés à partir des tentatives.

Les fichiers source ne comportent ni association compte–produit ni SLA contractuel. `seed_analytics_context.py` ajoute une association déterministe à un produit du bon pays et de la bonne entité, et des SLA fictifs de quinze jours ouvrés IARD / trente jours VIE. Les tables portent les modèles `SUBSCRIPTIONS_SIMULATED_V1` / `SLA_SIMULATED_V1` ; les titres des graphiques signalent la simulation. Cela constitue une hypothèse métier explicite, pas une donnée de souscription ou un contrat réellement fourni.

Le classement affiche jusqu'à dix produits, selon le catalogue disponible. Le scénario initial porte sur février 2026 : l'ARPC affiche donc des points mensuels, sans inventer une tendance sur plusieurs mois. La carte mondiale colore les huit pays ouest-africains ; son cadrage n'est pas une carte régionale dédiée. Les faibles revenus hors assurance et les différences entre pays reflètent les distributions du générateur de démonstration.

## Vérification

`verify_superset.py` exécute les onze requêtes via l'API Superset, contrôle leurs résultats, réapplique un filtre CI à chacune et vérifie les valeurs NPL du scénario. L'inspection du navigateur complète ces assertions : elle vérifie le rendu des graphiques, et a permis de remplacer le type legacy `heatmap` par `heatmap_v2`, enregistré dans Superset 5.0.0. Les captures validées sont dans `docs/screenshots/`.

La santé du pipeline K8s et la reprise des alertes sont vérifiées séparément ; un graphique affiché ne constitue pas une preuve de contrôle d'accès. Le rapport `superset-verification.json` conserve explicitement `sso_verified=false`.

Références : [images Docker Superset](https://superset.apache.org/admin-docs/installation/docker-builds/), [types de graphiques Superset 5.0.0](https://github.com/apache/superset/blob/5.0.0/superset-frontend/packages/superset-ui-core/src/chart/types/VizType.ts).
