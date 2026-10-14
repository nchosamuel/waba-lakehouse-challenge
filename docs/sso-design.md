# Authentification Superset et autorisations

Le profil local déploie Keycloak 26.4.0 dans le namespace `governance`, avec une base H2 persistée sur PVC. Superset 5.0.0 utilise OpenID Connect, le flux authorization code avec PKCE S256, des URI de retour exactes et un client confidentiel. Les comptes, mots de passe et le secret du client sont générés par `bootstrap.py` dans `.env`, ignoré par Git et protégé en lecture. L'import de realm privé est transmis à Kubernetes sur stdin ; aucun mot de passe n'est présent dans le manifeste versionné.

## Exécuter

Après le socle Kubernetes et Superset :

```sh
python3 scripts/kubernetes.py sso
.tools/kubectl --context kind-waba -n governance port-forward svc/keycloak 18080:8080
.tools/kubectl --context kind-waba -n serving port-forward svc/superset 8088:8088
```

Ouvrir http://localhost:8088/login/ et choisir Keycloak. Les identités de démonstration sont `waba-group-admin`, `waba-viewer`, `waba-compliance` et `waba-analyst-ci` (ainsi que les sept autres codes pays en minuscules). Leurs mots de passe correspondent aux clés privées `SSO_GROUP_ADMIN_PASSWORD`, `SSO_VIEWER_PASSWORD`, `SSO_COMPLIANCE_PASSWORD` et `SSO_ANALYST_CI_PASSWORD`, etc. Le compte d'administration du fournisseur d'identité est distinct des utilisateurs métier.

Keycloak conserve les utilisateurs et le realm lors des redémarrages. L'import au démarrage ignore un realm existant : changer le fichier d'import ne modifie donc pas automatiquement les utilisateurs déjà présents. Une rotation ou modification doit utiliser l'administration Keycloak, puis actualiser les secrets des clients concernés.

## Droits côté Superset

| Profil Keycloak | Droits |
|---|---|
| `group_admin` | Administration Superset et accès groupe. |
| `country_analyst` + un unique rôle `country_CI`, etc. | Jeux de données déclarés et rapports réglementaires ; filtre de lignes serveur `country_code = 'CI'`, appliqué à chaque dataset. SQL Lab interdit. |
| `compliance_officer` | Uniquement les deux datasets physiques Gold `regulatory_bceao_daily` et `regulatory_cima_daily`. Les tableaux de bord généraux ne sont pas accessibles. SQL Lab interdit. |
| `viewer` | Liste explicite des dix datasets d'agrégats des tableaux de bord ; aucun dataset détaillé de comptes, IBAN ou sinistres individuels. SQL Lab interdit. |

Les comptes sans profil reconnu, avec plusieurs profils fonctionnels ou avec un analyste sans pays unique sont refusés. Les rôles sont synchronisés à la connexion ; les huit règles de lignes sont provisionnées de manière idempotente. Les filtres de tableau de bord peuvent être retirés ou modifiés sans enlever les restrictions serveur. Les sous-requêtes SQL ad hoc sont désactivées pour empêcher une expression de graphique de consulter une autre table. Les datasets sont autorisés par une liste explicite : ajouter un dataset ultérieurement ne lui attribue pas automatiquement l'accès lecteur.

Pour respecter la mémoire du Mac, le passage au profil streaming met Superset en pause et conserve Keycloak si Trino est sécurisé ; le profil BI les réactive. Les PVC et identités sont conservés.

La déconnexion efface la session locale et termine la session Keycloak. Une session déjà ouverte doit être renouvelée pour recevoir un changement d'attribution de rôle du fournisseur d'identité. Le compte local Superset généré précédemment est conservé pour l'API privée de provisionnement et de vérification ; l'interface interactive utilise Keycloak.

## Vérification et limites

`python3 scripts/verify_sso.py` effectue de vraies connexions OIDC, teste les onze requêtes sans filtre visuel pour les huit analystes, un filtre contradictoire, les droits lecteur/conformité et le refus de SQL Lab et d'une sous-requête. Le rapport `sso-verification.json` déclare séparément les résultats et la couverture Trino. L'inspection navigateur complète ce contrôle.

Superset et Trino disposent désormais chacun d'un contrôle d'accès vérifié : voir [trino-sso-design.md](trino-sso-design.md) et son rapport distinct. Ce profil utilise des port-forwards locaux et une base H2 persistante ; TLS de bout en bout, PostgreSQL et haute disponibilité restent nécessaires pour une cible de production. Quand Trino sécurisé est activé, Keycloak reste en marche pour renouveler les jetons techniques, y compris en streaming.


Références de configuration : [Superset OAuth et mapping des rôles](https://superset.apache.org/admin-docs/configuration/configuring-superset/), [sécurité Superset](https://superset.apache.org/admin-docs/security/), [conteneurs Keycloak](https://www.keycloak.org/server/containers), [hostname et backchannel Keycloak](https://www.keycloak.org/server/hostname), [import du realm](https://www.keycloak.org/server/importExport).
