# Trino : SSO, HTTPS et contrôle SQL

Trino 475 utilise Keycloak 26.4.0 pour sa Web UI et ses clients SQL. Les humains suivent le flux OpenID Connect authorization code. Le serveur vérifie la signature, l'émetteur et l'audience du jeton ; les rôles SQL proviennent de la claim signée `waba_trino_roles`. La politique fichier refuse les ressources non autorisées et toute usurpation d'identité. Les noms fournis par un client SQL ne suffisent pas à s'authentifier.

## Déployer et vérifier

Après le socle, les données, Superset et son SSO :

```sh
python3 scripts/kubernetes.py sso
# Conserver le port-forward Keycloak localhost:18080 -> 8080.
python3 scripts/trino_sso.py
.tools/kubectl --context kind-waba -n serving port-forward svc/trino 8843:8443
python3 scripts/verify_trino_sso.py
```

Pour recréer un cluster vierge en réutilisant un ancien `.env` déjà sécurisé, repasser les deux flags SSO à `false` avant le socle, puis réactiver successivement les étapes SSO ci-dessus. Les secrets existants restent conservés.

Ouvrir https://localhost:8843/ui/ pour la connexion via Keycloak. Le certificat local auto-signé est dans `.tools/trino-tls/ca.crt`, ignoré par Git. Les scripts SQL et les connexions des applications vérifient ce certificat et le nom du serveur. Le navigateur de test est une session isolée autorisant ce certificat local ; cela ne modifie pas le magasin de confiance du Mac. Le contrôle TLS automatique indépendant vérifie bien le certificat. Aucun secret, jeton ou mot de passe n'est publié.

`46-trino-security.yaml` contient le profil de configuration sécurisé. Le certificat PKCS12, son mot de passe, les secrets des clients et la clé de communication interne sont transmis par Secrets Kubernetes, sans valeurs privées dans les manifestes. Le certificat local expire au bout de trente jours : il doit être renouvelé pour une utilisation ultérieure. Le profil Compose d'origine demeure une démonstration non sécurisée distincte ; les preuves de cette étape concernent le profil Kubernetes sécurisé.

## Accès humains

| Profil | Autorisation directe dans Trino |
|---|---|
| `group_admin` | Accès aux données de toutes les entités et administration SQL. |
| `country_analyst` + `country_CI`, etc. | Lecture Iceberg ; filtre serveur `country_code = 'CI'` sur les tables raw, Bronze, Silver, Gold et les vues BI. Écritures et usurpation d'un autre utilisateur interdites. |
| `compliance_officer` | Lecture de `gold.regulatory_bceao_daily` et `gold.regulatory_cima_daily` uniquement. |
| `viewer` | Lecture des dix vues explicitement déclarées dans `iceberg.bi` ; aucun accès aux comptes, IBAN ou sinistres individuels. Les jointures et sous-requêtes vers les tables détaillées sont également refusées. |

Les rôles sont attribués dans Keycloak. Les comptes analystes provisionnés ont chacun un seul pays. Les règles SQL utilisent les groupes du jeton, et non une liste de noms d'analystes. Une modification du rôle prend effet lors de l'émission du jeton suivant ; les jetons existants demeurent valides jusqu'à leur expiration. La déconnexion de chaque application ferme sa session locale et la session IdP ; elle ne revendique pas la fermeture automatique de toutes les autres sessions applicatives déjà ouvertes.

Les vues BI sont `SECURITY DEFINER` : leur propriétaire technique peut lire les sources Silver/Gold nécessaires, alors que le lecteur ne peut consulter que les résultats agrégés. Trino reconstruit l'identité du propriétaire d'une vue sans les groupes OAuth du jeton original. La politique prévoit donc explicitement les droits `SELECT`/`GRANT_SELECT` de cette identité interne sur ces sources. La vue Lambda dispose aussi d'un accès propriétaire au seul topic SQL `kafka.default.silver_bank_transactions`. Ces autorisations ne permettent pas à un humain d'endosser ce compte : l'impersonation est interdite. Les vues existantes sont recréées avec le propriétaire technique authentifié.

## Connexions techniques

Trois clients confidentiels distincts utilisent `client_credentials`, avec des secrets générés dans `.env` :

- `waba-bi` : lecture Silver/Gold/BI pour Superset. Les restrictions par utilisateur Superset restent appliquées par Superset ; cette connexion partagée n'est pas une impersonation du compte humain.
- `waba-dashboard` : lecture Gold pour les indicateurs Streamlit.
- `waba-validation` : identité privilégiée réservée aux contrôles et à la création des vues. Sa clé est disponible dans l'environnement privé de vérification ; aucun utilisateur métier n'en reçoit les identifiants.

Les clients techniques renouvellent leurs jetons courts et utilisent HTTPS vers Trino avec vérification de la CA locale. Les utilisateurs métier n'utilisent pas de grant mot de passe direct. Le listener HTTP 8080 reste disponible pour la découverte interne et les endpoints de santé publics ; les requêtes SQL HTTP sont refusées. Keycloak reste en marche pendant les traitements quand ce profil est activé, car les clients doivent pouvoir renouveler leurs jetons.

## Défense et preuve

Le test a révélé que Trino 475 autorise, dans son implémentation OAuth, des jetons sans audience. Les clients SQL reçoivent une audience explicite `waba-trino`, tandis que Superset reçoit sa propre audience. La claim d'autorisation SQL est réservée aux clients Trino : un jeton d'un autre client sans cette claim ne reçoit aucun accès aux données, même si son identité est reconnue. Aucun contrôle universel de tous les futurs clients Keycloak n'est revendiqué ; leur configuration doit conserver cette séparation. La propriété de mapping de groupes est dépréciée dans Trino 475 ; elle est épinglée et testée ici et devra être revue lors d'une migration de version.

`verify_trino_sso.py` effectue de vrais échanges authorization code et teste la Web UI, la déconnexion locale/IdP, huit analystes sur les cinq couches, des prédicats contradictoires, des tentatives d'écriture et d'usurpation, dix vues lecteur, les deux tables conformité, des jointures et sous-requêtes interdites, les accès sans jeton/invalide/audience incorrecte et le refus de SQL HTTP. Les rapports `trino-sso-verification.json`, `sso-verification.json` et `superset-verification.json` attestent séparément ces contrôles. Les onze graphiques Superset, six tests d'interface Streamlit et vingt-trois tests unitaires ont également été revérifiés. Une requête Streamlit sur la vue Lambda sécurisée réussit.

Cette preuve concerne un déploiement local à un nœud. Keycloak utilise toujours HTTP et H2 persistante ; les UIs Streamlit/Superset utilisent des port-forwards locaux. Une cible de production requiert TLS de bout en bout, certificats renouvelés, base d'identité adaptée, haute disponibilité et contrôle des accès réseau au stockage et au catalogue. Les quatre rôles testés protègent les accès Superset et Trino ; ils ne constituent pas une protection globale de tous les services internes du lac.

Références : [OAuth Trino 475](https://github.com/trinodb/trino/blob/475/docs/src/main/sphinx/security/oauth2.md), [politiques SQL Trino 475](https://github.com/trinodb/trino/blob/475/docs/src/main/sphinx/security/file-system-access-control.md), [mapping des groupes OAuth](https://github.com/trinodb/trino/blob/475/core/trino-main/src/main/java/io/trino/server/security/oauth2/OAuth2Config.java), [validation d'audience](https://github.com/trinodb/trino/blob/475/core/trino-main/src/main/java/io/trino/server/security/oauth2/NimbusOAuth2Client.java), [contrôle du propriétaire de vue](https://github.com/trinodb/trino/blob/475/lib/trino-plugin-toolkit/src/main/java/io/trino/plugin/base/security/FileBasedSystemAccessControl.java).
