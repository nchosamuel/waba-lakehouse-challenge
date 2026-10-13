# Comprendre les deux données métier ajoutées

## NPL : mesurer les encours en défaut

L'encours est le capital que les emprunteurs doivent encore rembourser à une date donnée. Une échéance payée ou impayée n'est qu'un événement du calendrier de remboursement.

Exemple : trois prêts ont respectivement 900 000 €, 90 000 € et 10 000 € de capital restant dû. Seul le dernier est en défaut. Compter les prêts donnerait 1/3 = 33,3 %, mais le NPL pondéré par l'encours vaut **10 000 / 1 000 000 = 1 %**. Compter les échéances impayées ne donne pas non plus cette information.

Il faut une photographie du portefeuille : compte prêt, date, capital restant dû et état du défaut. Notre source simulée `loan_positions` fournit ces champs. Gold additionne les encours des prêts en défaut ou ayant au moins 90 jours d'impayé, puis divise par l'encours total. Le seuil de 90 jours est une hypothèse explicite du démonstrateur.

Le scénario fournit 100 prêts par pays, avec des encours de même taille et 3 à 8 prêts en défaut selon le pays. Ces cibles contrôlent le générateur ; le calcul Gold reste fondé sur les montants, pas sur ces pourcentages imposés.

## Assurance : distinguer encaissement et acquisition

Une prime encaissée est une somme reçue. Une prime acquise correspond à la couverture d'assurance déjà fournie.

Pour une prime de 1 200 € couvrant douze mois, notre modèle linéaire reconnaît 100 € par mois. Après trois mois, 300 € sont acquis et les 900 € restants correspondent à de la couverture future. Si 210 € de sinistres sont payés sur les trois mois, le ratio est **210 / 300 = 70 %**, et non 210 / 1 200 = 17,5 %.

Dans une assurance réelle, le calendrier d'acquisition dépend des contrats et des règles comptables. Ici, nous utilisons une couverture mensuelle explicite et une acquisition linéaire par jour. Au 10 février 2026, dix jours sur vingt-huit sont écoulés : pour 10 000 € de primes mensuelles, 3 571,43 € sont acquis.

Le CSV initial indiquait les paiements de primes et les opérations de sinistre, mais pas les dates de couverture ni le rattachement d'un sinistre entre soumission et paiement. `accounting_context` ajoute ces informations et les intérêts effectivement reçus des remboursements. Les anciennes opérations sans contexte restent visibles et sont comptées parmi les exclusions ; elles ne servent pas à fabriquer un ratio comptable.

## Pourquoi c'est un scénario simulé

Le challenge demande des données fictives. Ces données complémentaires représentent les informations qu'un système de prêts et un système d'assurance devraient fournir en situation réelle. Elles sont générées avant le calcul des indicateurs ; Gold ne choisit pas son dénominateur pour obtenir artificiellement un ratio cible.

Nos hypothèses restent limitées : portefeuille de prêts microfinance, taux de change configurés pour la simulation, couverture linéaire et calendrier sans jours fériés. Les tableaux ne sont pas des déclarations réglementaires officielles.

## Pourquoi les traitements passent un par un

Airflow organise les étapes ; Spark effectue les calculs. Lancer simultanément plusieurs Spark consommerait davantage de mémoire. Sur ce Mac, Airflow utilise un exécuteur séquentiel, un pool Spark d'un slot et un service de soumission verrouillé. Les fichiers attendent leur traitement ; les données ne sont pas supprimées pour économiser de la RAM.
