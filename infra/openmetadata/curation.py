"""Declared financial semantics; descriptions never claim official compliance."""
TEAMS={'BANK':'WABA Banque','INSURANCE':'WABA Assurance','MOBILE_MONEY':'WABA Mobile Money','MICROFINANCE':'WABA Microfinance'}
TERMS={
 'NPL':"Ratio des encours en défaut (ou retard d'au moins 90 jours) sur les encours totaux. Encours de démonstration simulés, portefeuille Microfinance.",
 'ARPC':"Revenus de commissions et intérêts documentés par client actif et par mois. Les montants des virements ne sont pas des revenus.",
 'LossRatio':"Sinistres payés divisés par les primes acquises sur la période. Couverture et primes comptables issues du scénario simulé.",
 'EarnedPremium':"Part de prime attribuée à la période de couverture écoulée ; contexte comptable explicitement simulé.",
 'ProcessingTime':"Délai de traitement des sinistres en jours ouvrés ; calendrier et SLA de démonstration déclarés.",
 'TransactionVolume':"Nombre et montant des transactions réussies. Le montant transféré est distinct des commissions.",
 'CrossBorderTransfer':"Paiement mobile réussi entre un pays expéditeur et un pays destinataire différents.",
 'RegulatoryDemo':"Agrégats de démonstration J+1, avec périmètre BCEAO/CIMA déclaré. Aucun formulaire officiel ou conformité réglementaire complète revendiqué.",
}
GOLD={
 'daily_transaction_volume':(['BANK','MICROFINANCE'],'TransactionVolume',"Activité bancaire quotidienne par pays, entité et type de transaction ; montants réussis et commissions distincts.",[]),
 'npl_ratio_by_country':(['MICROFINANCE'],'NPL',"Encours totaux et en défaut par pays et type de prêt. Portefeuille simulé Microfinance ; ratio pondéré par les encours et non les échéances.",['WABARegulatory.BCEAO']),
 'customer_arpu_monthly':(['BANK','MICROFINANCE'],'ARPC',"Revenus mensuels et clients actifs par pays/entité/segment. Les intérêts proviennent du contexte comptable explicite, et non du montant remboursé total.",[]),
 'loss_ratio_by_product':(['INSURANCE'],'LossRatio',"Sinistres payés et primes acquises par produit/pays/mois ; modélisation des couvertures déclarée et opérations sans contexte exclues.",['WABARegulatory.CIMA']),
 'claims_processing_time':(['INSURANCE'],'ProcessingTime',"Délai des sinistres payés par mois et ligne d'assurance, en jours ouvrés. Données simulées.",['WABARegulatory.CIMA']),
 'mobile_money_daily_flow':(['MOBILE_MONEY'],'TransactionVolume',"Flux mobile quotidiens par pays/opérateur : succès, échecs, montants et frais séparés.",[]),
 'cross_border_transfers':(['MOBILE_MONEY'],'CrossBorderTransfer',"Transferts réussis entre pays : origine, destination, nombre et montant EUR, sans assimilation à du revenu.",[]),
 'regulatory_bceao_daily':(['BANK','MICROFINANCE'],'RegulatoryDemo',"Démonstration BCEAO J+1, encours observés au plus tard à la date du rapport. Les pays hors périmètre sont signalés. Aucun formulaire officiel.",['WABARegulatory.BCEAO']),
 'regulatory_cima_daily':(['INSURANCE'],'RegulatoryDemo',"Démonstration CIMA J+1 : primes acquises au prorata et sinistres payés à date, avec périmètre déclaré. Aucun formulaire officiel.",['WABARegulatory.CIMA']),
}
PII_FIELDS={'customer_id','account_id','account_number','iban','loan_account_id','sender_account','receiver_account','email','phone','full_name','first_name','last_name','address','customer_name','phone_number','subject_id'}
