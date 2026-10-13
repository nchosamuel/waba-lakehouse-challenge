"""Explicit raw data contracts shared by producer and consumer."""
REFERENCES = ('customers', 'accounts', 'branches', 'products')
FLOW_PREFIX = {'bank_transactions': 'bank_txn', 'insurance_operations': 'insurance_ops',
               'mobile_money_payments': 'mobile_money', 'loan_repayments': 'loan_repayments'}
KEYS = dict(customers='customer_id', accounts='account_id', branches='branch_id', products='product_id',
            bank_transactions='transaction_id', insurance_operations='operation_id',
            mobile_money_payments='payment_id', loan_repayments='repayment_id')
# country_code joins product_id because the catalogue is replicated per country.
SCHEMAS = {
 'customers': 'customer_id:string country_code:string entity_type:string segment:string kyc_level:string onboarding_date:date region:string is_active:boolean',
 'accounts': 'account_id:string customer_id:string country_code:string entity_type:string account_type:string currency:string balance:double credit_limit:double opened_date:date status:string',
 'branches': 'branch_id:string country_code:string entity_type:string city:string region:string branch_type:string is_active:boolean',
 'products': 'product_id:string country_code:string entity_type:string product_name:string product_type:string',
 'bank_transactions': 'transaction_id:string timestamp:timestamp account_id:string customer_id:string beneficiary_account:string branch_id:string country_code:string transaction_type:string amount:double currency:string channel:string transaction_status:string fee_amount:double entity_type:string',
 'insurance_operations': 'operation_id:string timestamp:timestamp customer_id:string account_id:string country_code:string operation_type:string product_line:string amount:double currency:string claim_status:string processing_days:int entity_type:string',
 'mobile_money_payments': 'payment_id:string timestamp:timestamp sender_id:string receiver_id:string sender_country:string receiver_country:string country_code:string amount:double currency:string payment_type:string operator:string status:string fee_amount:double entity_type:string',
 'loan_repayments': 'repayment_id:string timestamp:timestamp loan_account_id:string customer_id:string country_code:string amount_due:double amount_paid:double currency:string due_date:date payment_date:date days_overdue:int loan_type:string repayment_status:string entity_type:string',
}
