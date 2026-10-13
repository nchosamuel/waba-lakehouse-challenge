"""Bounded-memory reference generation; existing demo keys remain unchanged."""
import csv
import functools
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from waba.generator import COUNTRIES, generate
from waba.contracts import REFERENCES, SCHEMAS

TARGETS = {'customers': 500_000, 'accounts': 800_000, 'branches': 200, 'products': 50}


@functools.lru_cache(maxsize=8)
def baseline(country):
    with TemporaryDirectory() as folder:
        data = generate(folder, country, 1, datasets_to_generate=['references'])
    return {name: data[name] for name in REFERENCES}


def count_for(name, country):
    index = COUNTRIES.index(country)
    count, remainder = divmod(TARGETS[name], len(COUNTRIES))
    return count + (index < remainder)


def entity_for(index):
    value = index % 100
    return 'BANK' if value < 55 else 'INSURANCE' if value < 75 else 'MOBILE_MONEY' if value < 93 else 'MICROFINANCE'


def reference_row(name, country, index):
    base = baseline(country)[name]
    if index < len(base):
        return base[index]
    currency = 'GHS' if country == 'GH' else 'XOF'
    entity = entity_for(index)
    if name == 'customers':
        return dict(customer_id=f'WABA-{country}-SC-{index:06d}', country_code=country,
                    entity_type=entity, segment=('RETAIL','SME','CORPORATE','PREMIUM')[index % 4],
                    kyc_level='STANDARD', onboarding_date='2025-01-01', region=country, is_active=True)
    if name == 'accounts':
        customer = reference_row('customers', country, index % count_for('customers', country))
        entity = customer['entity_type']
        account_type = {'BANK':'CURRENT', 'INSURANCE':'INSURANCE_POLICY',
                        'MOBILE_MONEY':'MOBILE_WALLET', 'MICROFINANCE':'LOAN'}[entity]
        return dict(account_id=f'WABA-{country}-SA-{index:07d}', customer_id=customer['customer_id'],
                    country_code=country, entity_type=entity, account_type=account_type,
                    currency=currency, balance=100000, credit_limit=0,
                    opened_date='2025-01-01', status='ACTIVE')
    if name == 'branches':
        return dict(branch_id=f'WABA-{country}-SB-{index:03d}', country_code=country,
                    entity_type=entity, city=country, region=country,
                    branch_type='FULL_SERVICE', is_active=True)
    if name == 'products':
        return dict(product_id=f'{country}-SP-{index:03d}', country_code=country,
                    entity_type=entity, product_name=f'Produit {entity} {index}', product_type=entity)
    raise ValueError('Unknown reference')


def write_reference(path, name, country):
    count = count_for(name, country)
    with Path(path).open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=[p.split(':')[0] for p in SCHEMAS[name].split()])
        writer.writeheader()
        for index in range(count):
            writer.writerow(reference_row(name, country, index))
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return {'rows': count, 'sha256': digest.hexdigest(), 'bytes': Path(path).stat().st_size}
