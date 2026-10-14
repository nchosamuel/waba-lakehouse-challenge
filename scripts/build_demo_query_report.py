"""Render read-only, timestamped results of actual Trino queries for the demo."""
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from verification_runtime import output, query, ROOT

fixture = json.loads(output('app', ['python', '-c',
    "from waba.publisher import client; print(client().get_object(Bucket='archive',Key='reports/streaming/latest-fixture.json')['Body'].read().decode())"]))
start = datetime.fromisoformat(fixture['published_at_utc']).isoformat(sep=' ')
queries = [
    ('raw', 'Huit pays dans les tables Raw',
     'SELECT country_code, COUNT(*) AS transactions\nFROM iceberg.raw.bank_transactions\nGROUP BY 1 ORDER BY 1;', ['Pays', 'Transactions']),
    ('fraud', 'Alertes métier de la cohorte simulée',
     "SELECT rule, country_code, COUNT(*) AS alerts\nFROM iceberg.gold.fraud_alerts\nWHERE detected_at >= TIMESTAMP '" + start + "'\nGROUP BY 1,2 ORDER BY 1,2;", ['Règle', 'Pays', 'Alertes']),
    ('lambda', 'Vue Lambda : batch Iceberg + messages Kafka',
     'SELECT country_code, SUM(total_amount_eur) AS total_eur\nFROM iceberg.gold.lambda_daily_transaction_volume\nGROUP BY 1 ORDER BY 1;', ['Pays', 'Montant EUR']),
]
blocks, results = [], []
for ident, title, sql, headers in queries:
    rows = query(sql.rstrip(';'))
    results.append({'section': ident, 'sql': sql, 'rows': rows})
    body = ''.join('<tr>' + ''.join('<td>' + html.escape(
        f'{float(v):,.2f}' if ident == 'lambda' and i == 1 else str(v)
    ) + '</td>' for i, v in enumerate(row)) + '</tr>' for row in rows)
    blocks.append(f'<section id="{ident}"><h2>{html.escape(title)}</h2><pre>{html.escape(sql)}</pre><table><thead><tr>' + ''.join('<th>' + h + '</th>' for h in headers) + '</tr></thead><tbody>' + body + '</tbody></table></section>')
now = datetime.now(timezone.utc).isoformat()
page = '''<!doctype html><html lang="fr"><meta charset="utf-8"><title>WABA — Contrôles Trino exécutés</title><style>body{margin:0;background:#eef3f8;color:#142239;font:22px Arial}header{background:#0c172a;color:white;padding:28px 60px}h1{font-size:34px;margin:0 0 10px}header p{font-size:18px;color:#b6c5d9}nav a{color:#57c8d6;margin-right:30px}section{padding:28px 60px;min-height:850px}h2{font-size:30px}pre{background:#142239;color:#e3edf8;padding:20px;border-radius:12px;font:20px monospace;line-height:1.45}table{width:100%;border-collapse:collapse;background:white;font-size:24px}th,td{padding:13px 18px;border-bottom:1px solid #dae2ec;text-align:left}th{background:#d8e8f3}</style><header><h1>Requêtes Trino exécutées · résultats de contrôle</h1><p>Lecture seule · données fictives · résultats obtenus à ''' + now + '''</p><nav><a href="#raw">Raw · huit pays</a><a href="#fraud">Fraude · cohorte simulée</a><a href="#lambda">Lambda · total unifié</a></nav><p>Cet écran rassemble des résultats réellement obtenus ; ce n’est pas un éditeur SQL.</p></header>''' + ''.join(blocks) + '</html>'
out = ROOT / 'output/playwright'
out.mkdir(parents=True, exist_ok=True)
(out / 'trino-results.html').write_text(page)
(ROOT / 'docs/demo-query-results.json').write_text(json.dumps({
    'executed_at_utc': now, 'runtime': 'Trino 475 on Kubernetes',
    'fixture_batch': fixture['batch_id'], 'results': results,
}, indent=2) + '\n')
print('Actual Trino results:', [(r['section'], len(r['rows'])) for r in results])
