"""Create explicit aggregate-only SECURITY DEFINER views for SQL viewers."""
import os
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'infra/superset'))
sys.path.insert(0,str(ROOT/'infra/trino/security'))
from datasets import DATASETS
from policy import VIEWS
from verification_runtime import query


def main():
    os.environ['WABA_RUNTIME']='kubernetes'
    query('CREATE SCHEMA IF NOT EXISTS iceberg.bi')
    for view,dataset in VIEWS.items():
        query('CREATE OR REPLACE VIEW iceberg.bi.'+view+' SECURITY DEFINER AS '+DATASETS[dataset])
    print('Ten aggregate-only BI views created; no account or individual-claim data exposed.')


if __name__=='__main__':main()
