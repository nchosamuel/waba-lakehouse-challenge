"""Recompute CI medallion stages with real Spark lineage capture enabled."""
import fcntl
from pathlib import Path
import platform
import tempfile
from kubernetes import ROOT,run,kube,secrets,execute,pause_metadata


def main():
    lock=(ROOT/'.tools/kubernetes.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    pause_metadata()
    environment=ROOT/'.env';lines=environment.read_text().splitlines()
    lines=[('WABA_LINEAGE_ENABLED=true' if line.startswith('WABA_LINEAGE_ENABLED=') else line) for line in lines]
    if not any(line.startswith('WABA_LINEAGE_ENABLED=') for line in lines):lines.append('WABA_LINEAGE_ENABLED=true')
    environment.write_text('\n'.join(lines)+'\n');secrets()
    image='waba/spark:20261014';run(['docker','build','-f','infra/k8s/spark.Dockerfile','-t',image,'.'])
    with tempfile.TemporaryDirectory(prefix='waba-lineage-image-') as tmp:
        archive=str(Path(tmp)/'image.tar');arch='arm64' if platform.machine()=='arm64' else 'amd64'
        run(['docker','image','save','--platform','linux/'+arch,'-o',archive,image]);run(['kind','load','image-archive','--name','waba',archive])
    for job in ('raw_to_bronze','bronze_to_silver','silver_to_gold'):
        execute(job,{'COUNTRY_CODES':'CI'})
    print('Successful CI writes recorded automatic raw/Bronze/Silver/Gold lineage in the archive.')


if __name__=='__main__':main()
