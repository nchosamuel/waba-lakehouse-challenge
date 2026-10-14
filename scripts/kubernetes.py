"""Non-destructive local Kubernetes lifecycle and verified Spark Operator execution.

Every kubectl call targets kind-waba explicitly. Secrets travel on stdin, never
in command arguments or generated manifests. No command deletes PVCs/clusters.
"""

import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tempfile
import platform
import uuid

ROOT = Path(__file__).resolve().parents[1]
KUBECTL = (
    str(ROOT / ".tools/kubectl") if (ROOT / ".tools/kubectl").is_file() else "kubectl"
)
sys.path.insert(0, str(ROOT / "src"))
from waba.kubernetes import spark_application

CONTEXT = "kind-waba"
IMAGE_TAG = "20261014"


def run(args, *, data=None, capture=False, env=None):
    result = subprocess.run(
        args,
        cwd=ROOT,
        input=data,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        check=True,
    )
    return result.stdout if capture else None


def kube(*args, data=None, capture=False):
    return run([KUBECTL, "--context", CONTEXT, *args], data=data, capture=capture)


def private_env():
    return dict(
        line.split("=", 1)
        for line in (ROOT / ".env").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    )


def apply(resource):
    kube("apply", "-f", "-", data=json.dumps(resource))


def secrets():
    values = private_env()
    cloud = {
        "AWS_ACCESS_KEY_ID": values["MINIO_ROOT_USER"],
        "AWS_SECRET_ACCESS_KEY": values["MINIO_ROOT_PASSWORD"],
        "AWS_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "PYTHONPATH": "/app/src",
    }
    cloud["WABA_LINEAGE_ENABLED"] = values.get("WABA_LINEAGE_ENABLED", "false")
    scopes = {
        "ingestion": {
            "MINIO_ROOT_USER": values["MINIO_ROOT_USER"],
            "MINIO_ROOT_PASSWORD": values["MINIO_ROOT_PASSWORD"],
            "SINGLE_USER_CREDENTIALS_USERNAME": values["NIFI_ADMIN_USER"],
            "SINGLE_USER_CREDENTIALS_PASSWORD": values["NIFI_ADMIN_PASSWORD"],
        },
        "processing": dict(
            cloud,
            SPARK_RUNNER_TOKEN=values["SPARK_RUNNER_TOKEN"],
            AIRFLOW_VAR_WABA_SPARK_RUNNER_TOKEN=values["SPARK_RUNNER_TOKEN"],
            AIRFLOW_ADMIN_USER=values["AIRFLOW_ADMIN_USER"],
            AIRFLOW_ADMIN_PASSWORD=values["AIRFLOW_ADMIN_PASSWORD"],
            AIRFLOW__WEBSERVER__SECRET_KEY=values["AIRFLOW_SECRET_KEY"],
        ),
        "serving": dict(
            cloud,
            MINIO_ROOT_USER=values["MINIO_ROOT_USER"],
            MINIO_ROOT_PASSWORD=values["MINIO_ROOT_PASSWORD"],
        ),
    }
    if values.get("WABA_TRINO_SSO_ENABLED") == "true":
        scopes["serving"].update(WABA_TRINO_SSO_ENABLED="true",TRINO_SERVICE_CLIENT="waba-dashboard",TRINO_SERVICE_SECRET=values["KEYCLOAK_DASHBOARD_SERVICE_SECRET"],KEYCLOAK_VALIDATION_SERVICE_SECRET=values["KEYCLOAK_VALIDATION_SERVICE_SECRET"])
    for namespace, data in scopes.items():
        # create/update avoids kubectl's last-applied annotation containing secret data.
        item = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "waba-runtime", "namespace": namespace},
            "type": "Opaque",
            "stringData": data,
        }
        existing = (
            subprocess.run(
                [
                    KUBECTL,
                    "--context",
                    CONTEXT,
                    "get",
                    "secret",
                    "waba-runtime",
                    "-n",
                    namespace,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        )
        if existing:
            current = json.loads(
                kube(
                    "get",
                    "secret",
                    "waba-runtime",
                    "-n",
                    namespace,
                    "-o",
                    "json",
                    capture=True,
                )
            )
            item["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
        kube("replace" if existing else "create", "-f", "-", data=json.dumps(item))

    spark_secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "waba-spark-runtime", "namespace": "processing"},
        "type": "Opaque",
        "stringData": cloud,
    }
    # Server-side apply without a last-applied annotation; generated values remain private.
    kube("apply", "--server-side", "-f", "-", data=json.dumps(spark_secret))

    bi_secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "waba-bi-runtime", "namespace": "serving"},
        "type": "Opaque",
        "stringData": {
            key: values[key]
            for key in (
                "SUPERSET_ADMIN_USER",
                "SUPERSET_ADMIN_PASSWORD",
                "SUPERSET_SECRET_KEY",
                "KEYCLOAK_SUPERSET_SECRET",
                "WABA_SSO_ENABLED",
            )
        },
    }
    if values.get("WABA_TRINO_SSO_ENABLED") == "true":
        bi_secret["stringData"].update(WABA_TRINO_SSO_ENABLED="true",KEYCLOAK_BI_SERVICE_SECRET=values["KEYCLOAK_BI_SERVICE_SECRET"])
    kube("apply", "--server-side", "-f", "-", data=json.dumps(bi_secret))


def pause_observability():
    for namespace,names in [('monitoring',['prometheus','loki','grafana']),('processing',['alloy','observability-exporter'])]:
        resources=json.loads(kube("get","deployments","-n",namespace,"-o","json",capture=True))["items"]
        present=["deployment/"+r["metadata"]["name"] for r in resources if r["metadata"]["name"] in names]
        if present:
            kube("scale",*present,"-n",namespace,"--replicas=0")
            kube("wait","--for=delete","pod","-n",namespace,"-l","app in ("+",".join(names)+")","--timeout=120s")


def pause_metadata():
    exists=subprocess.run([KUBECTL,"--context",CONTEXT,"get","deployment/openmetadata","-n","governance"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
    if not exists:return
    jobs=json.loads(kube("get","jobs","-n","governance","-l","app=metadata-discovery","-o","json",capture=True))["items"]
    if any(j.get("status",{}).get("active",0) for j in jobs):
        raise RuntimeError("Wait for the active metadata discovery job before switching modes")
    kube("patch","cronjob/metadata-discovery","-n","governance","--type=merge","-p",'{"spec":{"suspend":true}}')
    kube("scale","deployment/openmetadata","deployment/metadata-search","deployment/metadata-db","-n","governance","--replicas=0")
    kube("wait","--for=delete","pod","-n","governance","-l","app in (openmetadata,metadata-search,metadata-db)","--field-selector=status.phase!=Succeeded,status.phase!=Failed","--timeout=120s")


def scale_identity(replicas):
    if replicas == 0 and private_env().get("WABA_TRINO_SSO_ENABLED") == "true":
        return  # Secured Trino clients still need the issuer for renewable tokens.
    exists = subprocess.run([KUBECTL,"--context",CONTEXT,"get","deployment/keycloak","-n","governance"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode == 0
    if exists:
        kube("scale","deployment/keycloak","-n","governance","--replicas="+str(replicas))


def wait_job(name, namespace, timeout=600):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        status=json.loads(kube("get","job/"+name,"-n",namespace,"-o","json",capture=True)).get("status",{})
        conditions={c["type"]:c["status"] for c in status.get("conditions",[])}
        if conditions.get("Complete")=="True":
            return
        if conditions.get("Failed")=="True":
            raise RuntimeError("Job failed; inspect preserved logs: "+namespace+"/"+name)
        time.sleep(2)
    raise TimeoutError("Job did not complete: "+namespace+"/"+name)


def wait_deployment(name, namespace):
    kube("rollout", "status", "deployment/" + name, "-n", namespace, "--timeout=300s")


def build():
    run(["docker", "compose", "build", "app", "minio", "airflow"])
    run(
        [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.streaming.yaml",
            "--profile",
            "streaming",
            "build",
            "streaming",
        ]
    )
    for name in ("app", "spark", "airflow"):
        run(
            [
                "docker",
                "build",
                "-f",
                f"infra/k8s/{name}.Dockerfile",
                "-t",
                f"waba/{name}:{IMAGE_TAG}",
                ".",
            ]
        )
    run(["docker", "tag", "waba-lakehouse-minio:latest", "waba/minio:" + IMAGE_TAG])
    for image in (
        "apache/iceberg-rest-fixture:1.9.1",
        "trinodb/trino:475",
        "apache/kafka:3.9.1",
        "apache/nifi:2.4.0",
        "ghcr.io/kubeflow/spark-operator/controller:2.2.0",
    ):
        run(["docker", "pull", image])
    architecture = "arm64" if platform.machine() == "arm64" else "amd64"
    # Docker's containerd store may retain a multi-platform index with missing
    # sibling manifests. Export the node's platform explicitly (kind known issue).
    with tempfile.TemporaryDirectory(prefix="waba-images-") as directory:
        for image in [
            *[
                f"waba/{name}:{IMAGE_TAG}"
                for name in ("app", "spark", "airflow", "minio")
            ],
            "apache/iceberg-rest-fixture:1.9.1",
            "trinodb/trino:475",
            "apache/kafka:3.9.1",
            "apache/nifi:2.4.0",
            "ghcr.io/kubeflow/spark-operator/controller:2.2.0",
        ]:
            archive = str(Path(directory) / "image.tar")
            run(
                [
                    "docker",
                    "image",
                    "save",
                    "--platform",
                    "linux/" + architecture,
                    "--output",
                    archive,
                    image,
                ]
            )
            run(["kind", "load", "image-archive", "--name", "waba", archive])


def job(command, namespace="processing"):
    name = "waba-init-" + uuid.uuid4().hex[:8]
    apply(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": name, "namespace": namespace},
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 1200,
                "template": {
                    "metadata": {"labels": {"app.kubernetes.io/part-of": "waba"}},
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "init",
                                "image": "waba/spark:" + IMAGE_TAG,
                                "command": command,
                                "envFrom": [
                                    {"secretRef": {"name": "waba-spark-runtime"}}
                                ],
                                "resources": {
                                    "requests": {"memory": "128Mi"},
                                    "limits": {"memory": "768Mi"},
                                },
                            }
                        ],
                    },
                },
            },
        }
    )
    try:
        deadline = time.monotonic() + 1200
        while time.monotonic() < deadline:
            status = json.loads(
                kube("get", "job", name, "-n", namespace, "-o", "json", capture=True)
            ).get("status", {})
            if status.get("succeeded"):
                break
            if status.get("failed"):
                raise RuntimeError("Initialization job failed: " + name)
            time.sleep(5)
        else:
            raise TimeoutError("Initialization job timed out: " + name)
    finally:
        kube("logs", "job/" + name, "-n", namespace)


def execute(job_name, parameters=None):
    active = json.loads(
        kube("get", "sparkapplications", "-n", "processing", "-o", "json", capture=True)
    )["items"]
    if any(
        item.get("status", {}).get("applicationState", {}).get("state")
        not in ("COMPLETED", "FAILED")
        for item in active
    ):
        raise RuntimeError(
            "A Spark writer is active; stop it before submitting another job"
        )
    name = "waba-" + job_name.replace("_", "-") + "-" + uuid.uuid4().hex[:8]
    apply(spark_application(name, job_name, parameters))
    deadline = time.monotonic() + 1200
    while time.monotonic() < deadline:
        item = json.loads(
            kube(
                "get",
                "sparkapplication",
                name,
                "-n",
                "processing",
                "-o",
                "json",
                capture=True,
            )
        )
        status = item.get("status", {})
        state = status.get("applicationState", {}).get("state")
        if state in ("COMPLETED", "FAILED"):
            driver = status.get("driverInfo", {}).get("podName")
            if driver:
                try:
                    kube("logs", driver, "-n", "processing")
                except subprocess.CalledProcessError:
                    pass  # Submission may fail before the driver exists.
            if state != "COMPLETED":
                raise RuntimeError(
                    "Spark failed: " + json.dumps(status.get("applicationState"))
                )
            return name
        time.sleep(5)
    raise TimeoutError("Spark application timed out; resource retained: " + name)


def start():
    run([sys.executable, "bootstrap.py"])
    run(
        [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.streaming.yaml",
            "--profile",
            "streaming",
            "stop",
        ]
    )
    clusters = run(["kind", "get", "clusters"], capture=True).splitlines()
    if "waba" not in clusters:
        run(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                "waba",
                "--image",
                "kindest/node:v1.32.3",
                "--wait",
                "180s",
            ]
        )
    build()
    kube("apply", "-f", "infra/k8s/00-namespaces.yaml")
    secrets()
    kube("apply", "-f", "infra/k8s/05-spark-templates.yaml")
    for name in (
        "10-foundation.yaml",
        "20-streaming-services.yaml",
        "30-batch-orchestration.yaml",
        "40-business-intelligence.yaml",
    ):
        kube("apply", "-f", "infra/k8s/" + name)
    if private_env().get("WABA_TRINO_SSO_ENABLED") == "true":
        kube("apply", "-f", "infra/k8s/46-trino-security.yaml")
    # NiFi/Kafka are not needed while seeding the complete reference population.
    kube(
        "scale",
        "deployment/nifi",
        "deployment/kafka",
        "-n",
        "ingestion",
        "--replicas=0",
    )
    wait_deployment("minio", "ingestion")
    job(
        [
            "python3",
            "-c",
            "from waba.publisher import client; s=client(); existing={b['Name'] for b in s.list_buckets()['Buckets']}; [s.create_bucket(Bucket=n) for n in ('raw-landing','lakehouse','archive') if n not in existing]; print('Buckets ready')",
        ]
    )
    for name, ns in (
        ("catalog", "processing"),
        ("trino", "serving"),
        ("app", "serving"),
    ):
        wait_deployment(name, ns)
    run(
        [
            "helm",
            "repo",
            "add",
            "--force-update",
            "spark-operator",
            "https://kubeflow.github.io/spark-operator",
        ]
    )
    run(
        [
            "helm",
            "upgrade",
            "--install",
            "spark-operator",
            "spark-operator/spark-operator",
            "--version",
            "2.2.0",
            "--namespace",
            "processing",
            "--kube-context",
            CONTEXT,
            "-f",
            "infra/k8s/operator-values.yaml",
            "--wait",
            "--timeout",
            "300s",
        ]
    )
    print("Foundation ready. Next: python3 scripts/kubernetes.py seed", flush=True)


def seed():
    for name in ("spark-operator-controller", "spark-operator-webhook"):
        wait_deployment(name, "processing")
    # Files already published with full-v1 metadata are preserved by the publisher.
    job(["python3", "/app/jobs/generate_full_references.py"])
    job(["python3", "/app/jobs/seed_demo.py"])
    for name in (
        "ingest_raw",
        "raw_to_bronze",
        "bronze_to_silver",
        "silver_to_gold",
        "regulatory_report",
        "seed_stream_context",
        "seed_analytics_context",
    ):
        execute(name, {"REPORT_DATE": "2026-02-28"})


def streaming():
    pause_observability()
    pause_metadata()
    applications = json.loads(
        kube("get", "sparkapplications", "-n", "processing", "-o", "json", capture=True)
    )["items"]
    if any(
        a["metadata"]["name"] != "waba-streaming"
        and a.get("status", {}).get("applicationState", {}).get("state")
        not in ("COMPLETED", "FAILED")
        for a in applications
    ):
        raise RuntimeError(
            "Wait for the active batch application before switching to streaming"
        )
    kube("scale", "deployment/superset", "-n", "serving", "--replicas=0")
    scale_identity(0)
    kube(
        "scale",
        "deployment/airflow",
        "deployment/spark-runner",
        "-n",
        "processing",
        "--replicas=0",
    )
    kube(
        "scale",
        "deployment/kafka",
        "deployment/nifi",
        "-n",
        "ingestion",
        "--replicas=1",
    )
    for name in ("kafka", "nifi"):
        wait_deployment(name, "ingestion")
    job(["python3", "/app/jobs/init_kafka.py"])
    # Configure NiFi through a localhost port-forward, preserving TLS isolation.
    forward = subprocess.Popen(
        [
            KUBECTL,
            "--context",
            CONTEXT,
            "port-forward",
            "-n",
            "ingestion",
            "svc/nifi",
            "8443:8443",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        sys.path.insert(0, str(ROOT / "scripts"))
        from nifi_api import NiFi

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                NiFi()
                break
            except Exception:
                time.sleep(3)
        else:
            raise TimeoutError("NiFi authentication was not ready")
        run(
            [sys.executable, "scripts/configure_nifi.py"],
            env=dict(
                os.environ,
                WABA_NIFI_REPORT_PATH="docs/kubernetes-nifi-flow-verification.json",
            ),
        )
    finally:
        forward.terminate()
        forward.wait(timeout=10)
    apply(spark_application("waba-streaming", "streaming_pipeline"))
    print(
        "Streaming submitted. Inspect: kubectl --context kind-waba -n processing get sparkapplications",
        flush=True,
    )


def batch():
    pause_observability()
    pause_metadata()
    kube("scale", "deployment/superset", "-n", "serving", "--replicas=0")
    scale_identity(0)
    kube("scale", "deployment/nifi", "-n", "ingestion", "--replicas=0")
    # Deleting this workload stops execution; its checkpoint PVC is preserved.
    kube(
        "delete",
        "sparkapplication",
        "waba-streaming",
        "-n",
        "processing",
        "--ignore-not-found",
    )
    kube(
        "wait",
        "--for=delete",
        "pod",
        "-l",
        "sparkoperator.k8s.io/app-name=waba-streaming",
        "-n",
        "processing",
        "--timeout=120s",
    )
    kube(
        "scale",
        "deployment/airflow",
        "deployment/spark-runner",
        "-n",
        "processing",
        "--replicas=1",
    )
    for name in ("spark-runner", "airflow"):
        wait_deployment(name, "processing")


def bi():
    pause_observability()
    pause_metadata()
    kube("scale", "deployment/superset", "-n", "serving", "--replicas=0")
    scale_identity(0)
    kube("scale", "deployment/nifi", "-n", "ingestion", "--replicas=0")
    kube(
        "scale",
        "deployment/airflow",
        "deployment/spark-runner",
        "-n",
        "processing",
        "--replicas=0",
    )
    kube(
        "delete",
        "sparkapplication",
        "waba-streaming",
        "-n",
        "processing",
        "--ignore-not-found",
    )
    kube(
        "wait",
        "--for=delete",
        "pod",
        "-l",
        "sparkoperator.k8s.io/app-name=waba-streaming",
        "-n",
        "processing",
        "--timeout=120s",
    )
    execute("seed_analytics_context")
    secrets()
    if private_env().get("WABA_SSO_ENABLED") == "true":
        scale_identity(1)
        wait_deployment("keycloak", "governance")
    run(
        [
            "docker",
            "build",
            "-f",
            "infra/superset/Dockerfile",
            "-t",
            "waba/superset:" + IMAGE_TAG,
            ".",
        ]
    )
    with tempfile.TemporaryDirectory(prefix="waba-bi-image-") as directory:
        archive = str(Path(directory) / "image.tar")
        architecture = "arm64" if platform.machine() == "arm64" else "amd64"
        run(
            [
                "docker",
                "image",
                "save",
                "--platform",
                "linux/" + architecture,
                "-o",
                archive,
                "waba/superset:" + IMAGE_TAG,
            ]
        )
        run(["kind", "load", "image-archive", "--name", "waba", archive])
    kube("apply", "-f", "infra/k8s/40-business-intelligence.yaml")
    kube("scale", "deployment/superset", "-n", "serving", "--replicas=1")
    wait_deployment("superset", "serving")
    print(
        "Superset ready: kubectl --context kind-waba -n serving port-forward svc/superset 8088:8088"
    )


def sso():
    """Deploy identity and enable Superset OIDC without erasing metadata/PVCs."""
    applications = json.loads(kube("get","sparkapplications","-n","processing","-o","json",capture=True))["items"]
    if any(a.get("status",{}).get("applicationState",{}).get("state") not in ("COMPLETED","FAILED") for a in applications):
        raise RuntimeError("Switch to the bi profile before enabling identity; active Spark workloads exist")
    run([sys.executable, "bootstrap.py"])
    values = private_env()
    sys.path.insert(0, str(ROOT / "infra/keycloak"))
    from realm import realm
    for name,namespace,data in (
        ("waba-identity-runtime","governance",{
            "KC_BOOTSTRAP_ADMIN_USERNAME":values["KEYCLOAK_ADMIN_USER"],
            "KC_BOOTSTRAP_ADMIN_PASSWORD":values["KEYCLOAK_ADMIN_PASSWORD"]}),
        ("waba-keycloak-import","governance",{"waba-realm.json":json.dumps(realm(values))}),
    ):
        kube("apply","--server-side","-f","-",data=json.dumps({
            "apiVersion":"v1","kind":"Secret","metadata":{"name":name,"namespace":namespace},
            "type":"Opaque","stringData":data}))
    image = "quay.io/keycloak/keycloak:26.4.0"
    run(["docker","pull",image])
    with tempfile.TemporaryDirectory(prefix="waba-identity-image-") as directory:
        archive = str(Path(directory)/"image.tar")
        architecture = "arm64" if platform.machine() == "arm64" else "amd64"
        run(["docker","image","save","--platform","linux/"+architecture,"-o",archive,image])
        run(["kind","load","image-archive","--name","waba",archive])
    identity = json.loads((ROOT / "infra/k8s/45-identity.yaml").read_text())
    for item in identity["items"]:
        if item["kind"] == "Deployment":
            item["spec"]["replicas"] = 1
    apply(identity)
    wait_deployment("keycloak","governance")
    run(["docker","build","-f","infra/superset/Dockerfile","-t","waba/superset:"+IMAGE_TAG,"."])
    with tempfile.TemporaryDirectory(prefix="waba-sso-image-") as directory:
        archive = str(Path(directory)/"image.tar")
        architecture = "arm64" if platform.machine() == "arm64" else "amd64"
        run(["docker","image","save","--platform","linux/"+architecture,"-o",archive,"waba/superset:"+IMAGE_TAG])
        run(["kind","load","image-archive","--name","waba",archive])
    # Preserve the SQL service identity when reapplying interactive SSO.
    secrets()
    kube("set","env","deployment/superset","-n","serving","WABA_SSO_ENABLED=true")
    kube("rollout","restart","deployment/superset","-n","serving")
    wait_deployment("superset","serving")
    environment = ROOT / ".env"
    lines = environment.read_text().splitlines()
    lines = ["WABA_SSO_ENABLED=true" if line.startswith("WABA_SSO_ENABLED=") else line for line in lines]
    if not any(line.startswith("WABA_SSO_ENABLED=") for line in lines):
        lines.append("WABA_SSO_ENABLED=true")
    environment.write_text("\n".join(lines)+"\n")
    print("OIDC ready. Forward Keycloak 18080:8080 and Superset 8088:8088 on localhost.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "start",
            "build",
            "seed",
            "streaming",
            "batch",
            "bi",
            "sso",
            "status",
            "stop",
        ),
    )
    args = parser.parse_args()
    lock_directory = ROOT / ".tools"
    lock_directory.mkdir(exist_ok=True)
    lock = (lock_directory / "kubernetes.lock").open("w")
    if args.action != "status":
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another Kubernetes lifecycle command is running")
    if args.action == "status":
        for ns in ("ingestion", "processing", "serving", "governance", "monitoring"):
            kube("get", "pods,pvc", "-n", ns)
    elif args.action == "stop":
        pause_observability()
        pause_metadata()
        # Let the operator clean up the streaming driver/executor before stopping it.
        applications = json.loads(
            kube(
                "get",
                "sparkapplications",
                "-n",
                "processing",
                "-l",
                "app.kubernetes.io/part-of=waba",
                "-o",
                "json",
                capture=True,
            )
        )["items"]
        for app in applications:
            if app.get("status", {}).get("applicationState", {}).get("state") not in (
                "COMPLETED",
                "FAILED",
            ):
                kube(
                    "delete",
                    "sparkapplication",
                    app["metadata"]["name"],
                    "-n",
                    "processing",
                )
                kube(
                    "wait",
                    "--for=delete",
                    "pod",
                    "-l",
                    "sparkoperator.k8s.io/app-name=" + app["metadata"]["name"],
                    "-n",
                    "processing",
                    "--timeout=120s",
                )
        kube(
            "wait",
            "--for=delete",
            "pod",
            "-l",
            "sparkoperator.k8s.io/app-name=waba-streaming",
            "-n",
            "processing",
            "--timeout=120s",
        )
        for ns in ("ingestion", "processing", "serving", "governance"):
            kube("scale", "deployment", "--all", "-n", ns, "--replicas=0")
        print("Workloads stopped; cluster and all PVCs preserved.")
    else:
        globals()[args.action]()


if __name__ == "__main__":
    main()
