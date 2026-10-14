"""Render deterministic Kubernetes manifests (JSON is a YAML-compatible format)."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "infra/k8s"
LABEL = {"app.kubernetes.io/part-of": "waba"}


def resource(kind, name, namespace, **body):
    return {
        "apiVersion": (
            "v1"
            if kind
            in (
                "Service",
                "PersistentVolumeClaim",
                "ConfigMap",
                "ServiceAccount",
                "Namespace",
            )
            else "apps/v1"
        ),
        "kind": kind,
        "metadata": {
            "name": name,
            **({"namespace": namespace} if namespace else {}),
            "labels": LABEL,
        },
        **body,
    }


def pvc(name, ns, size):
    return resource(
        "PersistentVolumeClaim",
        name,
        ns,
        spec={
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": size}},
        },
    )


def service(name, ns, ports):
    return resource(
        "Service",
        name,
        ns,
        spec={
            "selector": {"app": name},
            "ports": [
                {"name": "p" + str(port), "port": port, "targetPort": port}
                for port in ports
            ],
        },
    )


def alias(name, ns, target):
    return resource(
        "Service",
        name,
        ns,
        spec={"type": "ExternalName", "externalName": target + ".svc.cluster.local"},
    )


def deployment(
    name,
    ns,
    image,
    port,
    memory,
    *,
    env=None,
    volumes=None,
    mounts=None,
    replicas=1,
    command=None,
    args=None,
    init=None,
    probe=None,
    fs_group=None,
    service_account=None,
):
    container = {
        "name": name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": port}],
        "resources": {
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"memory": memory},
        },
        "env": [{"name": k, "value": str(v)} for k, v in (env or {}).items()],
        "envFrom": [{"secretRef": {"name": "waba-runtime"}}],
        "volumeMounts": mounts or [],
    }
    probe = probe or {"tcpSocket": {"port": port}}
    container.update(
        startupProbe=dict(probe, periodSeconds=10, failureThreshold=30),
        readinessProbe=dict(probe, periodSeconds=15, failureThreshold=3),
        livenessProbe=dict(probe, periodSeconds=30, failureThreshold=5),
    )
    if command:
        container["command"] = command
    if args:
        container["args"] = args
    pod = {
        "containers": [container],
        "volumes": volumes or [],
        "terminationGracePeriodSeconds": 45,
        "automountServiceAccountToken": bool(service_account),
    }
    if init:
        pod["initContainers"] = init
    if fs_group:
        pod["securityContext"] = {"fsGroup": fs_group}
    if service_account:
        pod["serviceAccountName"] = service_account
    return resource(
        "Deployment",
        name,
        ns,
        spec={
            "replicas": replicas,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": {"app": name}},
            "template": {"metadata": {"labels": dict(LABEL, app=name)}, "spec": pod},
        },
    )


def volume(name, claim):
    return {"name": name, "persistentVolumeClaim": {"claimName": claim}}


def mount(name, path, sub=None):
    return {"name": name, "mountPath": path, **({"subPath": sub} if sub else {})}


def config(name):
    return {"name": name, "configMap": {"name": name}}


def write(name, items):
    (OUT / name).write_text(
        json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=2)
        + "\n"
    )


namespaces = [
    resource("Namespace", name, None)
    for name in ("ingestion", "processing", "serving", "governance", "monitoring")
]
write("00-namespaces.yaml", namespaces)

foundation = [
    pvc("minio-data", "ingestion", "8Gi"),
    pvc("catalog-data", "processing", "1Gi"),
]
foundation += [
    deployment(
        "minio",
        "ingestion",
        "waba/minio:20261014",
        9000,
        "768Mi",
        command=["minio"],
        args=["server", "/data", "--console-address", ":9001"],
        volumes=[volume("data", "minio-data")],
        mounts=[mount("data", "/data")],
        probe={"httpGet": {"path": "/minio/health/live", "port": 9000}},
    ),
    service("minio", "ingestion", [9000, 9001]),
]
init = [
    {
        "name": "catalog-owner",
        "image": "waba/spark:20261014",
        "command": ["python3", "-c", "import os; os.chown('/catalog',1000,1000)"],
        "securityContext": {"runAsUser": 0},
        "volumeMounts": [mount("data", "/catalog")],
    }
]
foundation += [
    deployment(
        "catalog",
        "processing",
        "apache/iceberg-rest-fixture:1.9.1",
        8181,
        "768Mi",
        env={
            "AWS_REGION": "us-east-1",
            "CATALOG_WAREHOUSE": "s3://lakehouse/",
            "CATALOG_IO__IMPL": "org.apache.iceberg.aws.s3.S3FileIO",
            "CATALOG_S3_ENDPOINT": "http://minio:9000",
            "CATALOG_S3_PATH__STYLE__ACCESS": "true",
            "CATALOG_URI": "jdbc:sqlite:/catalog/catalog.db",
            "JAVA_TOOL_OPTIONS": "-Xmx384m",
        },
        volumes=[volume("data", "catalog-data")],
        mounts=[mount("data", "/catalog")],
        init=init,
        probe={"httpGet": {"path": "/v1/config", "port": 8181}},
    ),
    service("catalog", "processing", [8181]),
]
for ns in ("processing", "serving"):
    foundation.append(alias("minio", ns, "minio.ingestion"))
foundation += [
    alias("catalog", "serving", "catalog.processing"),
    alias("catalog", "ingestion", "catalog.processing"),
    alias("kafka", "processing", "kafka.ingestion"),
    alias("kafka", "serving", "kafka.ingestion"),
]
trino_data = {
    p.name: p.read_text()
    for p in (ROOT / "infra/trino/streaming-catalog").glob("*.properties")
}
foundation.append(resource("ConfigMap", "trino-catalog", "serving", data=trino_data))
foundation.append(
    resource(
        "ConfigMap",
        "trino-config",
        "serving",
        data={
            p: (ROOT / "infra/trino" / p).read_text()
            for p in ("config.properties", "jvm.config")
        },
    )
)
foundation.append(
    resource(
        "ConfigMap",
        "trino-kafka",
        "serving",
        data={
            p.name: p.read_text() for p in (ROOT / "infra/trino/kafka").glob("*.json")
        },
    )
)
foundation += [
    deployment(
        "trino",
        "serving",
        "trinodb/trino:475",
        8080,
        "2Gi",
        volumes=[
            config("trino-catalog"),
            config("trino-config"),
            config("trino-kafka"),
        ],
        mounts=[
            mount("trino-catalog", "/etc/trino/catalog"),
            mount("trino-config", "/etc/trino/config.properties", "config.properties"),
            mount("trino-config", "/etc/trino/jvm.config", "jvm.config"),
            mount("trino-kafka", "/etc/trino/kafka"),
        ],
        probe={"httpGet": {"path": "/v1/info", "port": 8080}},
    ),
    service("trino", "serving", [8080]),
]
foundation += [
    deployment(
        "app",
        "serving",
        "waba/app:20261014",
        8501,
        "768Mi",
        env={"PYTHONPATH": "/app/src", "S3_ENDPOINT": "http://minio:9000"},
        probe={"httpGet": {"path": "/_stcore/health", "port": 8501}},
    ),
    service("app", "serving", [8501]),
]
for item in foundation:
    if item["kind"] == "Deployment" and item["metadata"]["name"] == "app":
        pod=item["spec"]["template"]["spec"]
        pod["volumes"].append({"name":"trust","secret":{"secretName":"waba-trino-tls","optional":True,"items":[{"key":"ca.crt","path":"ca.crt"}]}})
        pod["containers"][0]["volumeMounts"].append(mount("trust","/etc/waba"))
write("10-foundation.yaml", foundation)

stream = [
    pvc("kafka-data", "ingestion", "2Gi"),
    pvc("nifi-data", "ingestion", "2Gi"),
    pvc("stream-checkpoints", "processing", "1Gi"),
]
kafka_env = {
    "KAFKA_NODE_ID": "1",
    "KAFKA_PROCESS_ROLES": "broker,controller",
    "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@kafka.ingestion.svc.cluster.local:9093",
    "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
    "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka.ingestion.svc.cluster.local:9092",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
    "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
    "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
    "KAFKA_HEAP_OPTS": "-Xms256m -Xmx384m",
    "CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
}
stream += [
    deployment(
        "kafka",
        "ingestion",
        "apache/kafka:3.9.1",
        9092,
        "768Mi",
        env=kafka_env,
        volumes=[volume("data", "kafka-data")],
        mounts=[mount("data", "/var/lib/kafka/data")],
        fs_group=1000,
    ),
    service("kafka", "ingestion", [9092, 9093]),
]
folders = (
    "conf",
    "state",
    "flowfile_repository",
    "content_repository",
    "provenance_repository",
)
init = [
    {
        "name": "nifi-state-init",
        "image": "apache/nifi:2.4.0",
        "command": [
            "bash",
            "-ec",
            "mkdir -p "
            + " ".join("/persist/" + f for f in folders)
            + "; cp -an /opt/nifi/nifi-current/conf/. /persist/conf/",
        ],
        "volumeMounts": [mount("data", "/persist")],
    }
]
stream += [
    deployment(
        "nifi",
        "ingestion",
        "apache/nifi:2.4.0",
        8443,
        "1536Mi",
        env={
            "NIFI_WEB_HTTPS_PORT": "8443",
            "NIFI_WEB_HTTPS_HOST": "0.0.0.0",
            "NIFI_WEB_PROXY_HOST": "localhost:8443,127.0.0.1:8443,nifi.ingestion.svc.cluster.local:8443",
            "NIFI_JVM_HEAP_INIT": "512m",
            "NIFI_JVM_HEAP_MAX": "768m",
        },
        volumes=[volume("data", "nifi-data")],
        mounts=[mount("data", "/opt/nifi/nifi-current/" + f, f) for f in folders],
        fs_group=1000,
        init=init,
    ),
    service("nifi", "ingestion", [8443]),
]
# KRaft must reach its controller before the broker's readiness can turn green.
for item in stream:
    if item["kind"] == "Service" and item["metadata"]["name"] == "kafka":
        item["spec"]["publishNotReadyAddresses"] = True
write("20-streaming-services.yaml", stream)

batch = [
    pvc("airflow-state", "processing", "1Gi"),
    pvc("airflow-logs", "processing", "1Gi"),
    resource("ServiceAccount", "waba-runner", "processing"),
]
role = {
    "apiVersion": "rbac.authorization.k8s.io/v1",
    "kind": "Role",
    "metadata": {"name": "waba-runner", "namespace": "processing"},
    "rules": [
        {
            "apiGroups": ["sparkoperator.k8s.io"],
            "resources": ["sparkapplications"],
            "verbs": ["get", "list", "create"],
        }
    ],
}
binding = {
    "apiVersion": "rbac.authorization.k8s.io/v1",
    "kind": "RoleBinding",
    "metadata": {"name": "waba-runner", "namespace": "processing"},
    "subjects": [
        {"kind": "ServiceAccount", "name": "waba-runner", "namespace": "processing"}
    ],
    "roleRef": {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "Role",
        "name": "waba-runner",
    },
}
batch += [
    role,
    binding,
    deployment(
        "spark-runner",
        "processing",
        "waba/spark:20261014",
        8090,
        "256Mi",
        replicas=0,
        command=["python3", "/app/jobs/kubernetes_runner.py"],
        service_account="waba-runner",
        probe={"httpGet": {"path": "/health", "port": 8090}},
    ),
    service("spark-runner", "processing", [8090]),
]
airflow_env = {
    "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
    "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    "AIRFLOW__CORE__PARALLELISM": "1",
    "AIRFLOW__SCHEDULER__PARSING_PROCESSES": "1",
    "AIRFLOW__CORE__DEFAULT_TIMEZONE": "utc",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:////opt/airflow/state/airflow.db",
    "AIRFLOW__WEBSERVER__WORKERS": "1",
    "AIRFLOW__WEBSERVER__EXPOSE_CONFIG": "false",
    "AIRFLOW__API__AUTH_BACKENDS": "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session",
    "AIRFLOW_VAR_WABA_SPARK_RUNNER_URL": "http://spark-runner:8090",
    "PYTHONPATH": "/app/src",
}
batch += [
    deployment(
        "airflow",
        "processing",
        "waba/airflow:20261014",
        8081,
        "2Gi",
        replicas=0,
        env=airflow_env,
        volumes=[volume("state", "airflow-state"), volume("logs", "airflow-logs")],
        mounts=[
            mount("state", "/opt/airflow/state"),
            mount("logs", "/opt/airflow/logs"),
        ],
        fs_group=0,
        probe={"httpGet": {"path": "/health", "port": 8081}},
    ),
    service("airflow", "processing", [8081]),
]
write("30-batch-orchestration.yaml", batch)

# Hosts are examples; local tests use authenticated port-forwards without ingress.
ingresses = []
for ns, mappings in {
    "serving": [("app", 8501), ("trino", 8080), ("superset", 8088)],
    "processing": [("airflow", 8081)],
    "ingestion": [("nifi", 8443)],
    "monitoring": [("grafana", 3000)],
    "governance": [("openmetadata", 8585)],
}.items():
    for name, port in mappings:
        ingresses.append(
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress",
                "metadata": {
                    "name": name,
                    "namespace": ns,
                    "annotations": (
                        {"nginx.ingress.kubernetes.io/backend-protocol": "HTTPS"}
                        if name == "nifi"
                        else {}
                    ),
                },
                "spec": {
                    "ingressClassName": "nginx",
                    "rules": [
                        {
                            "host": name + ".waba.local",
                            "http": {
                                "paths": [
                                    {
                                        "path": "/",
                                        "pathType": "Prefix",
                                        "backend": {
                                            "service": {
                                                "name": name,
                                                "port": {"number": port},
                                            }
                                        },
                                    }
                                ]
                            },
                        }
                    ],
                },
            }
        )
write("80-ingress.yaml", ingresses)

bi = deployment(
    "superset",
    "serving",
    "waba/superset:20261014",
    8088,
    "1024Mi",
    replicas=0,
    volumes=[volume("state", "superset-state")],
    mounts=[mount("state", "/app/superset_home")],
    fs_group=1000,
    probe={"httpGet": {"path": "/health", "port": 8088}},
)
bi["spec"]["template"]["spec"]["volumes"].append({"name":"trust","secret":{"secretName":"waba-trino-tls","optional":True,"items":[{"key":"ca.crt","path":"ca.crt"}]}})
bi["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].append(mount("trust","/etc/waba"))
bi["spec"]["template"]["spec"]["containers"][0]["envFrom"] = [
    {"secretRef": {"name": "waba-bi-runtime"}}
]
write(
    "40-business-intelligence.yaml",
    [
        pvc("superset-state", "serving", "1Gi"),
        bi,
        service("superset", "serving", [8088]),
    ],
)

write(
    "05-spark-templates.yaml",
    [
        resource(
            "ConfigMap",
            "waba-spark-templates",
            "processing",
            data={
                name: (OUT / name).read_text()
                for name in ("spark-driver-pod.yaml", "stream-driver-pod.yaml")
            },
        )
    ],
)

identity = deployment(
    "keycloak", "governance", "quay.io/keycloak/keycloak:26.4.0", 8080, "1Gi",
    replicas=0, args=["start-dev", "--import-realm"],
    env={"KC_HOSTNAME":"http://localhost:18080", "KC_HOSTNAME_BACKCHANNEL_DYNAMIC":"true",
         "KC_HEALTH_ENABLED":"true", "JAVA_OPTS_KC_HEAP":"-Xms128m -Xmx512m"},
    volumes=[volume("state","keycloak-state"), {"name":"realm", "secret":{"secretName":"waba-keycloak-import"}}],
    mounts=[mount("state","/opt/keycloak/data"),mount("realm","/opt/keycloak/data/import")],
    fs_group=1000,
    probe={"httpGet":{"path":"/health/ready","port":9000}},
)
identity["spec"]["template"]["spec"]["containers"][0]["envFrom"] = [{"secretRef":{"name":"waba-identity-runtime"}}]
write("45-identity.yaml", [pvc("keycloak-state","governance","1Gi"),identity,service("keycloak","governance",[8080])])

# Explicit secure profile overlay; keep the original Compose/local profile separate.
import copy
secure = copy.deepcopy(next(r for r in foundation if r["kind"] == "Deployment" and r["metadata"]["name"] == "trino"))
pod = secure["spec"]["template"]["spec"]
container = pod["containers"][0]
container["envFrom"] = [{"secretRef":{"name":"waba-trino-runtime"}}]
container["ports"].append({"containerPort":8443})
for v in pod["volumes"]:
    if v["name"] == "trino-config":
        v["configMap"]["name"] = "trino-secure-config"
pod["volumes"] += [config("trino-security"), {"name":"tls","secret":{"secretName":"waba-trino-tls"}}]
container["volumeMounts"] += [mount("trino-security","/etc/trino/security"),mount("trino-security","/etc/trino/access-control.properties","access-control.properties"),mount("tls","/etc/trino/tls")]
write("46-trino-security.yaml", [
    resource("ConfigMap","trino-secure-config","serving",data={"config.properties":(ROOT/"infra/trino/security/config.properties").read_text(),"jvm.config":(ROOT/"infra/trino/jvm.config").read_text()}),
    resource("ConfigMap","trino-security","serving",data={n:(ROOT/"infra/trino/security"/n).read_text() for n in ("access-control.properties","rules.json")}),
    secure, service("trino","serving",[8080,8443])])

metadata_env = {
    "DB_DRIVER_CLASS":"org.postgresql.Driver","DB_SCHEME":"postgresql","DB_HOST":"metadata-db","DB_PORT":"5432","DB_PARAMS":"","OM_DATABASE":"openmetadata_db","DB_USER":"openmetadata",
    "DB_CONNECTION_POOL_INITIAL_SIZE":"2","DB_CONNECTION_POOL_MIN_SIZE":"2","DB_CONNECTION_POOL_MAX_SIZE":"8",
    "ELASTICSEARCH_HOST":"metadata-search","ELASTICSEARCH_PORT":"9200","SEARCH_TYPE":"opensearch","ELASTICSEARCH_SCHEME":"http",
    "AUTHENTICATION_PROVIDER":"basic","AUTHENTICATION_ENABLE_SELF_SIGNUP":"false","AUTHORIZER_ADMIN_PRINCIPALS":"[waba-catalog-admin]","AUTHORIZER_PRINCIPAL_DOMAIN":"example.invalid",
    "AUTHENTICATION_PUBLIC_KEYS":"[http://openmetadata:8585/api/v1/system/config/jwks]",
    "RSA_PUBLIC_KEY_FILE_PATH":"/keys/public_key.der","RSA_PRIVATE_KEY_FILE_PATH":"/keys/private_key.der","JWT_ISSUER":"waba-catalog","JWT_KEY_ID":"waba-local-catalog",
    "PIPELINE_SERVICE_CLIENT_ENABLED":"false","PIPELINE_SERVICE_CLIENT_CLASS_NAME":"org.openmetadata.service.clients.pipeline.noop.NoopClient",
    "OPENMETADATA_HEAP_OPTS":"-Xms256m -Xmx768m","SERVER_HOST_API_URL":"http://openmetadata:8585/api",
}
metadata_db = deployment("metadata-db","governance","postgres:15.12",5432,"512Mi",replicas=0,
    env={"POSTGRES_USER":"openmetadata","POSTGRES_DB":"openmetadata_db","PGDATA":"/var/lib/postgresql/data/pgdata"},
    volumes=[volume("state","metadata-db")],mounts=[mount("state","/var/lib/postgresql/data")],fs_group=999,
    probe={"exec":{"command":["pg_isready","-U","openmetadata","-d","openmetadata_db"]}})
metadata_search = deployment("metadata-search","governance","opensearchproject/opensearch:2.19.1",9200,"1Gi",replicas=0,
    env={"discovery.type":"single-node","DISABLE_INSTALL_DEMO_CONFIG":"true","DISABLE_SECURITY_PLUGIN":"true","OPENSEARCH_JAVA_OPTS":"-Xms256m -Xmx512m","bootstrap.memory_lock":"false"},
    volumes=[volume("state","metadata-search")],mounts=[mount("state","/usr/share/opensearch/data")],fs_group=1000,
    probe={"httpGet":{"path":"/_cluster/health","port":9200}})
metadata_server = deployment("openmetadata","governance","docker.getcollate.io/openmetadata/server:1.9.0",8585,"1536Mi",replicas=0,
    env=metadata_env,volumes=[{"name":"keys","secret":{"secretName":"waba-metadata-keys"}}],mounts=[mount("keys","/keys")],
    probe={"httpGet":{"path":"/api/v1/system/version","port":8585}})
for item in (metadata_db,metadata_search,metadata_server):
    item["spec"]["template"]["spec"]["containers"][0]["envFrom"]=[{"secretRef":{"name":"waba-metadata-runtime"}}]
write("50-openmetadata.yaml",[pvc("metadata-db","governance","2Gi"),pvc("metadata-search","governance","2Gi"),metadata_db,service("metadata-db","governance",[5432]),metadata_search,service("metadata-search","governance",[9200]),metadata_server,service("openmetadata","governance",[8585,8586])])

write("55-metadata-ingestion.yaml",[{"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"metadata-discovery","namespace":"governance"},"spec":{"schedule":"15 2 * * *","timeZone":"Etc/UTC","concurrencyPolicy":"Forbid","successfulJobsHistoryLimit":2,"failedJobsHistoryLimit":2,"jobTemplate":{"spec":{"backoffLimit":1,"activeDeadlineSeconds":600,"template":{"metadata":{"labels":{"app":"metadata-discovery"}},"spec":{"restartPolicy":"Never","automountServiceAccountToken":False,"containers":[{"name":"metadata-discovery","image":"waba/metadata-ingestion:20261014","imagePullPolicy":"IfNotPresent","envFrom":[{"secretRef":{"name":"waba-metadata-pipeline"}}],"resources":{"requests":{"memory":"128Mi","cpu":"100m"},"limits":{"memory":"512Mi"}},"volumeMounts":[{"name":"trust","mountPath":"/etc/waba","readOnly":True}]}],"volumes":[{"name":"trust","secret":{"secretName":"waba-metadata-trust"}}]}}}}}}])
