"""Reproducible NiFi flow, with runtime-only credentials and bounded queues."""

import json
import os
import time
from pathlib import Path
from nifi_api import NiFi, private_env

api = NiFi()
env = private_env()
root = api.call("/flow/process-groups/root")["processGroupFlow"]["id"]
flow = api.call("/flow/process-groups/" + root)["processGroupFlow"]["flow"]
group = next(
    (
        g
        for g in flow.get("processGroups", [])
        if g["component"]["name"] == "WABA Streaming"
    ),
    None,
)
if not group:
    group = api.call(
        "/process-groups/" + root + "/process-groups",
        "POST",
        {
            "revision": {"version": 0},
            "component": {"name": "WABA Streaming", "position": {"x": 0, "y": 0}},
        },
    )
gid = group["id"]
# Stop this group before configuring. Its state and queued FlowFiles remain intact.
api.call("/flow/process-groups/" + gid, "PUT", {"id": gid, "state": "STOPPED"})
for attempt in range(60):
    status = api.call("/flow/process-groups/" + gid + "/status")["processGroupStatus"][
        "aggregateSnapshot"
    ]
    if status.get("activeThreadCount", 0) == 0:
        break
    time.sleep(0.5)
else:
    raise TimeoutError("NiFi processors did not stop")


def existing(kind):
    if kind == "processors":
        items = api.call("/flow/process-groups/" + gid)["processGroupFlow"]["flow"].get(
            "processors", []
        )
    else:
        items = api.call("/flow/process-groups/" + gid + "/controller-services").get(
            "controllerServices", []
        )
    return {e["component"]["name"]: e for e in items}


def component(name, kind):
    previous = existing(kind).get(name)
    if previous:
        return previous
    typelist = "processorTypes" if kind == "processors" else "controllerServiceTypes"
    path = "processor-types" if kind == "processors" else "controller-service-types"
    dto = next(
        e for e in api.call("/flow/" + path)[typelist] if e["type"].endswith("." + name)
    )
    body = {"name": name, "type": dto["type"], "bundle": dto["bundle"]}
    if kind == "processors":
        body["position"] = {"x": 0, "y": 0}
    return api.call(
        "/process-groups/" + gid + "/" + kind,
        "POST",
        {"revision": {"version": 0}, "component": body},
    )


def wait_service(identifier, wanted):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        entity = api.call("/controller-services/" + identifier)
        if entity["component"]["state"] == wanted:
            return entity
        time.sleep(0.5)
    raise TimeoutError("NiFi service did not reach " + wanted)


services = {
    name: component(name, "controller-services")
    for name in (
        "AWSCredentialsProviderControllerService",
        "CSVReader",
        "JsonRecordSetWriter",
        "Kafka3ConnectionService",
    )
}
service_props = {
    "AWSCredentialsProviderControllerService": {
        "Access Key": env["MINIO_ROOT_USER"],
        "Secret Key": env["MINIO_ROOT_PASSWORD"],
    },
    "CSVReader": {
        "Skip Header Line": "true",
        "Escape Character": '"',
        "schema-access-strategy": "infer-schema",
    },
    "JsonRecordSetWriter": {"output-grouping": "output-array"},
    "Kafka3ConnectionService": {
        "bootstrap.servers": "kafka:9092",
        "security.protocol": "PLAINTEXT",
    },
}
for name, entity in services.items():
    state = api.call("/controller-services/" + entity["id"])
    if state["component"]["state"] == "ENABLED":
        api.call(
            "/controller-services/" + entity["id"] + "/run-status",
            "PUT",
            {"revision": state["revision"], "state": "DISABLED"},
        )
        state = wait_service(entity["id"], "DISABLED")
    api.call(
        "/controller-services/" + entity["id"],
        "PUT",
        {
            "revision": state["revision"],
            "component": {"id": entity["id"], "properties": service_props[name]},
        },
    )
    state = wait_service(entity["id"], "DISABLED")
    api.call(
        "/controller-services/" + entity["id"] + "/run-status",
        "PUT",
        {"revision": state["revision"], "state": "ENABLED"},
    )

    wait_service(entity["id"], "ENABLED")

names = (
    "ListS3",
    "RouteOnAttribute",
    "UpdateAttribute",
    "FetchS3Object",
    "ConvertRecord",
    "SplitJson",
    "JoltTransformJSON",
    "PublishKafka",
    "PutS3Object",
)
processors = {name: component(name, "processors") for name in names}
s3 = {
    "Bucket": "raw-landing",
    "Region": "us-east-1",
    "AWS Credentials Provider service": services[
        "AWSCredentialsProviderControllerService"
    ]["id"],
    "Endpoint Override URL": "http://minio:9000",
}
props = {
    "ListS3": s3,
    "RouteOnAttribute": {
        "events": "${filename:matches('^(CI|SN|ML|BF|GN|TG|BJ|GH)/(bank_transactions|insurance_operations|mobile_money_payments|loan_repayments)/.*[.]csv$')}"
    },
    "UpdateAttribute": {
        "source_file": "${filename}",
        "dataset": "${filename:substringAfter('/'):substringBefore('/')}",
        "ingestion_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", 'UTC')}",
    },
    "FetchS3Object": dict(s3, **{"Object Key": "${source_file}"}),
    "ConvertRecord": {
        "Record Reader": services["CSVReader"]["id"],
        "Record Writer": services["JsonRecordSetWriter"]["id"],
    },
    "SplitJson": {"JsonPath Expression": "$[*]"},
    "JoltTransformJSON": {
        "Jolt Specification": json.dumps(
            [
                {
                    "operation": "default",
                    "spec": {
                        "source_file": "${source_file}",
                        "dataset": "${dataset}",
                        "ingestion_timestamp": "${ingestion_timestamp}",
                    },
                }
            ]
        )
    },
    "PublishKafka": {
        "Kafka Connection Service": services["Kafka3ConnectionService"]["id"],
        "Topic Name": "raw-${dataset:replace('_','-')}",
        "Transactions Enabled": "false",
    },
    "PutS3Object": dict(
        s3,
        **{
            "Bucket": "archive",
            "Object Key": "nifi-failures/${source_file}/${uuid}.json",
        },
    ),
}
terminations = {
    "RouteOnAttribute": ["unmatched"],
    "SplitJson": ["original"],
    "PublishKafka": ["success"],
    "PutS3Object": ["success"],
}
for index, (name, entity) in enumerate(processors.items()):
    state = api.call("/processors/" + entity["id"])
    body = {
        "id": entity["id"],
        "position": {"x": (index % 3) * 450, "y": (index // 3) * 230},
        "config": {
            "properties": props[name],
            "schedulingPeriod": "3 sec" if name == "ListS3" else "0 sec",
            "concurrentlySchedulableTaskCount": 1,
            "autoTerminatedRelationships": terminations.get(name, []),
        },
    }
    api.call(
        "/processors/" + entity["id"],
        "PUT",
        {"revision": state["revision"], "component": body},
    )

connections = api.call("/flow/process-groups/" + gid)["processGroupFlow"]["flow"].get(
    "connections", []
)
existing_edges = {
    (
        e["component"]["source"]["id"],
        e["component"]["destination"]["id"],
        tuple(e["component"]["selectedRelationships"]),
    )
    for e in connections
}
edges = [
    ("ListS3", "RouteOnAttribute", "success"),
    ("RouteOnAttribute", "UpdateAttribute", "events"),
    ("UpdateAttribute", "FetchS3Object", "success"),
    ("FetchS3Object", "ConvertRecord", "success"),
    ("ConvertRecord", "SplitJson", "success"),
    ("SplitJson", "JoltTransformJSON", "split"),
    ("JoltTransformJSON", "PublishKafka", "success"),
    ("FetchS3Object", "FetchS3Object", "failure"),
    ("PublishKafka", "PublishKafka", "failure"),
    ("PutS3Object", "PutS3Object", "failure"),
]
for name in ("ConvertRecord", "SplitJson", "JoltTransformJSON"):
    edges.append((name, "PutS3Object", "failure"))
for source, target, relationship in edges:
    sid, tid = processors[source]["id"], processors[target]["id"]
    if (sid, tid, (relationship,)) in existing_edges:
        continue
    connection = {
        "source": {"id": sid, "groupId": gid, "type": "PROCESSOR"},
        "destination": {"id": tid, "groupId": gid, "type": "PROCESSOR"},
        "selectedRelationships": [relationship],
        "backPressureObjectThreshold": 1000,
        "backPressureDataSizeThreshold": "16 MB",
        "flowFileExpiration": "0 sec",
    }
    api.call(
        "/process-groups/" + gid + "/connections",
        "POST",
        {"revision": {"version": 0}, "component": connection},
    )
time.sleep(2)
errors = {}
for name, entity in processors.items():
    status = api.call("/processors/" + entity["id"])["component"]
    if status.get("validationErrors"):
        errors[name] = status["validationErrors"]
if errors:
    raise RuntimeError(json.dumps(errors))
api.call("/flow/process-groups/" + gid, "PUT", {"id": gid, "state": "RUNNING"})
report = {
    "group_id": gid,
    "processors": list(processors),
    "queue_limit_count": 1000,
    "queue_limit_bytes": "16 MB",
    "credentials": "runtime only",
}
Path(
    os.environ.get("WABA_NIFI_REPORT_PATH", "docs/nifi-flow-verification.json")
).write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report))
