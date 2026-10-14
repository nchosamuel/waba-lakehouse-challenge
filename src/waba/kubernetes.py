"""Spark Operator resource contract shared by CLI and Airflow submission adapter."""

IMAGE = "waba/spark:20261014"
SUPPORTED_JOBS = {
    "ingest_raw",
    "raw_to_bronze",
    "bronze_to_silver",
    "silver_to_gold",
    "regulatory_report",
    "seed_stream_context",
    "seed_analytics_context",
    "seed_lambda_membership",
    "streaming_pipeline",
}


def spark_application(name, job, parameters=None):
    if job not in SUPPORTED_JOBS:
        raise ValueError("Unknown Spark job")
    if any(
        key not in {"COUNTRY_CODES", "DATE_FROM", "DATE_TO", "REPORT_DATE"}
        or not isinstance(value, str)
        for key, value in (parameters or {}).items()
    ):
        raise ValueError("Unsupported Spark parameters")
    streaming = job == "streaming_pipeline"
    env = [{"name": key, "value": value} for key, value in (parameters or {}).items()]
    env.append({"name": "WABA_SHARED_STAGING", "value": "true"})
    container = {
        "cores": 1,
        "memory": "1g",
        "memoryOverhead": "1g",
        "env": env,
        "envFrom": [{"secretRef": {"name": "waba-spark-runtime"}}],
    }
    driver = dict(container, serviceAccount="waba-spark", memoryOverhead="1g")
    if streaming:
        driver["volumeMounts"] = [{"name": "checkpoints", "mountPath": "/checkpoints"}]
    spec = {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": f"local:///app/jobs/{job}.py",
        "sparkVersion": "3.5.5",
        "driver": driver,
        "executor": dict(
            container,
            instances=1,
            **(
                {"volumeMounts": [{"name": "checkpoints", "mountPath": "/checkpoints"}]}
                if streaming
                else {}
            ),
        ),
        "restartPolicy": {"type": "Never"},
        "sparkConf": {
            "spark.kubernetes.driver.podTemplateFile": "/app/infra/k8s/"
            + ("stream-driver-pod.yaml" if streaming else "spark-driver-pod.yaml"),
            "spark.kubernetes.executor.deleteOnTermination": "true",
            "spark.ui.prometheus.enabled": "true",
            "spark.kubernetes.allocation.driver.readinessTimeout": "30s",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.ui.retainedExecutions": "25",
            "spark.ui.retainedJobs": "25",
            "spark.ui.retainedStages": "50",
        },
    }
    if streaming:
        spec["restartPolicy"] = {
            "type": "OnFailure",
            "onFailureRetries": 3,
            "onFailureRetryInterval": 30,
            "onSubmissionFailureRetries": 3,
            "onSubmissionFailureRetryInterval": 20,
        }
        spec["volumes"] = [
            {
                "name": "checkpoints",
                "persistentVolumeClaim": {"claimName": "stream-checkpoints"},
            }
        ]
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": "processing",
            "labels": {"app.kubernetes.io/part-of": "waba"},
        },
        "spec": spec,
    }
