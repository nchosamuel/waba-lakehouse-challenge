"""Four parameterized DAGs with explicit chaining, retries and failure evidence."""

import json
import urllib.request
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from waba.pipeline_config import scope
from waba.generator import COUNTRIES


def failure_alert(context):
    # Durable Airflow task logs. Optional authenticated webhook in an Airflow Variable.
    event = {
        "event": "pipeline_failure",
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "run_id": context["run_id"],
    }
    print(json.dumps(event), flush=True)
    webhook = Variable.get("waba_alert_webhook", default_var="")
    if webhook:
        request = urllib.request.Request(
            webhook,
            data=json.dumps(event).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(request, timeout=15).close()


def run_job(job, **context):
    config = dict(context["params"])
    config.update(context["dag_run"].conf or {})
    scope(config)  # Validate all values before crossing the job submission boundary.
    if job == "regulatory_report" and not config.get("report_date"):
        # Daily data interval starts at 00:30 of D, ends 00:30 of D+1.
        config["report_date"] = context["data_interval_start"].date().isoformat()
    endpoint = Variable.get(
        "waba_spark_runner_url", default_var="http://spark-runner:8090"
    )
    token = Variable.get("waba_spark_runner_token")
    request = urllib.request.Request(
        endpoint + "/run",
        data=json.dumps({"job": job, "config": config}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=1260) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        # Preserve Spark's error tail in task logs and let Airflow perform its retries.
        print(exc.read().decode(), flush=True)
        raise
    print(result["log_tail"], flush=True)
    return {"job": job, "exit_code": result["exit_code"]}


params = {
    "country_codes": Param(
        list(COUNTRIES),
        type="array",
        minItems=1,
        uniqueItems=True,
        items={"type": "string", "enum": list(COUNTRIES)},
    ),
    "date_from": Param(None, type=["null", "string"], format="date"),
    "date_to": Param(None, type=["null", "string"], format="date"),
    "report_date": Param(None, type=["null", "string"], format="date"),
}
defaults = {
    "owner": "waba-data",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": failure_alert,
    "execution_timeout": timedelta(minutes=22),
}


def dag(name, schedule):
    return DAG(
        name,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        default_args=defaults,
        params=params,
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
        tags=["waba", "batch", "level-2"],
    )


def job(name):
    return PythonOperator(
        task_id=name,
        python_callable=run_job,
        op_kwargs={"job": name},
        pool="spark_local",
    )


with dag("dag_ingest_raw", "*/15 * * * *") as dag_ingest_raw:
    ingest = job("ingest_raw")
    bronze = job("raw_to_bronze")
    next_stage = TriggerDagRunOperator(
        task_id="launch_silver",
        trigger_dag_id="dag_bronze_to_silver",
        trigger_run_id="ingestion__{{ run_id }}",
        conf="{{ dag_run.conf }}",
        wait_for_completion=False,
        skip_when_already_exists=True,
    )
    ingest >> bronze >> next_stage

with dag("dag_bronze_to_silver", None) as dag_bronze_to_silver:
    silver = job("bronze_to_silver")
    next_stage = TriggerDagRunOperator(
        task_id="launch_gold",
        trigger_dag_id="dag_silver_to_gold",
        trigger_run_id="silver__{{ run_id }}",
        conf="{{ dag_run.conf }}",
        wait_for_completion=False,
        skip_when_already_exists=True,
    )
    silver >> next_stage

with dag("dag_silver_to_gold", None) as dag_silver_to_gold:
    gold = job("silver_to_gold")

with dag("dag_regulatory_report", "30 0 * * *") as dag_regulatory_report:
    report = job("regulatory_report")
