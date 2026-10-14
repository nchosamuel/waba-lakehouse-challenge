"""Security and durability boundaries of the native Spark submission contract."""

import unittest
from waba.kubernetes import spark_application


class KubernetesContractTests(unittest.TestCase):
    def test_job_allowlist_rejects_path_traversal(self):
        with self.assertRaises(ValueError):
            spark_application("bad", "../../secret")
        with self.assertRaises(ValueError):
            spark_application(
                "bad", "ingest_raw", {"AWS_SECRET_ACCESS_KEY": "override"}
            )

    def test_secrets_referenced_for_both_driver_and_executor(self):
        spec = spark_application("batch", "ingest_raw", {"COUNTRY_CODES": "CI"})["spec"]
        self.assertEqual(spec["mode"], "cluster")
        self.assertEqual(spec["driver"]["serviceAccount"], "waba-spark")
        for role in ("driver", "executor"):
            self.assertEqual(
                spec[role]["envFrom"], [{"secretRef": {"name": "waba-spark-runtime"}}]
            )
        self.assertNotIn("volumes", spec)
        self.assertEqual(spec["restartPolicy"]["type"], "Never")

    def test_stream_restarts_with_same_durable_checkpoint(self):
        spec = spark_application("stream", "streaming_pipeline")["spec"]
        self.assertEqual(
            spec["volumes"][0]["persistentVolumeClaim"]["claimName"],
            "stream-checkpoints",
        )
        self.assertEqual(spec["driver"]["volumeMounts"][0]["mountPath"], "/checkpoints")
        self.assertEqual(
            spec["executor"]["volumeMounts"], spec["driver"]["volumeMounts"]
        )
        self.assertEqual(spec["restartPolicy"]["type"], "OnFailure")
        self.assertIn(
            "stream-driver-pod.yaml",
            spec["sparkConf"]["spark.kubernetes.driver.podTemplateFile"],
        )
