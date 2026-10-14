"""Checkpoint commit and regulatory deadline semantics; no running services needed."""
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock
import sys

spec=importlib.util.spec_from_file_location('observer',Path(__file__).resolve().parents[1]/'scripts/observability_exporter.py')
observer=importlib.util.module_from_spec(spec);spec.loader.exec_module(observer)


class ObservabilityTest(unittest.TestCase):
    def test_paused_kafka_never_starts_network_client(self):
        kafka=MagicMock()
        with patch.object(observer,'kube',return_value={'spec':{'replicas':0}}), patch.dict(sys.modules,{'kafka':kafka}):
            with self.assertRaisesRegex(RuntimeError,'deliberately paused'):
                observer.kafka_metrics(observer.Metrics())
        kafka.KafkaConsumer.assert_not_called()

    def test_stalled_source_does_not_freeze_other_metrics(self):
        import threading
        import time
        blocked = threading.Event()
        def stalled(m):
            blocked.wait(5)
        def healthy(m):
            m.add('healthy_metric',42)
        try:
            started = time.monotonic()
            body = observer.collect({'test_stall':stalled,'test_healthy':healthy}, timeout_seconds=0.05, demo={})
            self.assertLess(time.monotonic()-started,1)
            self.assertIn('healthy_metric 42.0',body)
            self.assertIn('waba_telemetry_available{source="test_stall"} 0.0',body)
            future = observer.SOURCE_FUTURES['test_stall']
            observer.collect({'test_stall':stalled}, timeout_seconds=0.01, demo={})
            self.assertIs(observer.SOURCE_FUTURES['test_stall'],future)
        finally:
            blocked.set()

    def test_ignore_uncommitted_offsets(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp);(path/'commits').mkdir();(path/'offsets').mkdir()
            (path/'commits/2').write_text('v1\n{}')
            (path/'offsets/2').write_text('v1\n{}\n{"silver-bank-transactions":{"0":20}}\n')
            (path/'offsets/3').write_text('v1\n{}\n{"silver-bank-transactions":{"0":999}}\n')
            self.assertEqual(observer.committed_offsets(tmp),{('silver-bank-transactions',0):20})
    def test_no_commit_is_not_processed(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(observer.committed_offsets(tmp),{})
    def test_deadline_boundary_and_success(self):
        before=datetime(2026,10,14,5,59,59,tzinfo=timezone.utc)
        due=datetime(2026,10,14,6,0,0,tzinfo=timezone.utc)
        self.assertEqual(observer.regulatory_missed(before,[]),0)
        self.assertEqual(observer.regulatory_missed(due,[]),1)
        self.assertEqual(observer.regulatory_missed(due,[('failed',)]),1)
        self.assertEqual(observer.regulatory_missed(due,[('running',)]),1)
        self.assertEqual(observer.regulatory_missed(due,[('success',)]),0)
    def test_deadline_uses_utc(self):
        self.assertEqual(observer.regulatory_missed(datetime.fromisoformat('2026-10-14T14:00:00+08:00'),[]),1)
    def test_prometheus_label_escaping(self):
        metrics=observer.Metrics();metrics.add('test',1,label='quote" newline\n')
        self.assertIn('label="quote\\" newline\\n"',metrics.render())
