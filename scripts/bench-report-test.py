#!/usr/bin/env python3
"""Regression checks for repeated-result aggregation and comparison direction."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('bench_report', Path(__file__).with_name('bench-report.py'))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class ReportTests(unittest.TestCase):
    def test_repeated_topic_runs_group_without_truncating_median(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for run, rate in enumerate([10, 11], 1):
                path = root / f'run-{run}/topic/run-1/topic-publish-replicate_once-128B.json'
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(dict(ack_mode=1, published=100,
                                               throughput_msgs_per_sec=rate)))
            result = report.load_results(root)
            self.assertEqual(len(result), 1)
            data = next(iter(result.values()))
            self.assertEqual(data['throughput_msgs_per_sec'], 10.5)
            self.assertEqual(data['runs'], 2)
            self.assertIn('Repeat variability', report.render_report(root))
            self.assertIn('replicate_once', report.render_report(root))

    def test_kv_counts_and_percentiles(self):
        text = report.section_kv({'kv/kv-local-serial.json': dict(
            scenario='kv-local-serial', entries=1001, published=1000, visible=1000,
            failed=0, get_p50_ns=123, get_p99_ns=456, read_zig_allocs=0,
            read_engine_allocs=0)})
        self.assertIn('1,001 | 1,000 | 1,000 | 0 | 123 | 456', text)
        self.assertIn('Read LMDB allocs', text)
        self.assertFalse(report.METRICS['get_p99_ns'])

    def test_kv_scale_and_engine_alloc_tables(self):
        text = report.section_kv({
            'kv/kv-scale-1000000.json': dict(scenario='native-reader-scale', entries=1000000, failed=0,
                                             get_p50_ns=781, view_p50_ns=642, reader_threads=4,
                                             parallel_operations=80000),
            'kv/kv-engine-alloc.json': dict(scenario='kv-engine-alloc', transactions=1000,
                                            engine_allocs=999, engine_allocs_per_txn=0.999)})
        self.assertNotIn('KV bounded same-host serial workload', text)
        self.assertIn('| 1,000,000 | 0 | 781 |', text)
        self.assertIn('| 642 |', text)
        self.assertIn('| 1,000 | 999 | 1.0 |', text)

    def test_comparison_direction(self):
        with tempfile.TemporaryDirectory() as tmp:
            old, new = Path(tmp)/'old', Path(tmp)/'new'
            old.mkdir()
            new.mkdir()
            (old/'test.json').write_text(json.dumps(dict(throughput_msgs_per_sec=100, latency_p99_ns=100)))
            (new/'test.json').write_text(json.dumps(dict(throughput_msgs_per_sec=80, latency_p99_ns=80)))
            text = report.render_comparison(new, old, 10)
            self.assertIn('1 metric(s) worse', text)
            self.assertIn('better', text)

    def test_aeron_delivery_counts_use_emitted_field_names(self):
        section = report.section_plain_aeron({'plain-aeron-remote-transit-128B.json': dict(sent=110000, total_received=110000)})
        self.assertIn('110,000 | 110,000', section)

    def test_mean_is_not_labeled_as_percentile(self):
        section = report.section_topic_cross({'topic-cross-128B.json': dict(received=10, lat_avg_ns=123)})
        self.assertNotIn('123', section)
        self.assertIn('n/a', section)


if __name__ == '__main__':
    unittest.main()
