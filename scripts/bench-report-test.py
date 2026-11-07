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
