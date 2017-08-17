#!/usr/bin/env python
import sample_responses
import sample_html
import spark_plugin
from spark_plugin import SparkProcessPlugin, SparkApplicationPlugin, \
                         MetricRecord
from unittest import TestCase
from mock import Mock, MagicMock


def mock_request_response(url, path, *args, **kwargs):
    if path == spark_plugin.MASTER_PATH:
        return sample_responses.master_metrics
    elif path == spark_plugin.WORKER_PATH:
        return sample_responses.worker_metrics
    elif path == spark_plugin.STANDALONE_STATE_PATH:
        return sample_responses.master_standalone_json
    elif path == spark_plugin.STANDALONE_APP_PATH:
        return sample_html.MockHTMLResponse()
    elif path == spark_plugin.MESOS_MASTER_APP_PATH:
        return sample_responses.master_mesos_json
    elif path == spark_plugin.APPS_ENDPOINT and 'jobs' in args:
        return sample_responses.running_jobs
    elif path == spark_plugin.APPS_ENDPOINT and 'stages' in args:
        return sample_responses.active_stages
    elif path == spark_plugin.APPS_ENDPOINT and 'executors' in args:
        return sample_responses.executors
    elif path == spark_plugin.APPS_ENDPOINT and 'streaming/statistics' in args:
        return sample_responses.streaming_stats


def get_mock_spark_agent():
    mock_spark_agent = Mock()
    mock_spark_agent.request_metrics = MagicMock(
                                        side_effect=mock_request_response)
    mock_spark_agent.rest_request = MagicMock(
                                        side_effect=mock_request_response)
    return mock_spark_agent


class MockMetricSink(object):

    def __init__(self):
        self.captured_records = []

    def emit(self, metric_record):
        self.captured_records.append(metric_record)


class SparkProcessTest(TestCase):

    def setUp(self):
        self.plugin = SparkProcessPlugin()
        self.plugin.spark_agent = get_mock_spark_agent()
        self.mock_sink = MockMetricSink()
        self.plugin.metric_sink = self.mock_sink

    def test_configure_exceptions(self):
        config_map = {"Dimensions": "foo=bar,hello=world"}
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map['MasterPort'] = "Non-integer port"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map['MasterPort'] = "8080.0"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        del config_map['MasterPort']

        config_map['WorkerPort'] = "Non-integer port"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map['WorkerPort'] = "8080.0"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

    def test_configure_worker_port_none(self):
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "MetricsURL": "http://host",
                      "MasterPort": 8080}

        expected_global_dim = {"foo": "bar", "hello": "world"}
        expected_metric_address = "http://host"
        expected_master_port = "8080"

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.metric_address, expected_metric_address)
        self.assertEqual(self.plugin.master_port, expected_master_port)
        self.assertIsNone(self.plugin.worker_port)

    def test_configure_master_port_none(self):
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "MetricsURL": "http://host",
                      "WorkerPort": 8081}

        expected_global_dim = {"foo": "bar", "hello": "world"}
        expected_metric_address = "http://host"
        expected_worker_port = "8081"

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.metric_address, expected_metric_address)
        self.assertEqual(self.plugin.worker_port, expected_worker_port)
        self.assertIsNone(self.plugin.master_port)

    def test_configure_all(self):
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "Dimension": "key=value",
                      "MetricsURL": "http://host",
                      "MasterPort": 8080,
                      "WorkerPort": 8081}

        expected_global_dim = {"foo": "bar", "hello": "world", "key": "value"}
        expected_metric_address = "http://host"
        expected_master_port = "8080"
        expected_worker_port = "8081"

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.metric_address, expected_metric_address)
        self.assertEqual(self.plugin.worker_port, expected_worker_port)
        self.assertEqual(self.plugin.master_port, expected_master_port)

    def test_get_metrics(self):
        resp = {}
        process = ""

        expected_resp = []
        actual_resp = self.plugin.get_metrics(resp, process)
        self.assertListEqual(actual_resp, expected_resp)

        resp = {
            "gauges": {
                "fail_metric_name_1": {
                    "value": 0
                },
                "master.workers": {
                    "value": 100
                }
            },
            "counters": {
                "fail_metric_name_2": {
                    "count": 0
                },
                "HiveExternalCatalog.fileCacheHits": {
                    "count": 2
                }
            }
        }

        self.plugin.global_dimensions = {"foo": "bar"}
        process = "master"

        expected_dim = {"foo": "bar", "spark_process": process}
        expected_resp = [MetricRecord("master.workers",
                                      "gauge", 100, expected_dim),
                         MetricRecord("HiveExternalCatalog.fileCacheHits",
                                      "counter", 2, expected_dim)]

        actual_resp = self.plugin.get_metrics(resp, process)
        self.assertEqual(len(expected_resp), len(actual_resp))
        self._validate_metrics(expected_resp, actual_resp)

    def test_read_and_post_metrics(self):
        self.plugin.metric_address = "http://host"
        self.plugin.master_port = "8080"
        self.plugin.worker_port = "8081"
        self.plugin.read()

        exp_mr_1 = MetricRecord("jvm.heap.committed", "gauge",
                                0.0257, {"spark_process": "master"})
        exp_mr_2 = MetricRecord("jvm.heap.committed", "gauge",
                                0.0434, {"spark_process": "worker"})
        exp_mr_3 = MetricRecord("jvm.heap.used", "gauge",
                                26716912, {"spark_process": "master"})
        exp_mr_4 = MetricRecord("jvm.heap.used", "gauge",
                                45102544, {"spark_process": "worker"})
        exp_mr_5 = MetricRecord("HiveExternalCatalog.fileCacheHits", "counter",
                                2, {"spark_process": "master"})
        exp_mr_6 = MetricRecord("HiveExternalCatalog.fileCacheHits", "counter",
                                0, {"spark_process": "worker"})
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3,
                            exp_mr_4, exp_mr_5, exp_mr_6]
        self._verify_records_captured(expected_records)

    def _validate_metrics(self, expected_resp, actual_resp):
        sep = sorted(expected_resp, key=lambda mr: mr.name)
        sap = sorted(actual_resp, key=lambda mr: mr.name)
        for i in range(len(sep)):
            self._validate_single_record(sep[i], sap[i])

    def _validate_single_record(self, expected_record, actual_record):
        """
        Taken from nginx-plus unit testing
        """
        self.assertIsNotNone(actual_record)
        self.assertEquals(expected_record.name, actual_record.name)
        self.assertEquals(expected_record.type, actual_record.type)
        self.assertEquals(expected_record.value, actual_record.value)
        self.assertDictEqual(expected_record.dimensions,
                             actual_record.dimensions)

    def _verify_records_captured(self, expected_records):
        """
        Taken from nginx-plus unit testing
        """
        for expected_record in expected_records:
            self.assertIsNotNone(next((record for record
                                       in self.mock_sink.captured_records
                                       if self._compare_records(
                                        expected_record, record)), None),
                                 'Captured records does not contain: {} \
                                  captured records: {}'
                                 .format(expected_record.to_string(),
                                         [record.to_string() for record
                                         in self.mock_sink.captured_records]))

    def _compare_records(self, expected_record, actual_record):
        """
        Taken from nginx-plus unit testing
        """
        try:
            self._validate_single_record(expected_record, actual_record)
            return True
        except Exception:
            pass
        return False


class SparkApplicationTest(TestCase):

    def setUp(self):
        self.plugin = SparkApplicationPlugin()
        self.plugin.spark_agent = get_mock_spark_agent()
        self.mock_sink = MockMetricSink()
        self.plugin.metric_sink = self.mock_sink

    def test_configure_exceptions(self):
        config_map = {"Dimensions": "foo=bar,hello=world"}
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map["Cluster"] = "Standalone"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        del config_map["Cluster"]
        config_map["Master"] = "http://host:port"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map["Cluster"] = "unsupported_cluster_name"
        config_map["Master"] = "http://host:port"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map["Cluster"] = "Standalone"
        config_map["Master"] = "http://host"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

    def _validate_single_record(self, expected_record, actual_record):
        """
        Taken from nginx-plus unit testing
        """
        self.assertIsNotNone(actual_record)
        self.assertEquals(expected_record.name, actual_record.name)
        self.assertEquals(expected_record.type, actual_record.type)
        self.assertEquals(expected_record.value, actual_record.value)
        self.assertDictEqual(expected_record.dimensions,
                             actual_record.dimensions)

    def _verify_records_captured(self, expected_records):
        """
        Taken from nginx-plus unit testing
        """
        for expected_record in expected_records:
            self.assertIsNotNone(next((record for record
                                       in self.mock_sink.captured_records
                                       if self._compare_records(
                                        expected_record, record)), None),
                                 'Captured records does not contain: {} \
                                  captured records: {}'
                                 .format(expected_record.to_string(),
                                         [record.to_string() for record
                                         in self.mock_sink.captured_records]))

    def _compare_records(self, expected_record, actual_record):
        """
        Taken from nginx-plus unit testing
        """
        try:
            self._validate_single_record(expected_record, actual_record)
            return True
        except Exception:
            pass
        return False



