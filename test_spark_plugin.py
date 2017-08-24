#!/usr/bin/env python
import sample_responses
import sample_html
import spark_plugin
from spark_plugin import SparkProcessPlugin, SparkApplicationPlugin, \
                         MetricRecord
from unittest import TestCase
from mock import Mock, MagicMock, patch


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

        config_map['WorkerPorts'] = "Non-integer port"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map['WorkerPorts'] = "8080.0"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

        config_map['MetricsURL'] = "localhost"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

    def test_configure_worker_port_none(self):
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "MetricsURL": "http://host",
                      "MasterPort": "8080"}

        expected_global_dim = {"foo": "bar", "hello": "world"}
        expected_metric_address = "http://host"
        expected_master_port = "8080"

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.metric_address, expected_metric_address)
        self.assertEqual(self.plugin.master_port, expected_master_port)
        self.assertEqual(0, len(self.plugin.worker_ports))

    def test_configure_master_port_none(self):
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "MetricsURL": "http://host",
                      "WorkerPorts": "8081"}

        expected_global_dim = {"foo": "bar", "hello": "world"}
        expected_metric_address = "http://host"
        expected_worker_port = "8081"

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.metric_address, expected_metric_address)
        self.assertEqual(self.plugin.worker_ports[0], expected_worker_port)
        self.assertIsNone(self.plugin.master_port)

    def test_configure(self):
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "Dimension": "key=value",
                      "MetricsURL": "http://host",
                      "MasterPort": "8080",
                      "WorkerPorts": "8081",
                      "EnhancedMetrics": "True",
                      "IncludeMetrics": "metric1,metric2"}

        expected_global_dim = {"foo": "bar", "hello": "world", "key": "value"}
        expected_metric_address = "http://host"
        expected_master_port = "8080"
        expected_worker_port = "8081"

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.metric_address, expected_metric_address)
        self.assertEqual(self.plugin.worker_ports[0], expected_worker_port)
        self.assertEqual(self.plugin.master_port, expected_master_port)
        assert(self.plugin.enhanced_flag)
        self.assertEqual(2, len(self.plugin.include))

    def test_get_metrics(self):
        resp = {}
        process = ""

        expected_resp = []

        include = set()
        include.add("HiveExternalCatalog.fileCacheHits")
        self.plugin.include = include
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
        self.plugin.worker_ports = ["8081"]
        include = set()
        include.add("HiveExternalCatalog.fileCacheHits")
        self.plugin.include = include
        self.plugin.read()

        exp_mr_1 = MetricRecord("jvm.heap.committed", "gauge",
                                0.0257, {"spark_process": "master"})
        exp_mr_2 = MetricRecord("jvm.heap.committed", "gauge",
                                0.0434, {"spark_process": "worker",
                                         "worker_port": "8081"})
        exp_mr_3 = MetricRecord("jvm.heap.used", "gauge",
                                26716912, {"spark_process": "master"})
        exp_mr_4 = MetricRecord("jvm.heap.used", "gauge",
                                45102544, {"spark_process": "worker",
                                           "worker_port": "8081"})
        exp_mr_5 = MetricRecord("HiveExternalCatalog.fileCacheHits", "counter",
                                2, {"spark_process": "master"})
        exp_mr_6 = MetricRecord("HiveExternalCatalog.fileCacheHits", "counter",
                                0, {"spark_process": "worker",
                                    "worker_port": "8081"})
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
        self.apps = {"app_id": ("app_name", "user", "http://host:port")}
        self.key = ("app_name", "user")
        self.expected_dim = {"app_name": "app_name", "user": "user"}
        self.expected_standalone_dim = {"app_name": "standalone_name",
                                        "user": "standalone_user"}
        self.expected_mesos_dim = {"app_name": "mesos_name",
                                   "user": "mesos_user"}

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

        config_map["Master"] = "host:8080"
        with self.assertRaises(ValueError):
            self.plugin.configure(config_map)

    def test_configure(self):
        cluster = "Standalone"
        config_map = {"Dimensions": "foo=bar,hello=world",
                      "Dimension": "key=value",
                      "Master": "http://host:8080",
                      "Cluster": cluster,
                      "EnhancedMetrics": "False",
                      "ExcludeMetrics": "metric1,metric2"}

        expected_global_dim = {"foo": "bar", "hello": "world",
                               "key": "value", "cluster": cluster}
        expected_master = "http://host:8080"
        expected_cluster = cluster

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.master, expected_master)
        self.assertEqual(self.plugin.cluster_mode, expected_cluster)
        assert(not self.plugin.enhanced_flag)
        self.assertEqual(2, len(self.plugin.exclude))

        cluster = "Mesos"
        config_map["Cluster"] = cluster
        expected_global_dim["cluster"] = cluster
        expected_cluster = cluster
        self.plugin.global_dimensions = {}

        self.plugin.configure(config_map)
        self.assertDictEqual(self.plugin.global_dimensions,
                             expected_global_dim)
        self.assertEqual(self.plugin.master, expected_master)
        self.assertEqual(self.plugin.cluster_mode, expected_cluster)

    def test_running_apps_fail(self):
        apps = self.plugin._get_running_apps()
        self.assertEqual(0, len(apps))

    @patch("spark_plugin.SparkApplicationPlugin._get_standalone_apps")
    def test_standalone_running_apps(self, mock_get_apps):
        self.plugin.cluster_mode = "Standalone"
        self.plugin._get_running_apps()
        mock_get_apps.assert_called()

    @patch("spark_plugin.SparkApplicationPlugin._get_mesos_apps")
    def test_mesos_running_apps(self, mock_get_apps):
        self.plugin.cluster_mode = "Mesos"
        self.plugin._get_running_apps()
        mock_get_apps.assert_called()

    def test_get_standalone_apps(self):
        expected_apps = {"standalone_id": ("standalone_name",
                                           "standalone_user",
                                           "http://172.31.15.190:4040")}
        actual_apps = self.plugin._get_standalone_apps()
        self.assertDictEqual(actual_apps, expected_apps)
        self.assertGreater(len(self.plugin.metrics), 0)
        self.assertDictEqual(self.plugin.metrics[("standalone_name",
                                                  "standalone_user")], {})

    def test_get_mesos_apps(self):
        expected_apps = {"mesos_id": ("mesos_name",
                                      "mesos_user",
                                      "http://10.0.2.155:4040")}
        actual_apps = self.plugin._get_mesos_apps()
        self.assertDictEqual(actual_apps, expected_apps)
        self.assertGreater(len(self.plugin.metrics), 0)
        self.assertDictEqual(self.plugin.metrics[("mesos_name",
                                                  "mesos_user")], {})

    def test_get_streaming_metrics(self):
        self.plugin.metrics[self.key] = {}
        self.plugin._get_streaming_metrics(self.apps)
        exp_mr_1 = MetricRecord("spark.streaming.avg_input_rate",
                                "gauge", 0.0, self.expected_dim)
        exp_mr_2 = MetricRecord("spark.streaming.avg_scheduling_delay",
                                "gauge", 4, self.expected_dim)
        exp_mr_3 = MetricRecord("spark.streaming.avg_processing_time",
                                "gauge", 93, self.expected_dim)
        exp_mr_4 = MetricRecord("spark.streaming.avg_total_delay",
                                "gauge", 97, self.expected_dim)
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3, exp_mr_4]
        actual_records = list(self.plugin.metrics[self.key].values())
        self._validate_metrics(expected_records, actual_records)

    def test_get_job_metrics(self):
        self.plugin.metrics[self.key] = {}
        self.plugin._get_job_metrics(self.apps)
        exp_mr_1 = MetricRecord("spark.job.num_active_tasks",
                                "gauge", 1, self.expected_dim)
        exp_mr_2 = MetricRecord("spark.job.num_active_stages",
                                "gauge", 2, self.expected_dim)
        exp_mr_3 = MetricRecord("spark.num_running_jobs",
                                "gauge", 1, self.expected_dim)
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3]
        actual_records = list(self.plugin.metrics[self.key].values())
        self._validate_metrics(expected_records, actual_records)

    def test_get_stage_metrics(self):
        self.plugin.metrics[self.key] = {}
        self.plugin._get_stage_metrics(self.apps)
        exp_mr_1 = MetricRecord("spark.stage.input_bytes",
                                "gauge", 1553, self.expected_dim)
        exp_mr_2 = MetricRecord("spark.stage.input_records",
                                "gauge", 5, self.expected_dim)
        exp_mr_3 = MetricRecord("spark.num_active_stages",
                                "gauge", 2, self.expected_dim)
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3]
        actual_records = list(self.plugin.metrics[self.key].values())
        self._validate_metrics(expected_records, actual_records)

    def test_get_executor_metrics(self):
        self.plugin.metrics[self.key] = {}
        self.plugin._get_executor_metrics(self.apps)
        exp_mr_1 = MetricRecord("spark.driver.memory_used",
                                "counter", 30750, self.expected_dim)
        exp_mr_2 = MetricRecord("spark.driver.disk_used",
                                "counter", 1155, self.expected_dim)
        exp_mr_3 = MetricRecord("spark.executor.memory_used",
                                "counter", 34735, self.expected_dim)
        exp_mr_4 = MetricRecord("spark.executor.disk_used",
                                "counter", 1973, self.expected_dim)
        exp_mr_5 = MetricRecord("spark.executor.count",
                                "gauge", 3, self.expected_dim)
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3, exp_mr_4, exp_mr_5]
        actual_records = list(self.plugin.metrics[self.key].values())
        self._validate_metrics(expected_records, actual_records)

    def test_mesos_read_and_post_metrics(self):
        exp_mr_1 = MetricRecord("spark.streaming.avg_input_rate",
                                "gauge", 0.0, self.expected_standalone_dim)
        exp_mr_2 = MetricRecord("spark.streaming.avg_scheduling_delay",
                                "gauge", 4, self.expected_standalone_dim)
        exp_mr_3 = MetricRecord("spark.streaming.avg_processing_time",
                                "gauge", 93, self.expected_standalone_dim)
        exp_mr_4 = MetricRecord("spark.streaming.avg_total_delay",
                                "gauge", 97, self.expected_standalone_dim)

        exp_mr_5 = MetricRecord("spark.job.num_active_tasks",
                                "gauge", 1, self.expected_standalone_dim)
        exp_mr_6 = MetricRecord("spark.job.num_active_stages",
                                "gauge", 2, self.expected_standalone_dim)
        exp_mr_7 = MetricRecord("spark.num_running_jobs",
                                "gauge", 1, self.expected_standalone_dim)

        exp_mr_8 = MetricRecord("spark.stage.input_bytes",
                                "gauge", 1553, self.expected_standalone_dim)
        exp_mr_9 = MetricRecord("spark.stage.input_records",
                                "gauge", 5, self.expected_standalone_dim)
        exp_mr_10 = MetricRecord("spark.num_active_stages",
                                 "gauge", 2, self.expected_standalone_dim)

        exp_mr_11 = MetricRecord("spark.driver.memory_used",
                                 "counter", 30750,
                                 self.expected_standalone_dim)
        exp_mr_12 = MetricRecord("spark.driver.disk_used",
                                 "counter", 1155, self.expected_standalone_dim)
        exp_mr_13 = MetricRecord("spark.executor.memory_used",
                                 "counter", 34735,
                                 self.expected_standalone_dim)
        exp_mr_14 = MetricRecord("spark.executor.disk_used",
                                 "counter", 1973, self.expected_standalone_dim)
        exp_mr_15 = MetricRecord("spark.executor.count",
                                 "gauge", 3, self.expected_standalone_dim)
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3, exp_mr_4,
                            exp_mr_5, exp_mr_6, exp_mr_7, exp_mr_8,
                            exp_mr_9, exp_mr_10, exp_mr_11, exp_mr_12,
                            exp_mr_13, exp_mr_14, exp_mr_15]

        self.plugin.cluster_mode = "Standalone"
        self.plugin.read()
        self._verify_records_captured(expected_records)

    def test_standalone_read_and_post_metrics(self):
        exp_mr_1 = MetricRecord("spark.streaming.avg_input_rate",
                                "gauge", 0.0, self.expected_mesos_dim)
        exp_mr_2 = MetricRecord("spark.streaming.avg_scheduling_delay",
                                "gauge", 4, self.expected_mesos_dim)
        exp_mr_3 = MetricRecord("spark.streaming.avg_processing_time",
                                "gauge", 93, self.expected_mesos_dim)
        exp_mr_4 = MetricRecord("spark.streaming.avg_total_delay",
                                "gauge", 97, self.expected_mesos_dim)

        exp_mr_5 = MetricRecord("spark.job.num_active_tasks",
                                "gauge", 1, self.expected_mesos_dim)
        exp_mr_6 = MetricRecord("spark.job.num_active_stages",
                                "gauge", 2, self.expected_mesos_dim)
        exp_mr_7 = MetricRecord("spark.num_running_jobs",
                                "gauge", 1, self.expected_mesos_dim)

        exp_mr_8 = MetricRecord("spark.stage.input_bytes",
                                "gauge", 1553, self.expected_mesos_dim)
        exp_mr_9 = MetricRecord("spark.stage.input_records",
                                "gauge", 5, self.expected_mesos_dim)
        exp_mr_10 = MetricRecord("spark.num_active_stages",
                                 "gauge", 2, self.expected_mesos_dim)

        exp_mr_11 = MetricRecord("spark.driver.memory_used",
                                 "counter", 30750,
                                 self.expected_mesos_dim)
        exp_mr_12 = MetricRecord("spark.driver.disk_used",
                                 "counter", 1155, self.expected_mesos_dim)
        exp_mr_13 = MetricRecord("spark.executor.memory_used",
                                 "counter", 34735,
                                 self.expected_mesos_dim)
        exp_mr_14 = MetricRecord("spark.executor.disk_used",
                                 "counter", 1973, self.expected_mesos_dim)
        exp_mr_15 = MetricRecord("spark.executor.count",
                                 "gauge", 3, self.expected_mesos_dim)
        expected_records = [exp_mr_1, exp_mr_2, exp_mr_3, exp_mr_4,
                            exp_mr_5, exp_mr_6, exp_mr_7, exp_mr_8,
                            exp_mr_9, exp_mr_10, exp_mr_11, exp_mr_12,
                            exp_mr_13, exp_mr_14, exp_mr_15]

        self.plugin.cluster_mode = "Mesos"
        self.plugin.read()
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
