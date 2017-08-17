#!/usr/bin/env python
from unittest import TestCase
from mock import patch
from spark_plugin import MetricSink, MetricRecord


class MetricSinkTest(TestCase):
    """
    Unit test for metric sink - adapted from collectd-nginx-plus
    """
    def setUp(self):
        self.sink = MetricSink()
        self.mock_values = CollectdValuesMock()

    @patch('spark_plugin.collectd.Values')
    def test_emit_record(self, mock_collectd):
        mock_collectd.return_value = self.mock_values

        metric_value = 1234567890
        metric_dimensions = {'spark_process': 'master'}

        expected_type = 'counter'
        expected_values = [metric_value]
        expected_plugin_instance = '[spark_process=master]'
        expected_type_instance = 'master.aliveWorkers'
        expected_meta = {'true': 'true'}
        expected_plugin = 'Apache Spark'

        record = MetricRecord(expected_type_instance, expected_type,
                              metric_value, metric_dimensions)

        self.sink.emit(record)
        self.assertEquals(1, len(self.mock_values.dispatch_collector))

        dispatched_value = self.mock_values.dispatch_collector[0]
        self.assertEquals(expected_plugin, dispatched_value.plugin)
        self.assertEquals(expected_values, dispatched_value.values)
        self.assertEquals(expected_type, dispatched_value.type)
        self.assertEquals(expected_type_instance,
                          dispatched_value.type_instance)
        self.assertEquals(expected_plugin_instance,
                          dispatched_value.plugin_instance)
        self.assertDictEqual(expected_meta, dispatched_value.meta)

    def test_format_dimensions(self):
        key_1 = 'my.key.1'
        key_2 = 'my.key.2'
        value_1 = 'my.value.1'
        value_2 = 'my.value.2'

        raw_dimensions = {key_1: value_1, key_2: value_2}

        expected_pair_1 = '{}={}'.format(key_1.replace('.', '_'), value_1)
        expected_pair_2 = '{}={}'.format(key_2.replace('.', '_'), value_2)

        actual_dimensions = self.sink._format_dimensions(raw_dimensions)
        pairs = actual_dimensions.split(',')
        self.assertEquals(2, len(pairs))
        self.assertTrue(expected_pair_1 in pairs)
        self.assertTrue(expected_pair_2 in pairs)


class CollectdValuesMock(object):
    def __init__(self):
        self.dispatch_collector = []

    def dispatch(self):
        self.dispatch_collector.append(self)
