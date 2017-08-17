#!/usr/bin/env python
import sample_responses
import sample_html
import spark_plugin
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







