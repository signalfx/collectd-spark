import json
from unittest import TestCase
from urllib.error import HTTPError, URLError

from mock import Mock, patch

from spark_plugin import SparkAgent


class FilePointer(object):
    def __init__(self):
        self.read = "HTTPError"
        self.readline = "HTTPError"


class SparkAgentTest(TestCase):
    def setUp(self):
        self.agent = SparkAgent()

    @patch("six.moves.urllib.request.urlopen")
    @patch("six.moves.urllib.request.Request")
    def test_rest_request(self, mock_request, mock_urlopen):
        mock_request_response = Mock()
        mock_request.return_value = mock_request_response

        mock_urlopen_response = Mock()
        mock_urlopen.return_value = mock_urlopen_response

        actual_response = self.agent.rest_request("http://host", "/path")

        mock_request.assert_called_with("http://host/path")
        mock_urlopen.assert_called_with(mock_request_response)
        self.assertEqual(actual_response, mock_urlopen_response)

    @patch("six.moves.urllib.request.urlopen")
    def test_rest_request_exceptions(self, mock_urlopen):
        num_args = 5
        mock_urlopen.side_effect = HTTPError(*[FilePointer() for i in range(num_args)])
        response = self.agent.rest_request("http://host", "/path")
        self.assertIsNone(response)

        mock_urlopen.side_effect = URLError(
            "Thrown from \
            test_rest_request_exceptions"
        )
        response = self.agent.rest_request("http://host", "/path")
        self.assertIsNone(response)

    @patch("spark_plugin.SparkAgent.rest_request")
    def test_request_metrics(self, mock_rest_request):
        data = {"key1": "value1", "key2": "value2"}
        mock_rest_request_response = Mock()
        mock_read = Mock()
        mock_read.return_value = json.dumps(data)
        mock_rest_request_response.read = mock_read
        mock_rest_request.return_value = mock_rest_request_response

        expected_response = data
        actual_response = self.agent.request_metrics("http://host", "/path")
        self.assertDictEqual(actual_response, expected_response)
