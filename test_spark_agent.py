#!/usr/bin/env python
import random
import string
from urllib2 import HTTPError, URLError
from unittest import TestCase
from mock import Mock, patch
from spark_plugin import SparkAgent


class FilePointer(object):
    def __init__(self):
        self.read = "HTTPError"
        self.readline = "HTTPError"

class SparkAgentTest(TestCase):

    def setUp(self):
        self.agent = SparkAgent()
        self.host = random_string()
        self.port = random_int()

    @patch('urllib2.urlopen')
    def test_rest_request_basic(self, mock_urlopen):
        mock_response = Mock()
        mock_urlopen.return_value = mock_response

        actual_response = self.agent.rest_request("http://host", "/path")
        self.assertEqual(actual_response, mock_response)

    @patch('urllib2.urlopen')
    def test_rest_request_exceptions(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(FilePointer(), FilePointer(),
                                             FilePointer(), FilePointer(),
                                             FilePointer())
        response = self.agent.rest_request("http://host", "/path")
        self.assertIsNone(response)

        mock_urlopen.side_effect = URLError("Thrown from \
            test_rest_request_exceptions")
        response = self.agent.rest_request("http://host", "/path")
        self.assertIsNone(response)


def random_string(length=8):
    return ''.join(random.choice(string.lowercase) for i in range(length))


def random_int(start=0, stop=100000):
    return random.randint(start, stop)
