#!/usr/bin/env python
from unittest import TestCase
from mock import Mock, patch
from spark_plugin import SparkPluginManager


class SparkPluginManagerTest(TestCase):

    def setUp(self):
        self.plugin_manager = SparkPluginManager()

    @patch("spark_plugin.SparkApplicationPlugin.configure")
    @patch("spark_plugin.SparkProcessPlugin.configure")
    def test_config_callback_all(self, sp_config, sa_config):
        metric_url = "http://app_master"
        master_port = 8080
        worker_port = 8081
        apps = "True"
        master = "http://master:8080"
        standalone_cluster = "Standalone"

        mock_config_metric_child_1 = get_mock_config_child("MetricsURL",
                                                           metric_url)
        mock_config_master_port_child_1 = get_mock_config_child("MasterPort",
                                                                master_port)
        mock_config_worker_port_child_1 = get_mock_config_child("WorkerPort",
                                                                worker_port)
        mock_config_apps_child_1 = get_mock_config_child("Applications", apps)
        mock_config_master_child_1 = get_mock_config_child("Master", master)
        mock_config_cluster_child_1 = get_mock_config_child("Cluster",
                                                            standalone_cluster)

        mock_config_1 = Mock()
        mock_config_1.children = [mock_config_metric_child_1,
                                  mock_config_master_port_child_1,
                                  mock_config_worker_port_child_1,
                                  mock_config_apps_child_1,
                                  mock_config_master_child_1,
                                  mock_config_cluster_child_1]

        self.plugin_manager.configure(mock_config_1)
        sp_config.assert_called()
        sa_config.assert_called()

    @patch("spark_plugin.SparkApplicationPlugin.configure")
    @patch("spark_plugin.SparkProcessPlugin.configure")
    def test_config_callback_app_only(self, sp_config, sa_config):
        apps = "True"
        master = "http://master:8080"
        mesos_cluster = "Mesos"
        mock_config_apps_child_1 = get_mock_config_child("Applications", apps)
        mock_config_master_child_1 = get_mock_config_child("Master", master)
        mock_config_cluster_child_1 = get_mock_config_child("Cluster",
                                                            mesos_cluster)

        mock_config_1 = Mock()
        mock_config_1.children = [mock_config_apps_child_1,
                                  mock_config_master_child_1,
                                  mock_config_cluster_child_1]
        self.plugin_manager.configure(mock_config_1)
        sp_config.assert_not_called()
        sa_config.assert_called()

    @patch("spark_plugin.SparkApplicationPlugin.configure")
    @patch("spark_plugin.SparkProcessPlugin.configure")
    def test_config_callback_process_only(self, sp_config, sa_config):
        metric_url = "http://app_master"
        master_port = 8080
        worker_port = 8081
        apps = "False"

        mock_config_metric_child_1 = get_mock_config_child("MetricsURL",
                                                           metric_url)
        mock_config_master_port_child_1 = get_mock_config_child("MasterPort",
                                                                master_port)
        mock_config_worker_port_child_1 = get_mock_config_child("WorkerPort",
                                                                worker_port)

        mock_config_apps_child_1 = get_mock_config_child("Applications", apps)
        mock_config_1 = Mock()
        mock_config_1.children = [mock_config_metric_child_1,
                                  mock_config_master_port_child_1,
                                  mock_config_worker_port_child_1]

        self.plugin_manager.configure(mock_config_1)
        sp_config.assert_called()
        sa_config.assert_not_called()

        mock_config_apps_child_1 = get_mock_config_child("Applications", apps)
        mock_config_1.children.append(mock_config_apps_child_1)

        self.plugin_manager.configure(mock_config_1)
        sp_config.assert_called()
        sa_config.assert_not_called()

    def test_read_callback(self):
        plugin_1 = Mock()
        plugin_1.read = Mock()
        plugin_2 = Mock()
        plugin_2.read = Mock()
        plugins = [plugin_1, plugin_2]
        self.plugin_manager.read(plugins)
        plugin_1.read.assert_called()
        plugin_2.read.assert_called()


def get_mock_config_child(key, value):
    mock_config_child = Mock()
    mock_config_child.key = key
    mock_config_child.values = [value]

    return mock_config_child
