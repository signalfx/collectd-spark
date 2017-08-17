#!/usr/bin/env python
from unittest import TestCase
from mock import Mock
from spark_plugin import SparkPluginManager


class SparkPluginManagerTest(TestCase):

    def setUp(self):
        self.plugin_manager = SparkPluginManager()

    def test_config_callback(self):
        metric_url = "http://app_master"
        master_port = 8080
        worker_port = 8081
        apps = "True"
        master = "http://master:8080"
        standalone_cluster = "Standalone"
        mesos_cluster = "Mesos"

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

        mock_config_apps_child_2 = get_mock_config_child("Applications", apps)
        mock_config_master_child_2 = get_mock_config_child("Master", master)
        mock_config_cluster_child_2 = get_mock_config_child("Cluster",
                                                            mesos_cluster)

        mock_config_2 = Mock()
        mock_config_2.children = [mock_config_apps_child_2,
                                  mock_config_master_child_2,
                                  mock_config_cluster_child_2]

        self.plugin_manager.configure(mock_config_1)
        self.plugin_manager.configure(mock_config_2)

        self.assertEquals(1, len(self.plugin_manager.process_plugins))
        self.assertEquals(2, len(self.plugin_manager.app_plugins))

        process_plugin = self.plugin_manager.process_plugins[0]
        actual_metric_url_1 = process_plugin.metric_address
        actual_master_port_1 = process_plugin.master_port
        actual_worker_port_1 = process_plugin.worker_port
        self.assertEquals(actual_metric_url_1, metric_url)
        self.assertEquals(actual_master_port_1, str(master_port))
        self.assertEquals(actual_worker_port_1, str(worker_port))

        app_plugin_1 = self.plugin_manager.app_plugins[0]
        actual_master_1 = app_plugin_1.master
        actual_cluster_1 = app_plugin_1.cluster_mode
        self.assertEquals(actual_master_1, master)
        self.assertEquals(actual_cluster_1, standalone_cluster)

        app_plugin_2 = self.plugin_manager.app_plugins[1]
        actual_master_2 = app_plugin_2.master
        actual_cluster_2 = app_plugin_2.cluster_mode
        self.assertEquals(actual_master_2, master)
        self.assertEquals(actual_cluster_2, mesos_cluster)

    def test_read_callback(self):
        mock_plugin_1 = Mock()
        mock_plugin_2 = Mock()
        mock_plugin_3 = Mock()

        self.plugin_manager.process_plugins = [mock_plugin_1, mock_plugin_2]
        self.plugin_manager.app_plugins = [mock_plugin_3]

        self.plugin_manager.read()
        mock_plugin_1.read.assert_called()
        mock_plugin_2.read.assert_called()
        mock_plugin_3.read.assert_called()


def get_mock_config_child(key, value):
    mock_config_child = Mock()
    mock_config_child.key = key
    mock_config_child.values = [value]

    return mock_config_child
