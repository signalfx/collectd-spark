#!/bin/bash
flake8 spark_plugin.py test_spark_plugin.py test_metric_sink.py test_spark_plugin_manager.py test_spark_agent.py test_html.py sample_responses.py
nosetests test_spark_plugin.py
nosetests test_metric_sink.py
nosetests test_spark_plugin_manager.py
nosetests test_spark_agent.py