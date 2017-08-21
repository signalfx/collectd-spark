#!/bin/bash
flake8 spark_plugin.py test_spark_plugin.py test_metric_sink.py test_spark_plugin_manager.py test_spark_agent.py test_html.py sample_responses.py
if [ "$?" -ne 0 ]; then
    exit 1;
fi
nosetests test_spark_plugin.py
if [ "$?" -ne 0 ]; then
    exit 1;
fi
nosetests test_metric_sink.py
if [ "$?" -ne 0 ]; then
    exit 1;
fi
nosetests test_spark_plugin_manager.py
if [ "$?" -ne 0 ]; then
    exit 1;
fi
nosetests test_spark_agent.py