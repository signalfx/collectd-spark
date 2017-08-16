#!/bin/bash
flake8 collectd-spark.py test_spark.py
py.test test_spark.py
