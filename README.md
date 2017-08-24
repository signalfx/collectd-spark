# collectd Apache Spark Plugin

An Apache Spark [collectd](http://www.collectd.org/) plugin which users can use to send metrics from Spark cluster/instances to SignalFx

## Installation

* Checkout this repository somewhere on your system accessible by collectd. The suggested location is `/usr/share/collectd/`
* Install the Python requirements with sudo ```pip install -r requirements.txt```
* Configure the plugin (see below)
* Restart collectd

## Requirements

* collectd 4.9 or later (for the Python plugin)
* Python 2.6 or later
* spark 2.2.0 or later 

## Configuration
See https://spark.apache.org/docs/latest/monitoring.html for more information on specific endpoints (specifically 'REST API' and 'Metrics').

At least one of the following keys are expected:

* MetricsURL - Master or worker URL in the form of "http://host" to query if Metrics sink (and therefore, by default the Metrics HTTP Servlet) is enabled  
* Applications - 'True' or 'False' string indicating if you want the specific collectd plugin to collect application level metrics 

At least one of the following keys is required if MetricsURL is provided: 

* MasterPort - Port that master metrics are being sent to from metrics sink (defaulted 8080)
* WorkerPorts - Ports that worker metrics are being sent to from metrics sink (single worker process defaulted 8081 in standalone)

The following keys are required if Applications is set to 'True': 

* Master - Address of Master node in the form "http://host:port" to query for active applications 
* Cluster - Cluster your Spark environment is running on (currently, "Standalone" and "Mesos" are supported)

Optional configurations keys include:

* EnhancedMetrics - Flag to specify whether to include additional metrics 
* IncludeMetrics - Metrics from enhanced metrics that can be included individually
* ExcludeMetrics - Metrics from enhanced metrics that can be excluded individually
* Dimensions - Add multiple global dimensions, formatted as "key1=value1,key2=value2,..."
* Dimension - Add single global dimension to your metrics, formatted as "key=value"


Note that multiple spark instances can be configured in the same file - if utilized, we recommend installing on the master node of the cluster for logical purposes. 

```
LoadPlugin python
<Plugin python>
  ModulePath "/opt/collectd-spark"

  Import spark_plugin
  
  <Module spark_plugin>
    MetricsURL "http://master"
    MasterPort "8080"
    Applications "True"
    EnhancedMetrics "False"
    IncludeMetrics "jvm.pools.Code-Cache.committed"
    Master "http://master:8080"
    Cluster "Standalone"
    Dimension "name=master"
  </Module>
  
  <Module spark_plugin>
    MetricsURL "http://worker"
    WorkerPorts "8081,8082"
    Applications "False"
  </Module>
</Plugin>
```
