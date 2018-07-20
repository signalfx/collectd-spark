try:
    import collectd
except ImportError:
    import dummy_collectd as collectd

import metrics
import urllib2
import json
import re

from urlparse import urljoin

PLUGIN_NAME = "apache_spark"
APPS = 'Applications'
DEFAULT_INTERVAL = 10

# Metric sink
METRIC_ADDRESS = "MetricsURL"

# Spark cluster modes
SPARK_STANDALONE_MODE = 'Standalone'
SPARK_MESOS_MODE = 'Mesos'
SPARK_YARN_MODE = 'Yarn'

# URL paths
APPS_ENDPOINT = 'api/v1/applications'
MASTER_PATH = 'metrics/master/json/'
WORKER_PATH = 'metrics/json/'

STANDALONE_STATE_PATH = '/json/'
STANDALONE_APP_PATH = '/app/'
MESOS_MASTER_APP_PATH = '/frameworks'

YARN_MASTER_APP_PATH = '/ws/v1/cluster/apps'

# Error match
HTTP_404_ERROR = "Error 404"


def _validate_url(url):
    return url.startswith("http://")


def _validate_kv(kv):
    """
    check for malformed data on split

    Args:
    kv (list): List of key value pair

    Returns:
    bool: True if list contained expected pair and False otherwise
    """
    if len(kv) == 2 and '' not in kv:
        return True
    return False


def _dimensions_str_to_dict(dimensions_str):
    """
    convert str config of dimensions into dictionary

    Args:
    dimensions_str (str): String representing custom dimensions
    """

    dimensions = {}
    dimensions_list = dimensions_str.strip().split(',')

    for dimension in dimensions_list:
        kv = dimension.strip().split('=')
        if _validate_kv(kv):
            dimensions[kv[0]] = kv[1]
        else:
            collectd.info("Could not validate key-val in dimensions \
             from configuration file.")
    return dimensions


def _add_metrics_to_set(set, metrics):
    metrics_list = metrics.strip().split(",")
    for metric in metrics_list:
        set.add(metric)


class MetricRecord(object):
    """
    Struct for all information needed to emit a single collectd metric.
    MetricSink is the expected consumer of instances of this class.
    Taken from collectd-nginx-plus plugin.
    """

    TO_STRING_FORMAT = '[name={},type={},value={},dimensions={}]'

    def __init__(self, metric_name, metric_type, value,
                 dimensions=None, timestamp=None):
        self.name = metric_name
        self.type = metric_type
        self.value = value
        self.dimensions = dimensions or {}

    def to_string(self):
        return MetricRecord.TO_STRING_FORMAT.format(self.name,
                                                    self.type,
                                                    self.value,
                                                    self.dimensions)


class MetricSink(object):
    """
    Responsible for transforming and dispatching a MetricRecord via collectd.
    Taken from collectd-nginx-plus plugin.
    """

    def emit(self, metric_record):
        """
        Construct a single collectd Values instance from the given MetricRecord
        and dispatch.
        """
        emit_value = collectd.Values()
        emit_value.plugin = PLUGIN_NAME
        emit_value.values = [metric_record.value]
        emit_value.type = metric_record.type
        emit_value.type_instance = metric_record.name
        emit_value.plugin_instance = '[{0}]'.format(
            self._format_dimensions(metric_record.dimensions))

        # With some versions of CollectD, a dummy metadata map must to be added
        # to each value for it to be correctly serialized to JSON by the
        # write_http plugin. See
        # https://github.com/collectd/collectd/issues/716
        emit_value.meta = {'true': 'true'}

        emit_value.dispatch()

    def _format_dimensions(self, dimensions):
        """
        Formats a dictionary of key/value pairs
        as a comma-delimited list of key=value tokens.
        Taken from docker-collectd-plugin.
        """
        return ','.join(['='.join((key.replace('.', '_'), value))
                        for key, value in dimensions.iteritems()])


class SparkAgent(object):
    """
    Agent for facilitating REST calls
    """

    def request_metrics(self, url, path, *args, **kwargs):
        """
        Makes REST call and converts response to JSON
        """
        resp = self.rest_request(url, path, *args, **kwargs)
        if not resp:
            return []

        try:
            return json.load(resp)
        except ValueError, e:
            collectd.info("Error parsing JSON from API call (%s) %s/%s" %
                          (e, url, path))
            return []

    def rest_request(self, url, path, *args, **kwargs):
        """
        Makes REST call to Spark API endpoint
        """
        url = url.rstrip('/')+"/"+path.lstrip('/')
        if args:
            for arg in args:
                url = url.rstrip('/')+"/"+arg.lstrip('/')

        if kwargs:
            query = '&'.join(['{0}={1}'.format(key, value)
                             for key, value in kwargs.iteritems()])
            url = urljoin(url, '?' + query)

        try:
            req = urllib2.Request(url)
            resp = urllib2.urlopen(req)
            return resp
        except (urllib2.HTTPError, urllib2.URLError) as e:
            if HTTP_404_ERROR not in str(e):
                collectd.info("Unable to make request at (%s) %s" % (e, url))
            return None
        except Exception:
            return None


class SparkProcessPlugin(object):
    """
    Spark process plugin parent for posting metrics
    """

    def __init__(self):
        self.global_dimensions = {}
        self.spark_agent = SparkAgent()
        self.metric_sink = MetricSink()
        self.master_port = None
        self.worker_ports = []
        self.enhanced_flag = False
        self.include = set()
        self.exclude = set()

    def configure(self, config_map):
        """
        Configuration callback for collectd

        Args:
        config_map (dict): mapping of config name to value
        """
        collectd.info("Configuring Spark Process Plugin ...")

        if 'MasterPort' not in config_map and 'WorkerPorts' not in config_map:
            raise ValueError("No port provided for metrics url - \
                please provide key 'MasterPort' and/or 'WorkerPorts'")
            return

        for key, value in config_map.iteritems():
            if key == METRIC_ADDRESS:
                if not _validate_url(value):
                    raise ValueError("URL is not prefixed with http://")
                self.metric_address = value
            elif key == 'MasterPort':
                collectd.info("MasterPort detected")
                self.master_port = str(int(value))
            elif key == 'WorkerPorts':
                collectd.info("WorkerPort(s) detected")
                for port in value:
                    self.worker_ports.append(str(int(port)))
            elif key == 'Dimensions' or key == 'Dimension':
                self.global_dimensions.update(
                        _dimensions_str_to_dict(value))
            elif key == 'EnhancedMetrics' and value == "True":
                self.enhanced_flag = True
            elif key == 'IncludeMetrics':
                _add_metrics_to_set(self.include, value)
            elif key == 'ExcludeMetrics':
                _add_metrics_to_set(self.exclude, value)

        collectd.info("Successfully configured Spark Process Plugin ...")

    def get_metrics(self, resp, process, *args):
        """
        Helper method to parse response json

        Args:
        resp (dict): response from API call, mapping of metric name to value
        process (str): String representing spark process
                        - either 'master' or 'worker'

        Returns:
        list of MetricRecord objects to be sent to SignalFx
        """
        if not resp:
            return []

        dim = {"spark_process": process}

        if args:
            dim["worker_port"] = args[0]

        dim.update(self.global_dimensions)

        metric_records = []

        for key, (metric_type, metric_type_cat, metric_key) \
                in metrics.SPARK_PROCESS_METRICS.iteritems():

            data = resp[metric_type_cat]
            if key not in data:
                continue

            metric_name = key
            metric_value = data[key][metric_key]

            metric_records.append(MetricRecord(metric_name,
                                               metric_type, metric_value, dim))

        if not self.enhanced_flag and len(self.include) == 0:
            return metric_records

        for key, (metric_type, metric_type_cat, metric_key) \
                in metrics.SPARK_PROCESS_METRICS_ENHANCED.iteritems():

            data = resp[metric_type_cat]
            if key not in data:
                continue
            if key in self.exclude:
                continue
            if len(self.include) > 0 and key not in self.include:
                continue

            metric_name = key
            metric_value = data[key][metric_key]

            metric_records.append(MetricRecord(metric_name,
                                               metric_type, metric_value, dim))

        return metric_records

    def read(self):
        """
        Makes API calls for cluster level metrics
        related to master and worker nodes
        and posts them to SignalFx
        """
        if self.master_port:
            master = self.metric_address+":"+self.master_port
            master_resp = self.spark_agent.request_metrics(master, MASTER_PATH)
            master_metrics = self.get_metrics(master_resp, 'master')
            self.post_metrics(master_metrics)

        if self.worker_ports:
            for worker_port in self.worker_ports:
                worker = self.metric_address+":"+worker_port
                worker_resp = self.spark_agent.request_metrics(worker,
                                                               WORKER_PATH)
                worker_metrics = self.get_metrics(worker_resp,
                                                  'worker', worker_port)
                self.post_metrics(worker_metrics)

    def post_metrics(self, metrics):
        """
        Post master and worker metrics
        """
        for metric in metrics:
            self.metric_sink.emit(metric)


class SparkApplicationPlugin(object):
    """
    Class encapsulating collectd callback functions
    """

    def __init__(self):
        self.global_dimensions = {}
        self.spark_agent = SparkAgent()
        self.metric_sink = MetricSink()
        self.metrics = {}
        self.cluster_mode = None
        self.master = None
        self.enhanced_flag = False
        self.include = set()
        self.exclude = set()

    def configure(self, config_map):
        """
        Configuration callback for collectd

        Args:
        config_map (dict): mapping of config name to value
        """
        collectd.info("Configuring Spark Application Plugin ...")

        required_keys = ('Cluster', 'Master')

        for key in required_keys:
            if key not in config_map:
                raise ValueError("Missing required config setting: %s" % key)

        for key, value in config_map.iteritems():
            if key == 'Cluster':
                if not self._validate_cluster(value):
                    raise ValueError("Cluster value not: %s, %s, or %s",
                                     (SPARK_STANDALONE_MODE, SPARK_MESOS_MODE,
                                      SPARK_YARN_MODE))
                self.cluster_mode = value
                self.global_dimensions['cluster'] = self.cluster_mode
            elif key == 'Master':
                if not self._validate_master(value):
                    raise ValueError("Master not configured as \
                                     (http://host:port)")
                self.master = value
            elif key == 'Dimensions' or key == 'Dimension':
                self.global_dimensions.update(
                        _dimensions_str_to_dict(value))

            elif key == 'EnhancedMetrics' and value == "True":
                self.enhanced_flag = True
            elif key == 'IncludeMetrics':
                _add_metrics_to_set(self.include, value)
            elif key == 'ExcludeMetrics':
                _add_metrics_to_set(self.exclude, value)

        collectd.info("Successfully configured Spark Application Plugin")

    def read(self):
        """
        Collect metrics from Spark API endpoints and send to SignalFx
        """

        # Get all active apps in cluster
        apps = self._get_running_apps()

        # Get streaming metrics
        self._get_streaming_metrics(apps)

        # Get the job metrics
        self._get_job_metrics(apps)

        # Get the stage metrics
        self._get_stage_metrics(apps)

        # Get the executor metrics
        self._get_executor_metrics(apps)

        # Send metrics to SignalFx
        self.post_metrics()

        # clear metrics data structure
        self.cleanup()

    def _get_standalone_apps(self):
        """
        Retrieve active applications for Spark standalone cluster

        Uses BeautifulSoup module to find driver URL mapped to the specific
        application. The driver URL is the API endpoint required to
        retrieve any application level metrics
        """
        resp = self.spark_agent.request_metrics(self.master,
                                                STANDALONE_STATE_PATH)
        apps = {}
        active_apps = {}

        if 'activeapps' in resp:
            active_apps = resp['activeapps']

        for app in active_apps:
            app_id = app.get('id')
            app_name = app.get('name')
            app_user = app.get('user')

            resp = self.spark_agent.rest_request(self.master,
                                                 STANDALONE_APP_PATH,
                                                 appId=app_id)
            html = resp.read()
            app_url_list = [match.start() for match in
                            re.finditer("Application Detail UI", html)]

            if not app_url_list or len(app_url_list) != 1:
                continue

            end = app_url_list[0]
            quote = html[end-2]
            start = html.rfind(quote, 0, end-2)
            if start == -1:
                continue
            tracking_url = html[start+1:end-2]
            apps[app_id] = (app_name, app_user, tracking_url)
            self.metrics[(app_name, app_user)] = {}
        return apps

    def _get_mesos_apps(self):
        """
        Retrieve all applications corresponding to Spark mesos cluster
        """
        resp = self.spark_agent.request_metrics(self.master,
                                                MESOS_MASTER_APP_PATH)
        apps = {}
        frameworks = {}
        if 'frameworks' in resp:
            frameworks = resp['frameworks']

        for app in frameworks:
            app_id = app.get('id')
            app_name = app.get('name')
            app_user = app.get('user')

            tracking_url = app.get('webui_url')
            if app_id and app_name and app_user and tracking_url:
                apps[app_id] = (app_name, app_user, tracking_url)
                self.metrics[(app_name, app_user)] = {}
        return apps

    def _get_yarn_apps(self):
        """
        Retrieve all applications corresponding to Spark mesos cluster
        from yarn
        """
        resp = self.spark_agent.request_metrics(self.master,
                                                YARN_MASTER_APP_PATH)
        apps = {}

        for app in resp.get('apps', {}).get('app', []):
            if app.get('applicationType', "").upper() == 'SPARK' \
               and app.get('state', "").upper() == "RUNNING":
                app_id = app.get('id')
                app_name = app.get('name')
                app_user = app.get('user')
                tracking_url = app.get('trackingUrl')

                if app_id and app_name and app_user and tracking_url:
                    apps[app_id] = (app_name, app_user, tracking_url)
                    self.metrics[(app_name, app_user)] = {}
        return apps

    def _get_running_apps(self):
        """
        Retrieve running applications in Spark based on cluster
        """
        if not self.cluster_mode:
            collectd.info("Cluster mode not defined")
            return []
        if self.cluster_mode == SPARK_STANDALONE_MODE:
            return self._get_standalone_apps()
        elif self.cluster_mode == SPARK_MESOS_MODE:
            return self._get_mesos_apps()
        elif self.cluster_mode == SPARK_YARN_MODE:
            return self._get_yarn_apps()

    def _get_streaming_metrics(self, apps):
        """
        Get streaming metrics corresponding to a streaming application

        Args:
        apps (dict): Mapping of application id to app name, user,
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent.request_metrics(tracking_url,
                                                    APPS_ENDPOINT, app_id,
                                                    'streaming/statistics')
            if not resp:
                continue

            dim = {"app_name": str(app_name), "user": str(app_user)}
            dim.update(self.global_dimensions)

            stream_stat_metrics = self.metrics[(app_name, app_user)]

            for key, (metric_type, metric_name) in \
                    metrics.SPARK_STREAMING_METRICS.iteritems():

                if key not in resp:
                    continue
                metric_value = resp[key]

                mr = stream_stat_metrics.get(key,
                                             MetricRecord(metric_name,
                                                          metric_type, 0, dim))
                mr.value += metric_value
                stream_stat_metrics[key] = mr

    def _get_job_metrics(self, apps):
        """
        Get job metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user,
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent.request_metrics(tracking_url,
                                                    APPS_ENDPOINT, app_id,
                                                    'jobs', status="RUNNING")
            if not resp:
                continue

            dim = {"app_name": str(app_name), "user": str(app_user)}
            dim.update(self.global_dimensions)

            job_metrics = self.metrics[(app_name, app_user)]
            if len(resp):
                job_metrics['spark.num_running_jobs'] = \
                    MetricRecord('spark.num_running_jobs',
                                 'gauge',
                                 len(resp), dim)

            for job in resp:
                for key, (metric_type, metric_name) in \
                        metrics.SPARK_JOB_METRICS.iteritems():

                    if key not in job:
                        continue
                    metric_value = job[key]

                    mr = job_metrics.get(key, MetricRecord(metric_name,
                                                           metric_type,
                                                           0, dim))
                    mr.value += metric_value
                    job_metrics[key] = mr

    def _get_stage_metrics(self, apps):
        """
        Get stage metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user,
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent.request_metrics(tracking_url,
                                                    APPS_ENDPOINT, app_id,
                                                    'stages', status="ACTIVE")
            if not resp:
                continue

            dim = {"app_name": str(app_name), "user": str(app_user)}
            dim.update(self.global_dimensions)

            stage_metrics = self.metrics[(app_name, app_user)]
            if len(resp):
                stage_metrics['spark.num_active_stages'] = MetricRecord(
                                                    'spark.num_active_stages',
                                                    'gauge', len(resp), dim)

            for stage in resp:
                for key, (metric_type, metric_name) in \
                        metrics.SPARK_STAGE_METRICS.iteritems():

                    if key not in stage:
                        continue

                    metric_value = stage[key]

                    mr = stage_metrics.get(key, MetricRecord(metric_name,
                                                             metric_type,
                                                             0, dim))
                    mr.value += metric_value
                    stage_metrics[key] = mr

            for key, mr in stage_metrics.iteritems():
                if key == "executorRunTime" and len(resp) > 0:
                    mr.value /= len(resp)

            if not self.enhanced_flag and len(self.include) == 0:
                continue

            for stage in resp:
                for key, (metric_type, metric_name) in \
                        metrics.SPARK_STAGE_METRICS_ENHANCED.iteritems():

                    if key not in stage:
                        continue
                    if metric_name in self.exclude:
                        continue
                    if len(self.include) > 0 and \
                            metric_name not in self.include:
                        continue

                    metric_value = stage[key]

                    mr = stage_metrics.get(key, MetricRecord(metric_name,
                                                             metric_type,
                                                             0, dim))
                    mr.value += metric_value
                    stage_metrics[key] = mr

    def _update_driver_metrics(self, executor, exec_metrics, dim):
        """
        Helper method to get driver specific metrics from executor endpoint

        Args:
        executor (dict): JSON response containing driver-specific metrics
        exec_metrics (dict): Mapping of metric name (key in JSON response) to
                             MetricRecord object to be sent by collectd
        dim (dict): Dictionary representing dimensions
        """
        for key, (metric_type, metric_name) in \
                metrics.SPARK_DRIVER_METRICS.iteritems():

            if key not in executor:
                continue
            metric_value = executor[key]

            mr = exec_metrics.get(metric_name, MetricRecord(metric_name,
                                                            metric_type,
                                                            0, dim))
            mr.value += metric_value
            exec_metrics[metric_name] = mr

        if not self.enhanced_flag and len(self.include) == 0:
            return

        for key, (metric_type, metric_name) in \
                metrics.SPARK_DRIVER_METRICS_ENHANCED.iteritems():

            if key not in executor:
                continue
            if metric_name in self.exclude:
                continue
            if len(self.include) > 0 and metric_name not in self.include:
                continue

            metric_value = executor[key]

            mr = exec_metrics.get(metric_name, MetricRecord(metric_name,
                                                            metric_type,
                                                            0, dim))
            mr.value += metric_value
            exec_metrics[metric_name] = mr

    def _update_executor_metrics(self, executor, exec_metrics, dim):
        """
        Helper method to get executor metrics

        Args:
        executor (dict): JSON response containing executor metrics
        exec_metrics (dict): Mapping of metric name (key in JSON response) to
                             MetricRecord object to be sent by collectd
        dim (dict): Dictionary representing dimensions
        """
        for key, (metric_type, metric_name) in \
                metrics.SPARK_EXECUTOR_METRICS.iteritems():

            if key not in executor:
                continue
            metric_value = executor[key]

            mr = exec_metrics.get(metric_name, MetricRecord(metric_name,
                                                            metric_type,
                                                            0, dim))
            mr.value += metric_value
            exec_metrics[metric_name] = mr

        if not self.enhanced_flag and len(self.include) == 0:
            return

        for key, (metric_type, metric_name) in \
                metrics.SPARK_EXECUTOR_METRICS_ENHANCED.iteritems():

            if key not in executor:
                continue
            if metric_name in self.exclude:
                continue
            if len(self.include) > 0 and metric_name not in self.include:
                continue

            metric_value = executor[key]

            mr = exec_metrics.get(metric_name, MetricRecord(metric_name,
                                                            metric_type,
                                                            0, dim))
            mr.value += metric_value
            exec_metrics[metric_name] = mr

    def _get_executor_metrics(self, apps):
        """
        Get executor metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user,
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent.request_metrics(tracking_url,
                                                    APPS_ENDPOINT,
                                                    app_id, 'executors')
            if not resp:
                continue

            dim = {"app_name": str(app_name), "user": str(app_user)}
            dim.update(self.global_dimensions)

            exec_metrics = self.metrics[(app_name, app_user)]

            if len(resp):
                exec_metrics['spark.executor.count'] = MetricRecord(
                                                        'spark.executor.count',
                                                        'gauge',
                                                        len(resp), dim)

            for executor in resp:
                if executor.get('id') == 'driver':
                    self._update_driver_metrics(executor, exec_metrics, dim)
                else:
                    self._update_executor_metrics(executor, exec_metrics, dim)

    def cleanup(self):
        """
        Clear metrics dictionary
        """
        self.metrics = {}

    def post_metrics(self):
        """
        Post app metrics to collectd
        """
        for (app_name, user) in self.metrics:
            app_metrics = self.metrics[(app_name, user)]
            for metric_name, mr in app_metrics.iteritems():
                self.metric_sink.emit(mr)

    def _validate_cluster(self, cluster_mode):
        return cluster_mode == SPARK_STANDALONE_MODE or \
            cluster_mode == SPARK_MESOS_MODE or cluster_mode == SPARK_YARN_MODE

    @classmethod
    def _validate_master(cls, master):
        """
        validate form of master url provided from config file

        Args:
        master (str): String representing master URL

        Returns:
        bool: True if master URL contains host and port as expected
              and False otherwise
        """
        host, port = master.rsplit(":", 1)
        if host and port:
            try:
                port = int(port)
            except (ValueError, TypeError):
                return False
            return _validate_url(host)
        else:
            return False


class SparkPluginManager(object):
    """
    Class managing Spark plugins for reporting metrics
    """

    def __init__(self):
        self.count = 1

    def configure(self, conf):
        collectd.info("Configuring plugins via Spark Plugin Manager")

        config_map = dict([(c.key, c.values[0]) if c.key != "WorkerPorts"
                           else (c.key, c.values) for c in conf.children])
        sp_plugin = None
        sa_plugin = None

        if METRIC_ADDRESS in config_map:
            sp_plugin = SparkProcessPlugin()
            sp_plugin.configure(config_map)

        if APPS in config_map and config_map[APPS] == 'True':
            sa_plugin = SparkApplicationPlugin()
            sa_plugin.configure(config_map)

        plugins = []
        if sp_plugin:
            plugins.append(sp_plugin)
        if sa_plugin:
            plugins.append(sa_plugin)

        if not plugins:
            collectd.info("Not enough parameters supplied to config file")
            return
        collectd.register_read(self.read, interval=DEFAULT_INTERVAL,
                               data=plugins, name="instance"+str(self.count))
        self.count += 1

    def read(self, plugin_list):
        for plugin in plugin_list:
            plugin.read()


if __name__ != '__main__':
    spm = SparkPluginManager()
    collectd.register_config(spm.configure)
