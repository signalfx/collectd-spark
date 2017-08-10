#import requests
import collectd
import metrics
import urllib2
import json

from bs4 import BeautifulSoup
from urlparse import urljoin

PLUGIN_NAME = "Apache Spark"
APPS = 'Applications'
DEFAULT_INTERVAL = 30 # Currently set to provide more compute time and reduce load  

# Metric sink
METRIC_ADDRESS = "MetricsURL"
MASTER_PATH = "metrics/master/json"
WORKER_PATH = "metrics/json"

# Spark cluster modes 
SPARK_STANDALONE_MODE = 'Standalone'
SPARK_MESOS_MODE = 'Mesos'
SPARK_YARN_MODE = 'Yarn'

# URL paths
APPS_ENDPOINT = 'api/v1/applications'
MASTER_PATH = 'metrics/master/json'
WORKER_PATH = 'metrics/json'

STANDALONE_STATE_PATH = '/json/'
STANDALONE_APP_PATH = '/app/'
MESOS_MASTER_APP_PATH = '/frameworks'

class MetricRecord(object):
    """
    Struct for all information needed to emit a single collectd metric.
    MetricSink is the expected consumer of instances of this class.
    Taken from collectd-nginx-plus plugin.
    """

    TO_STRING_FORMAT = '[name={},type={},value={},dimensions={},timestamp={}]'

    def __init__(self, metric_name, metric_type, value, dimensions=None, timestamp=None):
        self.name = metric_name
        self.type = metric_type
        self.value = value
        self.dimensions = dimensions or {}
        # self.timestamp = timestamp or time.time()

    def to_string(self):
        return MetricRecord.TO_STRING_FORMAT.format(self.name, self.type, self.value,\
            self.instance_id, self.dimensions, self.timestamp)

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

        #emit_value.time = metric_record.timestamp
        emit_value.plugin = PLUGIN_NAME
        emit_value.values = [metric_record.value]
        emit_value.type = metric_record.type
        emit_value.type_instance = metric_record.name
        emit_value.plugin_instance = '[{0}]'.format(self._format_dimensions(metric_record.dimensions))

        # With some versions of CollectD, a dummy metadata map must to be added
        # to each value for it to be correctly serialized to JSON by the
        # write_http plugin. See
        # https://github.com/collectd/collectd/issues/716
        emit_value.meta = {'true': 'true'}

        emit_value.dispatch()

    def _format_dimensions(self, dimensions):
        """
        Formats a dictionary of key/value pairs as a comma-delimited list of key=value tokens.
        Taken from docker-collectd-plugin.
        """
        return ','.join(['='.join((key.replace('.', '_'), value)) for key, value in dimensions.iteritems()])

class SparkAgent(object):
    """
    Agent for facilitating REST calls 
    """

    def _urlopen_to_json(self, url, path, *args, **kwargs):
        """
        Makes REST call and converts response to JSON
        """
        resp = self._url_open(url, path, *args, **kwargs)
        if (resp == None):
            return [] 

        try:
            return json.load(resp)
        except ValueError, e:
            collectd.info("Error parsing JSON from API call (%s) %s/%s" % 
                                (e, url, path))
            return []

    def _url_open(self, url, path, *args, **kwargs):
        """
        Makes REST call to Spark API endpoint 
        """
        url = url.rstrip('/')+"/"+path.lstrip('/')
        if args:
            for arg in args:
                url = url.rstrip('/')+"/"+arg.lstrip('/')

        if kwargs:
            query = '&'.join(['{0}={1}'.format(key, value) for key, value in kwargs.iteritems()])
            url = urljoin(url, '?' + query)

        try:
            collectd.info("********* URL in url_open ********")
            collectd.info(url)
            req = urllib2.Request(url)
            resp = urllib2.urlopen(req)
            #resp = requests.get(url)
            return resp
        except:
            collectd.info("Unable to make request at %s" % url)
            return None

class SparkProcessPlugin(object):
    """
    Spark process plugin parent for posting metrics 
    """

    def __init__(self):
        self.global_dimensions = {}
        self.spark_agent = SparkAgent()
        self.metric_sink = MetricSink()
        self.metrics = []

    def configure(self, config_map):
        """
        Configuration callback for collectd

        Args:
        config_map (dict): mapping of config name to value
        """

        collectd.info("#################")
        collectd.info("Configuring Spark Process Plugin ...")
        collectd.info("#################")

        if 'MasterPort' not in config_map and 'WorkerPort' not in config_map:
            raise ValueError("No port provided for metrics url - \
                please provide key 'MasterPort' and/or 'WorkerPort'")
            return

        for key, value in config_map.iteritems():
            if key == METRIC_ADDRESS:
                self.metric_address = value
            elif key == 'MasterPort': 
                collectd.info("MasterPort detected")
                self.master_port = str(int(value))
            elif key == 'WorkerPort':
                collectd.info("WorkerPort detected")
                self.worker_port = str(int(value))
            elif key == 'Dimensions' or key == 'Dimension':
                self.global_dimensions.update(self._dimensions_str_to_dict(value))

        collectd.info("#################")
        collectd.info("Successfully configured Spark Process Plugin ...")
        collectd.info("#################")

    def get_metrics(self, resp, process):
        """
        Helper method to parse response json 

        Args:
        resp (dict): response from API call, mapping of metric name to value 
        process (str): String representing spark process - either 'master' or 'worker'
        """
        if not resp: return

        dim = {"spark_process" : process}
        dim.update(self.global_dimensions)
        collectd.info("############# GETTING METRICS ############")
        collectd.info(dim['spark_process'])
        for key in metrics.SPARK_PROCESS_METRICS:
            metric_type_cat = metrics.SPARK_PROCESS_METRICS[key]['metric_type_category']
            data = resp[metric_type_cat]
            if key not in data:
                continue

            metric_name = key
            metric_type = metrics.SPARK_PROCESS_METRICS[key]['metric_type']

            metric_key = metrics.SPARK_PROCESS_METRICS[key]['key']
            metric_value = data[key][metric_key]

            self.metrics.append(MetricRecord(metric_name, metric_type, metric_value, dim))

    def read(self):
        """
        Makes API calls for cluster level metrics related to master and worker nodes
        and posts them to SignalFx
        """
        if self.master_port:
            master = self.metric_address+":"+self.master_port
            resp = self.spark_agent._urlopen_to_json(master, MASTER_PATH)
            self.get_metrics(resp, 'master')    

        if self.worker_port:
            worker = self.metric_address+":"+self.worker_port
            resp = self.spark_agent._urlopen_to_json(worker, WORKER_PATH)
            self.get_metrics(resp, 'worker')

        self.post_metrics()


    def post_metrics(self):
        """
        Post master and worker metrics 
        """
        for metric in self.metrics:
            self.metric_sink.emit(metric)

    def _dimensions_str_to_dict(self, dimensions_str):
        """
        convert str config of dimensions into dictionary

        Args:
        dimensions_str (str): String representing custom dimensions
        """

        dimensions = {}
        dimensions_list = dimensions_str.strip().split(',')

        for dimension in dimensions_list:
            kv = dimension.strip().split('=')
            if self._validate_kv(kv):
                dimensions[key_val_split[0]] = key_val_split[1]
            else:
                collectd.info("Could not validate key-val in \
                    dimensions from configuration file.")
        
        return dimensions


class SparkApplicationPlugin(object):
    """
    Class encapsulating collectd callback functions 
    """

    def __init__(self):
        self.global_dimensions = {}
        self.spark_agent = SparkAgent()
        self.metric_sink = MetricSink()
        self.metrics = {}

    def configure(self, config_map):
        """
        Configuration callback for collectd

        Args:
        config_map (dict): mapping of config name to value
        """

        collectd.info("#################")
        collectd.info("Configuring Spark Application Plugin ...")
        collectd.info("#################")

        required_keys = {'Cluster', 'Master'}

        for key in required_keys:
            if key not in config_map:
                raise ValueError("Missing required config setting: %s" % key)

        for key, value in config_map.iteritems():
            if key == 'Cluster':
                if not self._validate_cluster(value):   
                    raise ValueError("Cluster value not: %s, %s, or %s", \
                        (SPARK_STANDALONE_MODE, SPARK_MESOS_MODE, 
                            SPARK_YARN_MODE))
                self.cluster_mode = value
                self.global_dimensions['cluster'] = self.cluster_mode
            
            elif key == 'Master':
                if not self._validate_master(value):
                    raise ValueError("Master not configured as (host:port)")
                self.master = value
            
            elif key == 'Dimensions' or key == 'Dimension':
                self.global_dimensions.update(
                    self._dimensions_str_to_dict(value))

        collectd.info("#################")
        collectd.info("Successfully configured Spark Application Plugin")
        collectd.info("#################")

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

        # Get the rdd metrics
        # Commented out since no way to currently test this endpoint - getting empty [] back 
        # self._get_rdd_metrics(apps)

        # Send metrics to SignalFx
        self.post_metrics()

    def _get_standalone_apps(self):
        """
        Retrieve active applications for Spark standalone cluster 

        Uses BeautifulSoup module to find driver URL mapped to the specific
        application. The driver URL is the API endpoint required to 
        retrieve any application level metrics
        """
        resp = self.spark_agent._urlopen_to_json(self.master, STANDALONE_STATE_PATH)
        apps = {}

        active_apps = resp['activeapps']
        
        for app in active_apps:
            app_id = app.get('id')
            app_name = app.get('name')
            app_user = app.get('user')

            collectd.info(app_name)
            resp = self.spark_agent._url_open(self.master, STANDALONE_APP_PATH, appId=app_id)
            dom = BeautifulSoup(resp.read(), 'html.parser')
            app_detail_ui_links = dom.find_all('a', string='Application Detail UI')
            if not app_detail_ui_links:
                app_detail_ui_links = dom.find_all('a', text='Application Detail UI')

            if app_detail_ui_links and len(app_detail_ui_links) == 1:
                tracking_url = app_detail_ui_links[0].attrs['href']
                collectd.info("*#*#*#**#*# GOT DRIVER URL *#*#*#*#**#")
            else:
                collectd.info("********* COULD NOT get driver url ***********")
                continue
            apps[app_id] = (app_name, app_user, tracking_url)
            self.metrics[(app_name, app_user)] = {}
        return apps

    def _get_mesos_apps(self):
        """
        Retrieve all applications corresponding to Spark mesos cluster
        """
        resp = self.spark_agent._urlopen_to_json(self.master, MESOS_MASTER_APP_PATH)
        apps = {}

        if "frameworks" in resp:
            for app in resp["frameworks"]:
                app_id = app.get('id')
                app_name = app.get('name')
                app_user = app.get('user')

                tracking_url = app.get('webui_url')
                if app_id and app_name and app_user and tracking_url:
                    apps[app_id] = (app_name, app_user, tracking_url)
        collectd.info("########## GOT MESOS FRAMEWORK RESP AND APPS ############")
        return apps

    def _get_running_apps(self):
        """
        Retrieve running applications in Spark based on cluster 
        """
        if not self.cluster_mode:
            collectd.info("Cluster mode not defined")
            return []
        if self.cluster_mode == SPARK_STANDALONE_MODE:
            collectd.info("###### CLUSTER STANDALONE DETECTED #######")
            return self._get_standalone_apps()
        elif self.cluster_mode == SPARK_MESOS_MODE:
            collectd.info("###### CLUSTER MESOS DETECTED #######")
            return self._get_mesos_apps()

    def _get_job_metrics(self, apps):
        """
        Get job metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user, 
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent._urlopen_to_json(tracking_url, \
                APPS_ENDPOINT, app_id, 'jobs')
            if not resp: continue

            if 'streaming' in self.metrics[(app_name, app_user)]:
                continue
            dim = {"app_name" : str(app_name), "user" : str(app_user)}
            dim.update(self.global_dimensions)

            self.metrics[(app_name, app_user)]['jobs'] = {}
            job_metrics = self.metrics[(app_name, app_user)]['jobs']

            job_metrics['total'] = {}
            job_metrics['total']['spark.num_total_jobs'] = MetricRecord('spark.num_total_jobs', 'gauge', len(resp), dim)

            for job in resp:
                status = str(job.get("status")).lower()
                new_dim = {"status" : status}
                new_dim.update(dim)
                
                if status not in job_metrics: job_metrics[status] = {} 
                
                updated_metric_type = ''
                if status == 'running':
                    updated_metric_type = metrics.GAUGE

                for key, (metric_name, metric_type) in metrics.SPARK_JOB_METRICS.iteritems():
                    if key not in job: continue
                    metric_value = job[key]
                    
                    mr = job_metrics[status].get(key, MetricRecord(metric_name, 
                        updated_metric_type if updated_metric_type else metric_type, 0, new_dim))
                    mr.value += metric_value
                    job_metrics[status][key] = mr

    def _get_stage_metrics(self, apps):
        """
        Get stage metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user, 
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent._urlopen_to_json(tracking_url, \
                APPS_ENDPOINT, app_id, 'stages')
            if not resp: continue 

            if 'streaming' in self.metrics[(app_name, app_user)]:
                continue

            dim = {"app_name" : str(app_name), "user" : str(app_user)}
            dim.update(self.global_dimensions)

            self.metrics[(app_name, app_user)]['stages'] = {}
            stage_metrics = self.metrics[(app_name, app_user)]['stages']

            stage_metrics['total'] = {}
            stage_metrics['total']['spark.num_total_stages'] = MetricRecord('spark.num_total_stages', 'gauge', len(resp), dim)

            status_count = {}

            for stage in resp:
                status = str(stage.get("status")).lower()
                status_count[status] = status_count.get(status, 0) + 1
                new_dim = {"status" : status}
                new_dim.update(dim)
                if status not in stage_metrics: stage_metrics[status] = {}
                
                updated_metric_type = ''
                if status == 'active':
                    updated_metric_type = metrics.GAUGE

                for key, (metric_name, metric_type) in metrics.SPARK_STAGE_METRICS.iteritems():
                    if key not in stage: continue
                    metric_value = stage[key]
    
                    mr = stage_metrics[status].get(key, MetricRecord(metric_name, 
                        updated_metric_type if updated_metric_type else metric_type, 0, new_dim))
                    mr.value += metric_value
                    stage_metrics[status][key] = mr

            for status in stage_metrics:
                for key, mr in stage_metrics.iteritems():
                    if key == "executorRunTime":
                        mr.value /= status_count[status]


    def _update_driver_metrics(self, executor, exec_metrics, dim):
        """
        Helper method to get driver specific metrics from executor endpoint

        Args:
        executor (dict): JSON response containing driver-specific metrics   
        exec_metrics (dict): Mapping of metric name (key in JSON response) to 
                             MetricRecord object to be sent by collectd
        dim (dict): Dictionary representing dimensions  
        """
        for key, (metric_name, metric_type) in metrics.SPARK_DRIVER_METRICS.iteritems():
            if key not in executor: continue
            metric_value = executor[key]

            mr = exec_metrics['driver'].get(key, 
                MetricRecord(metric_name, metric_type, 0, dim))
            mr.value += metric_value
            exec_metrics['driver'][key] = mr

    def _update_executor_metrics(self, executor, exec_metrics, dim):
        """
        Helper method to get executor metrics 

        Args:
        executor (dict): JSON response containing executor metrics   
        exec_metrics (dict): Mapping of metric name (key in JSON response) to 
                             MetricRecord object to be sent by collectd
        dim (dict): Dictionary representing dimensions  
        """
        for key, (metric_name, metric_type) in metrics.SPARK_EXECUTOR_METRICS.iteritems():
            if key not in executor: continue
            metric_value = executor[key]

            mr = exec_metrics['executor'].get(key, 
                MetricRecord(metric_name, metric_type, 0, dim))
            mr.value += metric_value
            exec_metrics['executor'][key] = mr

    def _get_executor_metrics(self, apps):
        """
        Get executor metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user, 
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent._urlopen_to_json(tracking_url, \
                APPS_ENDPOINT, app_id, 'executors')
            if not resp: continue 

            dim = {"app_name" : str(app_name), "user" : str(app_user)}
            dim.update(self.global_dimensions)

            self.metrics[(app_name, app_user)]['executors'] = {}
            exec_metrics = self.metrics[(app_name, app_user)]['executors']
            exec_metrics['driver'] = {}
            exec_metrics['executor'] = {}

            if len(resp):
                mr = MetricRecord('spark.executor.count', 
                                    'gauge', len(resp), dim)    
                exec_metrics['executor']['spark.executor.count'] = mr

            for executor in resp:
                if executor.get('id') == 'driver':
                    self._update_driver_metrics(executor, exec_metrics, dim)
                else:
                    self._update_executor_metrics(executor, exec_metrics, dim)

    """ Commented out as dead code:
        can't formally test this method. 

    def _get_rdd_metrics(self, apps):
        '''
        Get RDD metrics corresponding to a running application

        Args:
        apps (dict): Mapping of application id to app name, user, 
                     and url endpoint for API call
        '''
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent._urlopen_to_json(tracking_url, \
                APPS_ENDPOINT, app_id, 'storage/rdd')
            if not resp: continue 

            dim = {"app_name" : str(app_name), "user" : str(app_user)}
            dim.update(self.global_dimensions)

            self.metrics[(app_name, app_user)]['storage'] = {}
            self.metrics[(app_name, app_user)]['storage']['rdd'] = {}

            rdd_metrics = self.metrics[(app_name, app_user)]['storage']['rdd']
            
            if (len(resp)):
                mr = MetricRecord('spark.rdd.count', 'counter', len(resp), dim)
                rdd_metrics['spark.rdd.count'] = mr

            for rdd in resp:
                for key, (metric_name, metric_type) in metrics.SPARK_RDD_METRICS.iteritems():
                    if key not in rdd: continue
                    metric_value = rdd[key]
    
                    mr = rdd_metrics.get(key, 
                        MetricRecord(metric_name, metric_type, 0, dim))
                    mr.value += metric_value
                    rdd_metrics[key] = mr
    """

    def _get_streaming_metrics(self, apps):
        """
        Get streaming metrics corresponding to a streaming application

        Args:
        apps (dict): Mapping of application id to app name, user, 
                     and url endpoint for API call
        """
        for app_id, (app_name, app_user, tracking_url) in apps.iteritems():
            resp = self.spark_agent._urlopen_to_json(tracking_url, \
                APPS_ENDPOINT, app_id, 'streaming/statistics')
            if not resp: 
                collectd.info("********* COULD NOT GET RESP FROM STREAMING APP *************")
                continue 

            dim = {"app_name" : str(app_name), "user" : str(app_user)}
            dim.update(self.global_dimensions)

            self.metrics[(app_name, app_user)]['streaming'] = {}
            self.metrics[(app_name, app_user)]['streaming']['statistics'] = {}

            stream_stat_metrics = self.metrics[(app_name, app_user)]['streaming']['statistics']

            collectd.info("@@@@@@@@@@@@@@@ GOT STAT RESP @@@@@@@@@@@@@@@@@@")
            for key, (metric_name, metric_type) in metrics.SPARK_STREAMING_METRICS.iteritems():
                if key not in resp: continue
                metric_value = resp[key]

                mr = stream_stat_metrics.get(key,
                    MetricRecord(metric_name, metric_type, 0, dim))
                mr.value += metric_value
                stream_stat_metrics[key] = mr

    def post_metrics(self):
        """
        Post app metrics to collectd
        """
        for (app_name, user) in self.metrics:
            endpoint_metrics = self.metrics[(app_name, user)]
            for endpoint in endpoint_metrics:
                ext_metrics = endpoint_metrics[endpoint]
                for ext in ext_metrics:
                    app_metrics = ext_metrics[ext]

                    for metric_name, mr in app_metrics.iteritems():
                        self.metric_sink.emit(mr)

    def _validate_cluster(self, cluster_mode):
        if cluster_mode == SPARK_YARN_MODE:
            raise ValueError("Sorry!!! Yarn not yet supported")
        return cluster_mode == SPARK_STANDALONE_MODE or \
            cluster_mode == SPARK_MESOS_MODE or cluster_mode == SPARK_YARN_MODE

    def _validate_master(self, master):
        """
        validate form of master url provided from config file

        Args:
        master (str): String representing master URL

        Returns:
        bool: True if master URL contains host and port as expected 
              and False otherwise 
        """
        host_port = master.rsplit(":", 1)
        if self._validate_kv(host_port):
            try:
                port = int(host_port[1])
            except:
                return False

            return True
        else:
            return False

    def _validate_kv(self, kv):
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

    def _dimensions_str_to_dict(self, dimensions_str):
        """
        convert str config of dimensions into dictionary

        Args:
        dimensions_str (str): String representing custom dimensions
        """

        dimensions = {}
        dimensions_list = dimensions_str.strip().split(',')

        for dimension in dimensions_list:
            kv = dimension.strip().split('=')
            if self._validate_kv(kv):
                dimensions[key_val_split[0]] = key_val_split[1]
            else:
                collectd.info("Could not validate key-val in dimensions \
                 from configuration file.")
        
        return dimensions

class SparkPluginManager(object):
    """
    Class managing Spark plugins for reporting metrics 
    """

    def __init__(self):
        self.sp_plugin = None
        self.sa_plugin = None

    def configure(self, conf):
        collectd.info("Configuring plugins via Spark Plugin Manager")
        
        config_map = dict([(c.key, c.values[0]) for c in conf.children])

        collectd.info("#################")
        for key, value in config_map.iteritems():
            
            collectd.info(key+" : "+str(value))
        if METRIC_ADDRESS in config_map:
            self.sp_plugin = SparkProcessPlugin()
            self.sp_plugin.configure(config_map)
        collectd.info("#################")

        if APPS not in config_map: return
        if config_map[APPS] == 'False': 
            collectd.info("Key - Applications - set to False: skipping \
                Application Plugin initialization")
            return
        self.sa_plugin = SparkApplicationPlugin()
        self.sa_plugin.configure(config_map)

    def read(self):
        if self.sp_plugin:
            self.sp_plugin.read()
        if self.sa_plugin:
            self.sa_plugin.read()

if __name__ != '__main__':
    spm = SparkPluginManager()
    collectd.register_config(spm.configure)
    collectd.register_read(spm.read, interval=DEFAULT_INTERVAL)