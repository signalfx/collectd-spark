master_metrics = {
   "version": "3.0.0",
   "gauges": {
      "jvm.heap.committed": {
         "value": 0.0257
      },
      "jvm.heap.used": {
         "value": 26716912
      }
   },
   "counters": {
      "HiveExternalCatalog.fileCacheHits": {
         "count": 2
      }
   }
}

worker_metrics = {
   "version": "3.0.0",
   "gauges": {
      "jvm.heap.committed": {
         "value": 0.0434
      },
      "jvm.heap.used": {
         "value": 45102544
      }
   },
   "counters": {
      "HiveExternalCatalog.fileCacheHits": {
         "count": 0
      }
   }
}

master_standalone_json = {
    "activeapps": [{
        "starttime": 1502914685924,
        "id": "app-20170816201805-0010",
        "name": "JavaNetworkWordCount",
        "user": "root",
        "memoryperslave": 512,
        "submitdate": "Wed Aug 16 20: 18: 05 UTC 2017",
        "state": "RUNNING",
        "duration": 5983
    }]
}

master_mesos_json = {
   "frameworks": [
      {
         "id": "driver-20170816222145-0030",
         "name": "Spark Pi",
         "pid": "scheduler-69e6e727-d77e: 45769",
         "used_resources": {
            "disk": 0.0,
            "mem": 7040.0,
            "gpus": 0.0,
            "cpus": 10.0
         },
         "offered_resources": {
            "disk": 0.0,
            "mem": 0.0,
            "gpus": 0.0,
            "cpus": 0.0
         },
         "hostname": "ip-10-0-2-155.us-west-2.compute.internal",
         "webui_url": "http: \/\/10.0.2.155: 4040",
         "active": True,
         "connected": True,
         "recovered": False,
         "user": "root",
         "failover_timeout": 0.0,
         "checkpoint": False,
         "registered_time": 1502922223.00481,
         "unregistered_time": 0.0,
         "resources": {
            "disk": 0.0,
            "mem": 7040.0,
            "gpus": 0.0,
            "cpus": 10.0
         },
         "reregistered_time": 1502922223.00481,
         "role": "*",
         "tasks": [
            {
               "id": "4",
               "name": "Task 4",
               "framework_id": "driver-20170816222145-0030",
               "executor_id": "",
               "slave_id": "851545b4-d267-4e9b-9c11-7002c74cf31d-S5",
               "state": "TASK_STAGING",
               "resources": {
                  "disk": 0.0,
                  "mem": 1408.0,
                  "gpus": 0.0,
                  "cpus": 2.0
               },
               "container": {
                  "type": "DOCKER",
                  "docker": {
                     "image": "mesosphere\/spark: 1.1.0-2.1.1-hadoop-2.6",
                     "network": "HOST",
                     "privileged": False,
                     "force_pull_image": False
                  }
               }
            },
            {
               "id": "3",
               "name": "Task 3",
               "framework_id": "driver-20170816222145-0030",
               "executor_id": "",
               "slave_id": "851545b4-d267-4e9b-9c11-7002c74cf31d-S2",
               "state": "TASK_STAGING",
               "resources": {
                  "disk": 0.0,
                  "mem": 1408.0,
                  "gpus": 0.0,
                  "cpus": 1.0
               },
               "container": {
                  "type": "DOCKER",
                  "docker": {
                     "image": "mesosphere\/spark: 1.1.0-2.1.1-hadoop-2.6",
                     "network": "HOST",
                     "privileged": False,
                     "force_pull_image": False
                  }
               }
            },
            {
               "id": "2",
               "name": "Task 2",
               "framework_id": "driver-20170816222145-0030",
               "executor_id": "",
               "slave_id": "851545b4-d267-4e9b-9c11-7002c74cf31d-S4",
               "state": "TASK_STAGING",
               "resources": {
                  "disk": 0.0,
                  "mem": 1408.0,
                  "gpus": 0.0,
                  "cpus": 1.0
               },
               "container": {
                  "type": "DOCKER",
                  "docker": {
                     "image": "mesosphere\/spark: 1.1.0-2.1.1-hadoop-2.6",
                     "network": "HOST",
                     "privileged": False,
                     "force_pull_image": False
                  }
               }
            },
            {
               "id": "1",
               "name": "Task 1",
               "framework_id": "driver-20170816222145-0030",
               "executor_id": "",
               "slave_id": "851545b4-d267-4e9b-9c11-7002c74cf31d-S3",
               "state": "TASK_STAGING",
               "resources": {
                  "disk": 0.0,
                  "mem": 1408.0,
                  "gpus": 0.0,
                  "cpus": 3.0
               },
               "container": {
                  "type": "DOCKER",
                  "docker": {
                     "image": "mesosphere\/spark: 1.1.0-2.1.1-hadoop-2.6",
                     "network": "HOST",
                     "privileged": False,
                     "force_pull_image": False
                  }
               }
            },
            {
               "id": "0",
               "name": "Task 0",
               "framework_id": "driver-20170816222145-0030",
               "executor_id": "",
               "slave_id": "851545b4-d267-4e9b-9c11-7002c74cf31d-S0",
               "state": "TASK_STAGING",
               "resources": {
                  "disk": 0.0,
                  "mem": 1408.0,
                  "gpus": 0.0,
                  "cpus": 3.0
               },
               "container": {
                  "type": "DOCKER",
                  "docker": {
                     "image": "mesosphere\/spark: 1.1.0-2.1.1-hadoop-2.6",
                     "network": "HOST",
                     "privileged": False,
                     "force_pull_image": False
                  }
               }
            }
         ]
      }
   ]
}

running_jobs = [{
  "jobId": 1,
  "name": "start at JavaNetworkWordCount.java: 70",
  "submissionTime": "2017-08-10T21: 14: 39.042GMT",
  "stageIds": [2],
  "status": "RUNNING",
  "numTasks": 1,
  "numActiveTasks": 1,
  "numCompletedTasks": 0,
  "numSkippedTasks": 0,
  "numFailedTasks": 0,
  "numActiveStages": 1,
  "numCompletedStages": 0,
  "numSkippedStages": 0,
  "numFailedStages": 0
}]

active_stages = []

executors = []

streaming_stats = []
