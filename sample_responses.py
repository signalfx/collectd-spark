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
        "id": "standalone_id",
        "name": "standalone_name",
        "user": "standalone_user",
        "state": "RUNNING"
    }]
}

master_mesos_json = {
   "frameworks": [
      {
         "id": "mesos_id",
         "name": "mesos_name",
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
         "webui_url": "http://10.0.2.155:4040",
         "active": True,
         "connected": True,
         "recovered": False,
         "user": "mesos_user",
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
  "numActiveTasks": 1,
  "numActiveStages": 2,
  "status": "RUNNING"
}]

active_stages = [{
  "inputBytes": 1353,
  "inputRecords": 2,
  "status": "ACTIVE"
}, {
  "inputBytes": 200,
  "inputRecords": 3,
  "status": "ACTIVE"
}]

executors = [{
  "id": "driver",
  "memoryUsed": 30750,
  "diskUsed": 1155,
}, {
  "id": "1",
  "memoryUsed": 21898,
  "diskUsed": 1823,
}, {
  "id": "0",
  "memoryUsed": 12837,
  "diskUsed": 150,
}]

streaming_stats = {
  "avgInputRate": 0.0,
  "avgSchedulingDelay": 4,
  "avgProcessingTime": 93,
  "avgTotalDelay": 97
}
