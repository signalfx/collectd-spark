GAUGE = 'gauge'
COUNTER = 'counter'

SPARK_PROCESS_METRICS = {
	
	# jvm generics

	"jvm.total.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.total.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 

	# heap memory 
	
	"jvm.heap.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.heap.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 

	# non-heap memory

	# if max = -1, this is technically unbounded (switching calculation to used/committed)

	"jvm.non-heap.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.non-heap.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 

	# marksweep

	"jvm.MarkSweepCompact.count":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.MarkSweepCompact.time":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 

	# memory pool
	"jvm.pools.Code-Cache.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Code-Cache.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Compressed-Class-Space.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Compressed-Class-Space.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Metaspace.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Metaspace.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Eden-Space.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Eden-Space.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Survivor-Space.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Survivor-Space.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Tenured-Gen.used":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 
	"jvm.pools.Tenured-Gen.committed":{
		"metric_type": GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}, 

	# hive specifics

	"HiveExternalCatalog.fileCacheHits":{  
        "metric_type": COUNTER,
		"metric_type_category" : "counters",
		"key" : "count"
    },
    "HiveExternalCatalog.filesDiscovered":{  
        "metric_type": COUNTER,
		"metric_type_category" : "counters",
		"key" : "count"
    },
    "HiveExternalCatalog.hiveClientCalls":{  
        "metric_type": COUNTER,
		"metric_type_category" : "counters",
		"key" : "count"
    },
    "HiveExternalCatalog.parallelListingJobCount":{  
        "metric_type": COUNTER,
		"metric_type_category" : "counters",
		"key" : "count"
    },
    "HiveExternalCatalog.partitionsFetched":{  
        "metric_type": COUNTER,
		"metric_type_category" : "counters",
		"key" : "count"
    },

	########################

	# worker specific
	"worker.coresFree":{  
		"metric_type" : GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	},
	"worker.coresUsed":{  
 		"metric_type" : GAUGE,
 		"metric_type_category" : "gauges", 
		"key" : "value"
	},
	"worker.executors":{  
 		"metric_type" : GAUGE,
 		"metric_type_category" : "gauges", 
		"key" : "value"
	},
	"worker.memFree_MB":{  
 		"metric_type" : GAUGE,
 		"metric_type_category" : "gauges", 
		"key" : "value"
	},
	"worker.memUsed_MB":{  
 		"metric_type" : GAUGE,
 		"metric_type_category" : "gauges", 
		"key" : "value"
    }, 

    # master specific 
    "master.aliveWorkers": {
		"metric_type" : GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	},

	"master.apps": {
		"metric_type" : GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	},

	"master.waitingApps": {
		"metric_type" : GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	},  
	"master.workers": {
		"metric_type" : GAUGE,
		"metric_type_category" : "gauges", 
		"key" : "value"
	}

}

SPARK_JOB_METRICS = {
    'numTasks': ('spark.job.num_tasks', COUNTER),
    'numActiveTasks': ('spark.job.num_active_tasks', GAUGE),
    'numCompletedTasks': ('spark.job.num_completed_tasks', COUNTER),
    'numSkippedTasks': ('spark.job.num_skipped_tasks', COUNTER),
    'numFailedTasks': ('spark.job.num_failed_tasks', COUNTER),
    'numActiveStages': ('spark.job.num_active_stages', GAUGE),
    'numCompletedStages': ('spark.job.num_completed_stages', COUNTER),
    'numSkippedStages': ('spark.job.num_skipped_stages', COUNTER),
    'numFailedStages': ('spark.job.num_failed_stages', COUNTER)
}

SPARK_STAGE_METRICS = {
    'numActiveTasks': ('spark.stage.num_active_tasks', GAUGE),
    'numCompleteTasks': ('spark.stage.num_complete_tasks', COUNTER),
    'numFailedTasks': ('spark.stage.num_failed_tasks', COUNTER),
    'executorRunTime': ('spark.stage.executor_run_time', COUNTER),
    'inputBytes': ('spark.stage.input_bytes', COUNTER),
    'inputRecords': ('spark.stage.input_records', COUNTER),
    'outputBytes': ('spark.stage.output_bytes', COUNTER),
    'outputRecords': ('spark.stage.output_records', COUNTER),
    'shuffleReadBytes': ('spark.stage.shuffle_read_bytes', COUNTER),
    'shuffleReadRecords': ('spark.stage.shuffle_read_records', COUNTER),
    'shuffleWriteBytes': ('spark.stage.shuffle_write_bytes', COUNTER),
    'shuffleWriteRecords': ('spark.stage.shuffle_write_records', COUNTER),
    'memoryBytesSpilled': ('spark.stage.memory_bytes_spilled', COUNTER),
    'diskBytesSpilled': ('spark.stage.disk_bytes_spilled', COUNTER)
}

SPARK_DRIVER_METRICS = {
    'rddBlocks': ('spark.driver.rdd_blocks', GAUGE),
    'memoryUsed': ('spark.driver.memory_used', GAUGE),
    'diskUsed': ('spark.driver.disk_used', GAUGE),
    'activeTasks': ('spark.driver.active_tasks', GAUGE),
    'failedTasks': ('spark.driver.failed_tasks', COUNTER),
    'completedTasks': ('spark.driver.completed_tasks', COUNTER),
    'totalTasks': ('spark.driver.total_tasks', COUNTER),
    'totalDuration': ('spark.driver.total_duration', COUNTER),
    'totalInputBytes': ('spark.driver.total_input_bytes', COUNTER),
    'totalShuffleRead': ('spark.driver.total_shuffle_read', COUNTER),
    'totalShuffleWrite': ('spark.driver.total_shuffle_write', COUNTER),
    'maxMemory': ('spark.driver.max_memory', GAUGE)
}

SPARK_EXECUTOR_METRICS = {
    'rddBlocks': ('spark.executor.rdd_blocks', GAUGE),
    'memoryUsed': ('spark.executor.memory_used', GAUGE),
    'diskUsed': ('spark.executor.disk_used', GAUGE),
    'activeTasks': ('spark.executor.active_tasks', GAUGE),
    'failedTasks': ('spark.executor.failed_tasks', COUNTER),
    'completedTasks': ('spark.executor.completed_tasks', COUNTER),
    'totalTasks': ('spark.executor.total_tasks', COUNTER),
    'totalDuration': ('spark.executor.total_duration', COUNTER),
    'totalInputBytes': ('spark.executor.total_input_bytes', COUNTER),
    'totalShuffleRead': ('spark.executor.total_shuffle_read', COUNTER),
    'totalShuffleWrite': ('spark.executor.total_shuffle_write', COUNTER),
    'maxMemory': ('spark.executor.max_memory', GAUGE)
}

SPARK_RDD_METRICS = {
    'numPartitions': ('spark.rdd.num_partitions', COUNTER),
    'numCachedPartitions': ('spark.rdd.num_cached_partitions', COUNTER),
    'memoryUsed': ('spark.rdd.memory_used', COUNTER),
    'diskUsed': ('spark.rdd.disk_used', COUNTER)
}

SPARK_STREAMING_METRICS = {
	"avgInputRate" : ("spark.streaming.avg_input_rate", GAUGE),
	"numTotalCompletedBatches" : ("spark.streaming.num_total_completed_batches", COUNTER),
	"numActiveBatches" : ("spark.streaming.num_active_batches", GAUGE),
	"numInactiveReceivers" : ("spark.streaming.num_inactive_receivers", GAUGE),
	"numReceivedRecords" : ("spark.streaming.num_received_records", COUNTER),
	"numProcessedRecords" : ("spark.streaming.num_processed_records", COUNTER),
	"avgProcessingTime" : ("spark.streaming.avg_processing_time", GAUGE),
	"avgSchedulingDelay" : ("spark.streaming.avg_scheduling_delay", GAUGE),
	"avgTotalDelay" : ("spark.streaming.avg_total_delay", GAUGE)
}