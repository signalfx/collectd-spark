GAUGE = 'gauge'
COUNTER = 'counter'

SPARK_PROCESS_METRICS = {
    # jvm generics
    "jvm.total.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.total.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    # heap memory
    "jvm.heap.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.heap.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    # non-heap memory
    # if max = -1, this is technically unbounded
    # (switching calculation to used/committed)
    "jvm.non-heap.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.non-heap.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    # marksweep
    "jvm.MarkSweepCompact.count": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.MarkSweepCompact.time": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    # memory pool
    "jvm.pools.Code-Cache.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Code-Cache.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Compressed-Class-Space.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Compressed-Class-Space.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Metaspace.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Metaspace.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Eden-Space.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Eden-Space.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Survivor-Space.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Survivor-Space.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Tenured-Gen.used": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "jvm.pools.Tenured-Gen.committed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    # hive specifics
    "HiveExternalCatalog.fileCacheHits": {
        "metric_type": COUNTER,
        "metric_type_category": "counters",
        "key": "count"
    },
    "HiveExternalCatalog.filesDiscovered": {
        "metric_type": COUNTER,
        "metric_type_category": "counters",
        "key": "count"
    },
    "HiveExternalCatalog.hiveClientCalls": {
        "metric_type": COUNTER,
        "metric_type_category": "counters",
        "key": "count"
    },
    "HiveExternalCatalog.parallelListingJobCount": {
        "metric_type": COUNTER,
        "metric_type_category": "counters",
        "key": "count"
    },
    "HiveExternalCatalog.partitionsFetched": {
        "metric_type": COUNTER,
        "metric_type_category": "counters",
        "key": "count"
    },
    # worker specific
    "worker.coresFree": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "worker.coresUsed": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "worker.executors": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "worker.memFree_MB": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "worker.memUsed_MB": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    # master specific
    "master.aliveWorkers": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "master.apps": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "master.waitingApps": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    },
    "master.workers": {
        "metric_type": GAUGE,
        "metric_type_category": "gauges",
        "key": "value"
    }
}

SPARK_JOB_METRICS = {
    'numTasks': ('spark.job.num_tasks', GAUGE),
    'numActiveTasks': ('spark.job.num_active_tasks', GAUGE),
    'numCompletedTasks': ('spark.job.num_completed_tasks', GAUGE),
    'numSkippedTasks': ('spark.job.num_skipped_tasks', GAUGE),
    'numFailedTasks': ('spark.job.num_failed_tasks', GAUGE),
    'numActiveStages': ('spark.job.num_active_stages', GAUGE),
    'numCompletedStages': ('spark.job.num_completed_stages', GAUGE),
    'numSkippedStages': ('spark.job.num_skipped_stages', GAUGE),
    'numFailedStages': ('spark.job.num_failed_stages', GAUGE)
}

SPARK_STAGE_METRICS = {
    'numActiveTasks': ('spark.stage.num_active_tasks', GAUGE),
    'numCompleteTasks': ('spark.stage.num_complete_tasks', GAUGE),
    'numFailedTasks': ('spark.stage.num_failed_tasks', GAUGE),
    'executorRunTime': ('spark.stage.executor_run_time', GAUGE),
    'inputBytes': ('spark.stage.input_bytes', GAUGE),
    'inputRecords': ('spark.stage.input_records', GAUGE),
    'outputBytes': ('spark.stage.output_bytes', GAUGE),
    'outputRecords': ('spark.stage.output_records', GAUGE),
    'shuffleReadBytes': ('spark.stage.shuffle_read_bytes', GAUGE),
    'shuffleReadRecords': ('spark.stage.shuffle_read_records', GAUGE),
    'shuffleWriteBytes': ('spark.stage.shuffle_write_bytes', GAUGE),
    'shuffleWriteRecords': ('spark.stage.shuffle_write_records', GAUGE),
    'memoryBytesSpilled': ('spark.stage.memory_bytes_spilled', GAUGE),
    'diskBytesSpilled': ('spark.stage.disk_bytes_spilled', GAUGE)
}

SPARK_DRIVER_METRICS = {
    'rddBlocks': ('spark.driver.rdd_blocks', GAUGE),
    'memoryUsed': ('spark.driver.memory_used', COUNTER),
    'diskUsed': ('spark.driver.disk_used', COUNTER),
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
    'memoryUsed': ('spark.executor.memory_used', COUNTER),
    'diskUsed': ('spark.executor.disk_used', COUNTER),
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

SPARK_STREAMING_METRICS = {
    "avgInputRate": ("spark.streaming.avg_input_rate", GAUGE),
    "numTotalCompletedBatches":
    ("spark.streaming.num_total_completed_batches", COUNTER),
    "numActiveBatches": ("spark.streaming.num_active_batches", GAUGE),
    "numInactiveReceivers": ("spark.streaming.num_inactive_receivers", GAUGE),
    "numReceivedRecords": ("spark.streaming.num_received_records", COUNTER),
    "numProcessedRecords": ("spark.streaming.num_processed_records", COUNTER),
    "avgProcessingTime": ("spark.streaming.avg_processing_time", GAUGE),
    "avgSchedulingDelay": ("spark.streaming.avg_scheduling_delay", GAUGE),
    "avgTotalDelay": ("spark.streaming.avg_total_delay", GAUGE)
}
