GAUGE = 'gauge'
COUNTER = 'counter'

SPARK_PROCESS_METRICS = {
    # jvm generics
    "jvm.total.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.total.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    # heap memory
    "jvm.heap.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.heap.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    # non-heap memory
    # if max = -1, this is technically unbounded
    # (switching calculation to used/committed)
    "jvm.non-heap.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.non-heap.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    # marksweep
    "jvm.MarkSweepCompact.count": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.MarkSweepCompact.time": (
        GAUGE,
        "gauges",
        "value"
    ),
    # worker specific
    "worker.coresFree": (
        GAUGE,
        "gauges",
        "value"
    ),
    "worker.coresUsed": (
        GAUGE,
        "gauges",
        "value"
    ),
    "worker.executors": (
        GAUGE,
        "gauges",
        "value"
    ),
    "worker.memFree_MB": (
        GAUGE,
        "gauges",
        "value"
    ),
    "worker.memUsed_MB": (
        GAUGE,
        "gauges",
        "value"
    ),
    # master specific
    "master.aliveWorkers": (
        GAUGE,
        "gauges",
        "value"
    ),
    "master.apps": (
        GAUGE,
        "gauges",
        "value"
    ),
    "master.waitingApps": (
        GAUGE,
        "gauges",
        "value"
    ),
    "master.workers": (
        GAUGE,
        "gauges",
        "value"
    )
}

SPARK_PROCESS_METRICS_ENHANCED = {
    # memory pool
    "jvm.pools.Code-Cache.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Code-Cache.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Compressed-Class-Space.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Compressed-Class-Space.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Metaspace.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Metaspace.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Eden-Space.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Eden-Space.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Survivor-Space.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Survivor-Space.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Tenured-Gen.used": (
        GAUGE,
        "gauges",
        "value"
    ),
    "jvm.pools.Tenured-Gen.committed": (
        GAUGE,
        "gauges",
        "value"
    ),
    # hive specifics
    "HiveExternalCatalog.fileCacheHits": (
        COUNTER,
        "counters",
        "count"
    ),
    "HiveExternalCatalog.filesDiscovered": (
        COUNTER,
        "counters",
        "count"
    ),
    "HiveExternalCatalog.hiveClientCalls": (
        COUNTER,
        "counters",
        "count"
    ),
    "HiveExternalCatalog.parallelListingJobCount": (
        COUNTER,
        "counters",
        "count"
    ),
    "HiveExternalCatalog.partitionsFetched": (
        COUNTER,
        "counters",
        "count"
    )
}

SPARK_JOB_METRICS = {
    "numTasks": (GAUGE, "spark.job.num_tasks"),
    "numActiveTasks": (GAUGE, "spark.job.num_active_tasks"),
    "numCompletedTasks": (GAUGE, "spark.job.num_completed_tasks"),
    "numSkippedTasks": (GAUGE, "spark.job.num_skipped_tasks"),
    "numFailedTasks": (GAUGE, "spark.job.num_failed_tasks"),
    "numActiveStages": (GAUGE, "spark.job.num_active_stages"),
    "numCompletedStages": (GAUGE, "spark.job.num_completed_stages"),
    "numSkippedStages": (GAUGE, "spark.job.num_skipped_stages"),
    "numFailedStages": (GAUGE, "spark.job.num_failed_stages")
}

SPARK_STAGE_METRICS = {
    "executorRunTime": (GAUGE, "spark.stage.executor_run_time"),
    "inputBytes": (GAUGE, "spark.stage.input_bytes"),
    "inputRecords": (GAUGE, "spark.stage.input_records"),
    "outputBytes": (GAUGE, "spark.stage.output_bytes"),
    "outputRecords": (GAUGE, "spark.stage.output_records"),
    "memoryBytesSpilled": (GAUGE, "spark.stage.memory_bytes_spilled"),
    "diskBytesSpilled": (GAUGE, "spark.stage.disk_bytes_spilled")
}

SPARK_STAGE_METRICS_ENHANCED = {
    "shuffleReadBytes": (GAUGE, "spark.stage.shuffle_read_bytes"),
    "shuffleReadRecords": (GAUGE, "spark.stage.shuffle_read_records"),
    "shuffleWriteBytes": (GAUGE, "spark.stage.shuffle_write_bytes"),
    "shuffleWriteRecords": (GAUGE, "spark.stage.shuffle_write_records"),
}

SPARK_DRIVER_METRICS = {
    "memoryUsed": (COUNTER, "spark.driver.memory_used"),
    "diskUsed": (COUNTER, "spark.driver.disk_used"),
    "totalInputBytes": (COUNTER, "spark.driver.total_input_bytes"),
    "totalShuffleRead": (COUNTER, "spark.driver.total_shuffle_read"),
    "totalShuffleWrite": (COUNTER, "spark.driver.total_shuffle_write"),
    "maxMemory": (GAUGE, "spark.driver.max_memory")
}

SPARK_DRIVER_METRICS_ENHANCED = {
    "rddBlocks": (GAUGE, "spark.driver.rdd_blocks"),
    "activeTasks": (GAUGE, "spark.driver.active_tasks"),
    "failedTasks": (COUNTER, "spark.driver.failed_tasks"),
    "completedTasks": (COUNTER, "spark.driver.completed_tasks"),
    "totalTasks": (COUNTER, "spark.driver.total_tasks"),
    "totalDuration": (COUNTER, "spark.driver.total_duration")
}

SPARK_EXECUTOR_METRICS = {
    "memoryUsed": (COUNTER, "spark.executor.memory_used"),
    "diskUsed": (COUNTER, "spark.executor.disk_used"),
    "totalInputBytes": (COUNTER, "spark.executor.total_input_bytes"),
    "totalShuffleRead": (COUNTER, "spark.executor.total_shuffle_read"),
    "totalShuffleWrite": (COUNTER, "spark.executor.total_shuffle_write"),
    "maxMemory": (GAUGE, "spark.executor.max_memory")
}

SPARK_EXECUTOR_METRICS_ENHANCED = {
    "rddBlocks": (GAUGE, "spark.executor.rdd_blocks"),
    "activeTasks": (GAUGE, "spark.executor.active_tasks"),
    "failedTasks": (COUNTER, "spark.executor.failed_tasks"),
    "completedTasks": (COUNTER, "spark.executor.completed_tasks"),
    "totalTasks": (COUNTER, "spark.executor.total_tasks"),
    "totalDuration": (COUNTER, "spark.executor.total_duration")
}

SPARK_STREAMING_METRICS = {
    "avgInputRate": (GAUGE, "spark.streaming.avg_input_rate"),
    "numTotalCompletedBatches":
    (COUNTER, "spark.streaming.num_total_completed_batches"),
    "numActiveBatches": (GAUGE, "spark.streaming.num_active_batches"),
    "numInactiveReceivers": (GAUGE, "spark.streaming.num_inactive_receivers"),
    "numReceivedRecords": (COUNTER, "spark.streaming.num_received_records"),
    "numProcessedRecords": (COUNTER, "spark.streaming.num_processed_records"),
    "avgProcessingTime": (GAUGE, "spark.streaming.avg_processing_time"),
    "avgSchedulingDelay": (GAUGE, "spark.streaming.avg_scheduling_delay"),
    "avgTotalDelay": (GAUGE, "spark.streaming.avg_total_delay")
}
