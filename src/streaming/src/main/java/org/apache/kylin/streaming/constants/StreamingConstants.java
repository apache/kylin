/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.streaming.constants;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.HDFSMetadataStore;

public class StreamingConstants {

    // spark job conf
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_MASTER_DEFAULT = "yarn";
    public static final String SPARK_DRIVER_MEM = "spark.driver.memory";
    public static final String SPARK_DRIVER_MEM_DEFAULT = "512m";
    public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";
    public static final String SPARK_EXECUTOR_INSTANCES_DEFAULT = "2";
    public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
    public static final String SPARK_CORES_MAX = "spark.cores.max";
    public static final String SPARK_EXECUTOR_CORES_DEFAULT = "2";
    public static final String SPARK_EXECUTOR_MEM = "spark.executor.memory";
    public static final String SPARK_EXECUTOR_MEM_DEFAULT = "1g";
    public static final String SPARK_DRIVER_OVERHEAD = "spark.driver.memoryOverhead";
    public static final String SPARK_DRIVER_OVERHEAD_DEFAULT = "1g";

    public static final String SPARK_YARN_DIST_JARS = "spark.yarn.dist.jars";
    public static final String SPARK_DRIVER_OPTS = "spark.driver.extraJavaOptions";
    public static final String SPARK_EXECUTOR_OPTS = "spark.executor.extraJavaOptions";
    public static final String SPARK_YARN_AM_OPTS = "spark.yarn.am.extraJavaOptions";
    public static final String SPARK_YARN_TIMELINE_SERVICE = "spark.hadoop.yarn.timeline-service.enabled";
    public static final String SPARK_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
    public static final String SPARK_SHUFFLE_PARTITIONS_DEFAULT = "8";

    //rest server
    public static final String REST_SERVER_IP = "kylin.spark.rest.server.ip";

    // main class
    public static final String SPARK_STREAMING_ENTRY = "org.apache.kylin.streaming.app.StreamingEntry";
    public static final String SPARK_STREAMING_MERGE_ENTRY = "org.apache.kylin.streaming.app.StreamingMergeEntry";

    // hadoop conf
    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    public static final String SLASH = "/";

    // streaming job
    public static final String STREAMING_META_URL = "kylin.streaming.meta-scheme";
    public static final String STREAMING_META_URL_DEFAULT = HDFSMetadataStore.HDFS_SCHEME;
    public static final String STREAMING_DURATION = "kylin.streaming.duration";
    public static final String STREAMING_CONFIG_PREFIX = "kylin.streaming.spark-conf.";
    public static final String STREAMING_DURATION_DEFAULT = "30";
    public static final String FILE_LAYER = "file_layer";
    public static final String ACTION_START = "START";
    public static final String ACTION_GRACEFUL_SHUTDOWN = "GRACEFUL_SHUTDOWN";

    // watermark
    public static final String STREAMING_WATERMARK = "kylin.streaming.watermark";
    public static final String STREAMING_WATERMARK_DEFAULT = KylinConfig.getInstanceFromEnv()
            .getStreamingJobWatermark();

    // kafka conf
    public static final String STREAMING_KAFKA_CONFIG_PREFIX = "kylin.streaming.kafka-conf.";
    public static final String STREAMING_MAX_OFFSETS_PER_TRIGGER = "kylin.streaming.kafka-conf.maxOffsetsPerTrigger";
    public static final String STREAMING_MAX_OFFSETS_PER_TRIGGER_DEFAULT = KylinConfig.getInstanceFromEnv()
            .getKafkaMaxOffsetsPerTrigger();
    public static final String STREAMING_KAFKA_STARTING_OFFSETS = "kylin.streaming.kafka-conf.startingOffsets";

    // merge job
    public static final String STREAMING_SEGMENT_MAX_SIZE = "kylin.streaming.segment-max-size";
    public static final String STREAMING_SEGMENT_MAX_SIZE_DEFAULT = "32m";
    public static final String STREAMING_SEGMENT_MERGE_THRESHOLD = "kylin.streaming.segment-merge-threshold";
    public static final String STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT = "3";

    // retry
    public static final String STREAMING_RETRY_ENABLE = "kylin.streaming.job-retry-enabled";

    // dimension table refresh conf
    public static final String STREAMING_TABLE_REFRESH_INTERVAL = "kylin.streaming.table-refresh-interval";

}
