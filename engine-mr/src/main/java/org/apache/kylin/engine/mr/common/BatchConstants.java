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

package org.apache.kylin.engine.mr.common;

public interface BatchConstants {

    /**
     * source data config
     */
    char INTERMEDIATE_TABLE_ROW_DELIMITER = 127;


    String SEQUENCE_FILE_DEFAULT_DELIMITER = "\01";

    /**
     * ConFiGuration entry names for MR jobs
     */

    String CFG_UPDATE_SHARD = "update.shard";
    String CFG_CUBOID_MODE = "cuboid.mode";
    String CFG_CUBE_NAME = "cube.name";
    String CFG_CUBE_SEGMENT_NAME = "cube.segment.name";
    String CFG_CUBE_SEGMENT_ID = "cube.segment.id";
    String CFG_CUBE_CUBOID_LEVEL = "cube.cuboid.level";

    String CFG_II_NAME = "ii.name";
    String CFG_II_SEGMENT_NAME = "ii.segment.name";

    String CFG_OUTPUT_PATH = "output.path";
    String CFG_PROJECT_NAME = "project.name";
    String CFG_TABLE_NAME = "table.name";
    String CFG_IS_MERGE = "is.merge";
    String CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER = "cube.intermediate.table.row.delimiter";
    String CFG_REGION_NUMBER_MIN = "region.number.min";
    String CFG_REGION_NUMBER_MAX = "region.number.max";
    String CFG_REGION_SPLIT_SIZE = "region.split.size";
    String CFG_HFILE_SIZE_GB = "hfile.size.gb";
    String CFG_STATS_JOB_ID = "stats.job.id";
    String CFG_STATS_JOB_FREQUENCY = "stats.sample.frequency";

    String CFG_KYLIN_LOCAL_TEMP_DIR = "/tmp/kylin/";
    String CFG_KYLIN_HDFS_TEMP_DIR = "/tmp/kylin/";

    String CFG_STATISTICS_LOCAL_DIR = CFG_KYLIN_LOCAL_TEMP_DIR + "cuboidstatistics/";
    String CFG_STATISTICS_ENABLED = "statistics.enabled";
    String CFG_STATISTICS_OUTPUT = "statistics.ouput";//spell error, for compatibility issue better not change it
    String CFG_STATISTICS_SAMPLING_PERCENT = "statistics.sampling.percent";
    String CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME = "cuboid_statistics.seq";

    String CFG_MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";

    String CFG_OUTPUT_COLUMN = "column";
    String CFG_OUTPUT_DICT = "dict";
    String CFG_OUTPUT_STATISTICS = "statistics";
    String CFG_OUTPUT_PARTITION = "partition";
    String CFG_MR_SPARK_JOB = "mr.spark.job";
    String CFG_SPARK_META_URL = "spark.meta.url";
    String CFG_GLOBAL_DICT_BASE_DIR = "global.dict.base.dir";

    String CFG_HLL_REDUCER_NUM = "cuboidHLLCounterReducerNum";

    String CFG_SHARD_NUM = "shard.num";

    String CFG_CONVERGE_CUBOID_PARTITION_PARAM = "converge.cuboid.partition.param";

    /**
     * command line ARGuments
     */
    String ARG_INPUT = "input";
    String ARG_OUTPUT = "output";
    String ARG_PROJECT = "project";
    String ARG_CUBOID_MODE = "cuboidMode";
    String ARG_UPDATE_SHARD = "updateShard"; // indicate if need update base cuboid shard
    String ARG_JOB_NAME = "jobname";
    String ARG_CUBING_JOB_ID = "cubingJobId";
    String ARG_CUBE_NAME = "cubename";
    String ARG_II_NAME = "iiname";
    String ARG_SEGMENT_NAME = "segmentname";
    String ARG_SEGMENT_ID = "segmentid";
    String ARG_PARTITION = "partitions";
    String ARG_STATS_ENABLED = "statisticsenabled";
    String ARG_STATS_OUTPUT = "statisticsoutput";
    String ARG_STATS_SAMPLING_PERCENT = "statisticssamplingpercent";
    String ARG_HTABLE_NAME = "htablename";
    String ARG_INPUT_FORMAT = "inputformat";
    String ARG_LEVEL = "level";
    String ARG_CONF = "conf";
    String ARG_DICT_PATH = "dictPath";
    String ARG_TABLE_NAME = "tableName";
    String ARG_LOOKUP_SNAPSHOT_ID = "snapshotID";
    String ARG_EXT_LOOKUP_SNAPSHOTS_INFO = "extlookupsnapshots";
    String ARG_META_URL = "metadataUrl";
    String ARG_HBASE_CONF_PATH = "hbaseConfPath";
    String ARG_SHRUNKEN_DICT_PATH = "shrunkenDictPath";
    String ARG_COUNTER_OUTPUT = "counterOutput";
    String ARG_BASE64_ENCODED_STEP_NAME = "base64StepName";
    String ARG_SQL_COUNT = "sqlCount";
    String ARG_BASE64_ENCODED_SQL = "base64EncodedSql";

    /**
     * logger and counter
     */
    String MAPREDUCE_COUNTER_GROUP_NAME = "Cube Builder";
    int NORMAL_RECORD_LOG_THRESHOLD = 100000;

    /**
     * dictionaries builder class
     */
    String GLOBAL_DICTIONNARY_CLASS = "org.apache.kylin.dict.GlobalDictionaryBuilder";

    String LOOKUP_EXT_SNAPSHOT_SRC_RECORD_CNT_PFX = "lookup.ext.snapshot.src.record.cnt.";
}
