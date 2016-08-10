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

    /**
     * ConFiGuration entry names for MR jobs
     */

    String CFG_CUBE_NAME = "cube.name";
    String CFG_CUBE_SEGMENT_NAME = "cube.segment.name";
    String CFG_CUBE_SEGMENT_ID = "cube.segment.id";
    String CFG_CUBE_CUBOID_LEVEL = "cube.cuboid.level";

    String CFG_II_NAME = "ii.name";
    String CFG_II_SEGMENT_NAME = "ii.segment.name";

    String CFG_OUTPUT_PATH = "output.path";
    String CFG_TABLE_NAME = "table.name";
    String CFG_IS_MERGE = "is.merge";
    String CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER = "cube.intermediate.table.row.delimiter";
    String CFG_REGION_NUMBER_MIN = "region.number.min";
    String CFG_REGION_NUMBER_MAX = "region.number.max";
    String CFG_REGION_SPLIT_SIZE = "region.split.size";
    String CFG_HFILE_SIZE_GB = "hfile.size.gb";

    String CFG_KYLIN_LOCAL_TEMP_DIR = "/tmp/kylin/";
    String CFG_KYLIN_HDFS_TEMP_DIR = "/tmp/kylin/";

    String CFG_STATISTICS_LOCAL_DIR = CFG_KYLIN_LOCAL_TEMP_DIR + "cuboidstatistics/";
    String CFG_STATISTICS_ENABLED = "statistics.enabled";
    String CFG_STATISTICS_OUTPUT = "statistics.ouput";//spell error, for compatibility issue better not change it
    String CFG_STATISTICS_SAMPLING_PERCENT = "statistics.sampling.percent";
    String CFG_STATISTICS_CUBE_ESTIMATION_FILENAME = "cube_statistics.txt";
    String CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME = "cuboid_statistics.seq";

    /**
     * command line ARGuments
     */
    String ARG_INPUT = "input";
    String ARG_OUTPUT = "output";
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

    /**
     * logger and counter
     */
    String MAPREDUCE_COUNTER_GROUP_NAME = "Cube Builder";
    int NORMAL_RECORD_LOG_THRESHOLD = 100000;
    int ERROR_RECORD_LOG_THRESHOLD = 100;
}
