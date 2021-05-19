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

package org.apache.kylin.job.constant;

/**
 */
public final class ExecutableConstants {

    private ExecutableConstants() {
    }

    public static final String YARN_APP_STATE = "yarn_application_state";
    public static final String YARN_APP_ID = "yarn_application_id";
    public static final String YARN_APP_URL = "yarn_application_tracking_url";
    public static final String MR_JOB_ID = "mr_job_id";
    public static final String SPARK_JOB_ID = "spark_job_id";
    public static final String FLINK_JOB_ID = "flink_job_id";
    public static final String HDFS_BYTES_WRITTEN = "hdfs_bytes_written";
    public static final String SOURCE_RECORDS_COUNT = "source_records_count";
    public static final String SOURCE_RECORDS_SIZE = "source_records_size";
    public static final String SPARK_DIMENSION_DIC_SEGMENT_ID = "spark_dimension_dic_segment_id";

    public static final String STEP_NAME_EXTRACT_DICTIONARY_FROM_GLOBAL = "Extract Dictionary from Global Dictionary";
    public static final String STEP_NAME_BUILD_DICTIONARY = "Build Dimension Dictionary";
    public static final String STEP_NAME_BUILD_SPARK_DICTIONARY = "Build Dimension Dictionary with Spark";
    public static final String STEP_NAME_BUILD_UHC_DICTIONARY = "Build UHC Dictionary";
    public static final String STEP_NAME_BUILD_SPARK_UHC_DICTIONARY = "Build UHC Dictionary with spark";
    public static final String STEP_NAME_CREATE_FLAT_HIVE_TABLE = "Create Intermediate Flat Hive Table";
    public static final String STEP_NAME_CREATE_FLAT_TABLE_WITH_SPARK = "Create Intermediate Flat Table With Spark";
    public static final String STEP_NAME_SQOOP_TO_FLAT_HIVE_TABLE = "Sqoop To Flat Hive Table";
    public static final String STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP = "Materialize Hive View in Lookup Tables";
    public static final String STEP_NAME_FACT_DISTINCT_COLUMNS = "Extract Fact Table Distinct Columns";
    public static final String STEP_NAME_CALCULATE_STATS_FROM_BASE_CUBOID = "Calculate Statistics from Base Cuboid";
    public static final String STEP_NAME_FILTER_RECOMMEND_CUBOID_DATA_FOR_OPTIMIZATION = "Filter Recommend Cuboid Data for Optimization";
    public static final String STEP_NAME_UPDATE_OLD_CUBOID_SHARD = "Update Old Cuboid Shard for Optimization";
    public static final String STEP_NAME_BUILD_BASE_CUBOID = "Build Base Cuboid";
    public static final String STEP_NAME_BUILD_IN_MEM_CUBE = "Build Cube In-Mem";
    public static final String STEP_NAME_BUILD_SPARK_CUBE = "Build Cube with Spark";
    public static final String STEP_NAME_BUILD_FLINK_CUBE = "Build Cube with Flink";
    public static final String STEP_NAME_BUILD_N_D_CUBOID = "Build N-Dimension Cuboid";
    public static final String STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION = "Calculate HTable Region Splits";
    public static final String STEP_NAME_CREATE_HBASE_TABLE = "Create HTable";
    public static final String STEP_NAME_CONVERT_CUBOID_TO_HFILE = "Convert Cuboid Data to HFile";
    public static final String STEP_NAME_BULK_LOAD_HFILE = "Load HFile to HBase Table";
    public static final String STEP_NAME_COPY_DICTIONARY = "Copy dictionary from Old Segment";
    public static final String STEP_NAME_MERGE_DICTIONARY = "Merge Cuboid Dictionary";
    public static final String STEP_NAME_MERGE_STATISTICS = "Merge Cuboid Statistics";
    public static final String STEP_NAME_MERGE_UPDATE_DICTIONARY = "Update Dictionary Data";
    public static final String STEP_NAME_MERGE_STATISTICS_WITH_OLD = "Merge Cuboid Statistics with Old for Optimization";
    public static final String STEP_NAME_SAVE_STATISTICS = "Save Cuboid Statistics";
    public static final String STEP_NAME_MERGE_CUBOID = "Merge Cuboid Data";
    public static final String STEP_NAME_MERGE_CLEANUP = "Clean Up Old Segment for merging job";
    public static final String STEP_NAME_MERGER_SPARK_SEGMENT = "Merge Segment Data";
    public static final String STEP_NAME_UPDATE_CUBE_INFO = "Update Cube Info";
    public static final String STEP_NAME_HIVE_CLEANUP = "Hive Cleanup";
    public static final String STEP_NAME_KAFKA_CLEANUP = "Kafka Intermediate File Cleanup";
    public static final String STEP_NAME_GARBAGE_COLLECTION = "Garbage Collection";
    public static final String STEP_NAME_GARBAGE_COLLECTION_HBASE = "Garbage Collection on HBase";
    public static final String STEP_NAME_GARBAGE_COLLECTION_HDFS = "Garbage Collection on HDFS";
    public static final String STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE = "Redistribute Flat Hive Table";
    public static final String STEP_NAME_MATERIALIZE_LOOKUP_TABLE_CONVERT_HFILE = "Convert Lookup Table to HFile";
    public static final String STEP_NAME_MATERIALIZE_LOOKUP_TABLE_BULK_LOAD = "Load HFile to HBase Table";
    public static final String STEP_NAME_LOOKUP_SNAPSHOT_CACHE_UPDATE = "Update Lookup Snapshot Cache to Query Engine";
    public static final String STEP_NAME_MATERIALIZE_LOOKUP_TABLE_META_STORE = "Take Snapshot to Metadata Store";
    public static final String STEP_NAME_MATERIALIZE_LOOKUP_TABLE_UPDATE_CUBE = "Update Cube Info";

    public static final String SPARK_SPECIFIC_CONFIG_NAME_MERGE_DICTIONARY = "mergedict";

    public static final String STEP_NAME_STREAMING_CREATE_DICTIONARY = "Build Dimension Dictionaries For Steaming Job";
    public static final String STEP_NAME_STREAMING_BUILD_BASE_CUBOID = "Build Base Cuboid Data For Streaming Job";
    public static final String STEP_NAME_STREAMING_SAVE_DICTS = "Save Cube Dictionaries";

    // MR - Hive Dict
    public static final String STEP_NAME_GLOBAL_DICT_MRHIVE_EXTRACT_DICTVAL = "Build Global Dict - extract distinct value from data";
    public static final String STEP_NAME_GLOBAL_DICT_MRHIVE_BUILD_DICTVAL = "Build Global Dict - merge to dict table";
    public static final String STEP_NAME_GLOBAL_DICT_MRHIVE_REPLACE_DICTVAL = "Build Global Dict - replace intermediate table";

    public static final String FLINK_SPECIFIC_CONFIG_NAME_MERGE_DICTIONARY = "mergedict";
    //kylin on parquet v2
    public static final String STEP_NAME_DETECT_RESOURCE = "Detect Resource";
    public static final String STEP_NAME_BUILD_CUBOID_FROM_PARENT_CUBOID = "Build recommend cuboid from parent cuboid";

}
