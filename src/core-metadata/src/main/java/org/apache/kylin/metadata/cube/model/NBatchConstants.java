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

package org.apache.kylin.metadata.cube.model;

public interface NBatchConstants {
    String P_DATAFLOW_ID = "dataflowId";
    String P_SEGMENT_IDS = "segmentIds";
    String P_JOB_ID = "jobId";
    String P_JOB_TYPE = "jobType";
    String P_LAYOUT_IDS = "layoutIds";
    String P_TO_BE_DELETED_LAYOUT_IDS = "toBeDeletedLayoutIds";
    String P_CLASS_NAME = "className";
    String P_JARS = "jars";
    String P_DIST_META_URL = "distMetaUrl";
    String P_OUTPUT_META_URL = "outputMetaUrl";
    String P_PROJECT_NAME = "project";
    String P_TABLE_NAME = "table";
    String P_SAMPLING_ROWS = "samplingRows";
    String P_TARGET_MODEL = "targetModel";
    String P_DATA_RANGE_START = "dataRangeStart";
    String P_DATA_RANGE_END = "dataRangeEnd";

    String P_IGNORED_SNAPSHOT_TABLES = "ignoredSnapshotTables";
    String P_NEED_BUILD_SNAPSHOTS = "needBuildSnapshots";
    String P_PARTITION_IDS = "partitionIds";
    String P_BUCKETS = "buckets";

    String P_INCREMENTAL_BUILD = "incrementalBuild";
    String P_SELECTED_PARTITION_COL = "selectedPartitionCol";
    String P_SELECTED_PARTITION_VALUE = "selectedPartition";
    String P_PARTIAL_BUILD = "partialBuild";

    String P_QUERY_ID = "queryId";
    String P_QUERY_PARAMS = "queryParams";
    String P_QUERY_CONTEXT = "queryContext";
    String P_QUERY_QUEUE = "queryQueue";

    /** use for stage calculate exec ratio */
    String P_INDEX_COUNT = "indexCount";
    String P_INDEX_SUCCESS_COUNT = "indexSuccessCount";
    /** value like : { "segmentId1": 1223, "segmentId2": 1223 } */
    String P_WAITE_TIME = "waiteTime";

    // ut only
    String P_BREAK_POINT_LAYOUTS = "breakPointLayouts";
}
