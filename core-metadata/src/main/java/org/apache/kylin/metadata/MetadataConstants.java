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

package org.apache.kylin.metadata;

/**
 * Constances to describe metadata and it's change.
 * 
 */
public interface MetadataConstants {

    String FILE_SURFIX = ".json";
    //Identifier Type, user or group
    String TYPE_USER = "user";
    String TYPE_GROUP = "group";

    // Extended attribute keys
    String TABLE_EXD_STATUS_KEY = "EXD_STATUS";
    String TABLE_EXD_MINFS = "minFileSize";
    String TABLE_EXD_TNF = "totalNumberFiles";
    String TABLE_EXD_LOCATION = "location";
    String TABLE_EXD_LUT = "lastUpdateTime";
    String TABLE_EXD_LAT = "lastAccessTime";
    String TABLE_EXD_COLUMN = "columns";
    String TABLE_EXD_PC = "partitionColumns";
    String TABLE_EXD_MAXFS = "maxFileSize";
    String TABLE_EXD_IF = "inputformat";
    String TABLE_EXD_PARTITIONED = "partitioned";
    String TABLE_EXD_TABLENAME = "tableName";
    String TABLE_EXD_OWNER = "owner";
    String TABLE_EXD_TFS = "totalFileSize";
    String TABLE_EXD_OF = "outputformat";
    /**
     * The value is an array
     */
    String TABLE_EXD_CARDINALITY = "cardinality";
    String TABLE_EXD_DELIM = "delim";
    String TABLE_EXD_DEFAULT_VALUE = "unknown";

    String KYLIN_INTERMEDIATE_PREFIX = "kylin_intermediate_";

    //kylin on parquetv2
    String P_CUBE_ID = "cubeId";
    String P_CUBE_NAME = "cubeName";
    String P_CUBOID_AGG_UDF = "newtenCuboidAggUDF";
    String P_SEGMENT_IDS = "segmentIds";
    String P_JOB_ID = "jobId";
    String P_JOB_TYPE = "jobType";
    String P_LAYOUT_IDS = "layoutIds";
    String P_LAYOUT_ID_PATH = "layoutIdPath";
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
    String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";

}
