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

    char INTERMEDIATE_TABLE_ROW_DELIMITER = 127;

    String CFG_CUBE_NAME = "cube.name";
    String CFG_CUBE_SEGMENT_NAME = "cube.segment.name";

    String CFG_II_NAME = "ii.name";
    String CFG_II_SEGMENT_NAME = "ii.segment.name";

    String OUTPUT_PATH = "output.path";

    String TABLE_NAME = "table.name";
    String TABLE_COLUMNS = "table.columns";

    String CFG_IS_MERGE = "is.merge";
    String CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER = "cube.intermediate.table.row.delimiter";

    String MAPREDUCE_COUNTER_GROUP_NAME = "Cube Builder";

    String REGION_NUMBER_MIN = "region.number.min";
    String REGION_NUMBER_MAX = "region.number.max";
    String REGION_SPLIT_SIZE = "region.split.size";
    
    String CFG_KYLIN_LOCAL_TEMP_DIR = "/tmp/kylin/";
    String CFG_KYLIN_HDFS_TEMP_DIR = "/tmp/kylin/";
    
    String CFG_STATISTICS_LOCAL_DIR = CFG_KYLIN_LOCAL_TEMP_DIR + "cuboidstatistics/";
    String CFG_STATISTICS_ENABLED = "statistics.enabled";
    String CFG_STATISTICS_OUTPUT = "statistics.ouput";
    String CFG_STATISTICS_SAMPLING_PERCENT = "statistics.sampling.percent";
    String CFG_STATISTICS_CUBE_ESTIMATION = "cube_statistics.txt";
    String CFG_STATISTICS_CUBOID_ESTIMATION = "cuboid_statistics.seq";

    int COUNTER_MAX = 100000;
    int ERROR_RECORD_THRESHOLD = 100;
}
