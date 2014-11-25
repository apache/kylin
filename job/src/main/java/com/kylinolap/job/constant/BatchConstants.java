/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.constant;

/**
 * @author George Song (ysong1)
 * 
 */
public interface BatchConstants {

    public static final char INTERMEDIATE_TABLE_ROW_DELIMITER = 127;

    public static final String CFG_CUBE_NAME = "cube.name";
    public static final String CFG_CUBE_SEGMENT_NAME = "cube.segment.name";

    public static final String INPUT_DELIM = "input.delim";

    public static final String OUTPUT_PATH = "output.path";

    public static final String TABLE_NAME = "table.name";
    public static final String TABLE_COLUMNS = "table.columns";

    public static final String CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER = "cube.intermediate.table.row.delimiter";

    public static final String MAPREDUCE_COUTNER_GROUP_NAME = "Cube Builder";

    public static final String MAPPER_SAMPLE_NUMBER = "mapper.sample.number";
    public static final String REGION_NUMBER = "region.number";
    public static final String CUBE_CAPACITY = "cube.capacity";

    public static final int COUNTER_MAX = 100000;
}
