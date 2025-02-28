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
package org.apache.kylin.engine.spark.source;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@ToString
public class NSparkTableMeta {

    @AllArgsConstructor
    @Getter
    @ToString
    public static class SparkTableColumnMeta {
        String name;
        String dataType;
        String comment;
    }

    String tableName;
    String sdLocation; // sd is short for storage descriptor
    String sdInputFormat;
    String sdOutputFormat;
    String owner;
    String provider;
    String tableType;
    String createTime;
    String lastAccessTime;
    long fileSize;
    long fileNum;
    boolean isNative;
    @Getter
    List<SparkTableColumnMeta> allColumns;
    List<SparkTableColumnMeta> partitionColumns;
    boolean isTransactional;
    boolean isRangePartition;

    String roleArn;
    String endpoint;
    String region;

    String tableComment;

    @Setter
    int sourceType;
}
