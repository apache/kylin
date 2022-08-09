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

public class NSparkTableMeta {
    public static class SparkTableColumnMeta {
        String name;
        String dataType;
        String comment;

        public SparkTableColumnMeta(String name, String dataType, String comment) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
        }

        @Override
        public String toString() {
            return "SparkTableColumnMeta{" + "name='" + name + '\'' + ", dataType='" + dataType + '\'' + ", comment='"
                    + comment + '\'' + '}';
        }

        public String getName() {
            return name;
        }

        public String getDataType() {
            return dataType;
        }

        public String getComment() {
            return comment;
        }
    }

    String tableName;
    String sdLocation;//sd is short for storage descriptor
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
    List<SparkTableColumnMeta> allColumns;
    List<SparkTableColumnMeta> partitionColumns;
    boolean isTransactional;
    boolean isRangePartition;
    String s3Role;
    String s3Endpoint;

    public List<SparkTableColumnMeta> getAllColumns() {
        return allColumns;
    }

    public NSparkTableMeta(String tableName, String sdLocation, String sdInputFormat, String sdOutputFormat,
            String owner, String provider, String tableType, String createTime, String lastAccessTime, long fileSize,
            long fileNum, boolean isNative, List<SparkTableColumnMeta> allColumns,
            List<SparkTableColumnMeta> partitionColumns, boolean isTransactional, boolean isRangePartition,
            String s3Role, String s3Endpoint) {
        this.tableName = tableName;
        this.sdLocation = sdLocation;
        this.sdInputFormat = sdInputFormat;
        this.sdOutputFormat = sdOutputFormat;
        this.owner = owner;
        this.provider = provider;
        this.tableType = tableType;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.fileSize = fileSize;
        this.fileNum = fileNum;
        this.isNative = isNative;
        this.allColumns = allColumns;
        this.partitionColumns = partitionColumns;
        this.isTransactional = isTransactional;
        this.isRangePartition = isRangePartition;
        this.s3Role = s3Role;
        this.s3Endpoint = s3Endpoint;
    }

    @Override
    public String toString() {
        return "SparkTableMeta{" + "tableName='" + tableName + '\'' + ", sdLocation='" + sdLocation + '\''
                + ", sdInputFormat='" + sdInputFormat + '\'' + ", sdOutputFormat='" + sdOutputFormat + '\''
                + ", owner='" + owner + ", provider='" + provider + '\'' + ", tableType='" + tableType
                + ", createTime='" + createTime + '\'' + ", lastAccessTime=" + lastAccessTime + ", fileSize=" + fileSize
                + ", fileNum=" + fileNum + ", isNative=" + isNative + ", allColumns=" + allColumns
                + ", partitionColumns=" + partitionColumns + ", isTransactional=" + isTransactional
                + ", isRangePartition=" + isRangePartition + ", s3Role=" + s3Role + ", s3Endpoint=" + s3Endpoint + '}';
    }
}
