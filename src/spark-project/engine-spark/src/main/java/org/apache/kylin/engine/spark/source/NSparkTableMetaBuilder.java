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

import com.google.common.collect.Lists;

public class NSparkTableMetaBuilder {
    private String tableName;
    private String sdLocation;
    private String sdInputFormat;
    private String sdOutputFormat;
    private String owner;
    private String provider;
    private String tableType;
    private String createTime;
    private String lastAccessTime;
    private long fileSize;
    private long fileNum;
    private boolean isNative = true;
    private List<NSparkTableMeta.SparkTableColumnMeta> allColumns = Lists.newArrayList();
    private List<NSparkTableMeta.SparkTableColumnMeta> partitionColumns = Lists.newArrayList();
    private boolean isTransactional = false;
    private boolean isRangePartition = false;
    private String s3Role;
    private String s3Endpoint;

    public NSparkTableMetaBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public NSparkTableMetaBuilder setSdLocation(String sdLocation) {
        this.sdLocation = sdLocation;
        return this;
    }

    public NSparkTableMetaBuilder setSdInputFormat(String sdInputFormat) {
        this.sdInputFormat = sdInputFormat;
        return this;
    }

    public NSparkTableMetaBuilder setSdOutputFormat(String sdOutputFormat) {
        this.sdOutputFormat = sdOutputFormat;
        return this;
    }

    public NSparkTableMetaBuilder setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public NSparkTableMetaBuilder setProvider(String provider) {
        this.provider = provider;
        return this;
    }

    public NSparkTableMetaBuilder setTableType(String tableType) {
        this.tableType = tableType;
        return this;
    }

    public NSparkTableMetaBuilder setCreateTime(String createTime) {
        this.createTime = createTime;
        return this;
    }

    public NSparkTableMetaBuilder setLastAccessTime(String lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    public NSparkTableMetaBuilder setFileSize(long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public NSparkTableMetaBuilder setFileNum(long fileNum) {
        this.fileNum = fileNum;
        return this;
    }

    public NSparkTableMetaBuilder setIsNative(boolean isNative) {
        this.isNative = isNative;
        return this;
    }

    public NSparkTableMetaBuilder setAllColumns(List<NSparkTableMeta.SparkTableColumnMeta> allColumns) {
        this.allColumns = allColumns;
        return this;
    }

    public NSparkTableMetaBuilder setPartitionColumns(List<NSparkTableMeta.SparkTableColumnMeta> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
    }

    public NSparkTableMetaBuilder setIsTransactional(boolean isTransactional) {
        this.isTransactional = isTransactional;
        return this;
    }

    public NSparkTableMetaBuilder setIsRangePartition(boolean isRangePartition) {
        this.isRangePartition = isRangePartition;
        return this;
    }

    public NSparkTableMetaBuilder setS3Role(String s3Role) {
        this.s3Role = s3Role;
        return this;
    }

    public NSparkTableMetaBuilder setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public NSparkTableMeta createSparkTableMeta() {
        return new NSparkTableMeta(tableName, sdLocation, sdInputFormat, sdOutputFormat, owner, provider, tableType,
                createTime, lastAccessTime, fileSize, fileNum, isNative, allColumns, partitionColumns, isTransactional,
                isRangePartition, s3Role, s3Endpoint);
    }
}