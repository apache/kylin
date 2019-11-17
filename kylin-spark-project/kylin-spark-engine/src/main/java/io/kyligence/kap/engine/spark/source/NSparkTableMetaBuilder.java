/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.source;

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

    public NSparkTableMeta createSparkTableMeta() {
        return new NSparkTableMeta(tableName, sdLocation, sdInputFormat, sdOutputFormat, owner, provider, tableType, createTime,
                lastAccessTime, fileSize, fileNum, isNative, allColumns, partitionColumns);
    }
}