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

package org.apache.kylin.source.hive;

import java.util.List;

import com.google.common.collect.Lists;

public class HiveTableMetaBuilder {
    private String tableName;
    private String sdLocation;
    private String sdInputFormat;
    private String sdOutputFormat;
    private String owner;
    private String tableType;
    private int lastAccessTime;
    private long fileSize;
    private long fileNum;
    private int skipHeaderLineCount;
    private boolean isNative = true;
    private List<HiveTableMeta.HiveTableColumnMeta> allColumns = Lists.newArrayList();
    private List<HiveTableMeta.HiveTableColumnMeta> partitionColumns = Lists.newArrayList();

    public HiveTableMetaBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public HiveTableMetaBuilder setSdLocation(String sdLocation) {
        this.sdLocation = sdLocation;
        return this;
    }

    public HiveTableMetaBuilder setSdInputFormat(String sdInputFormat) {
        this.sdInputFormat = sdInputFormat;
        return this;
    }

    public HiveTableMetaBuilder setSdOutputFormat(String sdOutputFormat) {
        this.sdOutputFormat = sdOutputFormat;
        return this;
    }

    public HiveTableMetaBuilder setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public HiveTableMetaBuilder setTableType(String tableType) {
        this.tableType = tableType;
        return this;
    }

    public HiveTableMetaBuilder setLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
        return this;
    }

    public HiveTableMetaBuilder setFileSize(long fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public HiveTableMetaBuilder setFileNum(long fileNum) {
        this.fileNum = fileNum;
        return this;
    }

    public HiveTableMetaBuilder setSkipHeaderLineCount(String skipHeaderLineCount) {
        if (null == skipHeaderLineCount)
            this.skipHeaderLineCount = 0;
        else
            this.skipHeaderLineCount = Integer.parseInt(skipHeaderLineCount);
        return this;
    }

    public HiveTableMetaBuilder setIsNative(boolean isNative) {
        this.isNative = isNative;
        return this;
    }

    public HiveTableMetaBuilder setAllColumns(List<HiveTableMeta.HiveTableColumnMeta> allColumns) {
        this.allColumns = allColumns;
        return this;
    }

    public HiveTableMetaBuilder setPartitionColumns(List<HiveTableMeta.HiveTableColumnMeta> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
    }

    public HiveTableMeta createHiveTableMeta() {
        return new HiveTableMeta(tableName, sdLocation, sdInputFormat, sdOutputFormat, owner, tableType, lastAccessTime,
                fileSize, fileNum, skipHeaderLineCount, isNative, allColumns, partitionColumns);
    }
}
