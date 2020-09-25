/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import java.util.Map;
import java.util.Objects;
import org.apache.kylin.engine.spark.metadata.ColumnDesc;
import org.apache.kylin.engine.spark.metadata.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;

import java.util.List;

public class LayoutEntity implements IStorageAware {

    public static LayoutEntity newLayoutEntity(long layoutId) {
        LayoutEntity layoutEntity = new LayoutEntity();
        layoutEntity.setId(layoutId);
        return layoutEntity;
    }

    @JsonProperty("id")
    private long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("col_order")
    private List<Integer> colOrder = Lists.newArrayList();

    @JsonProperty("update_time")
    private long updateTime;

    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_PARQUET;

    @JsonProperty("shard_by_columns")
    private List<Integer> shardByColumns = Lists.newArrayList();

    private Map<Integer, ColumnDesc> orderedDimensions;
    private Map<Integer, FunctionDesc> orderedMeasures;

    long rows;
    long sourceRows;
    long byteSize;
    long fileCount;
    int shardNum;

    @Override
    public int getStorageType() {
        return this.storageType;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public Map<Integer, ColumnDesc> getOrderedDimensions() { // dimension order abides by rowkey_col_desc
        return orderedDimensions;
    }

    public Map<Integer, FunctionDesc> getOrderedMeasures() { // measure order abides by column family
        return orderedMeasures;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<Integer> getShardByColumns() {
        return shardByColumns;
    }

    public void setShardByColumns(List<Integer> shardByColumns) {
        this.shardByColumns = shardByColumns;
    }

    public boolean fullyDerive(LayoutEntity child) {
       return orderedDimensions.keySet().containsAll(child.orderedDimensions.keySet()) &&
                orderedMeasures.keySet().containsAll(child.orderedMeasures.keySet()) ;
    }

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public int getShardNum() {
        return shardNum;
    }

    public void setShardNum(int shardNum) {
        this.shardNum = shardNum;
    }

    public void setOrderedDimensions(Map<Integer, ColumnDesc> orderedDimensions) {
        this.orderedDimensions = orderedDimensions;
    }

    public void setOrderedMeasures(Map<Integer, FunctionDesc> orderedMeasures) {
        this.orderedMeasures = orderedMeasures;
    }

    public boolean isTableIndex() {
        return orderedMeasures.isEmpty();
    }
}
