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

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.List;

public class LayoutEntity implements IStorageAware {
    @JsonBackReference
    private IndexEntity indexEntity;

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

    private ImmutableBiMap<Integer, TblColRef> orderedDimensions;
    private ImmutableBiMap<Integer, DataModel.Measure> orderedMeasures;

    public ImmutableBiMap<Integer, TblColRef> getOrderedDimensions() { // dimension order abides by rowkey_col_desc
        if (orderedDimensions != null)
            return orderedDimensions;

        synchronized (this) {
            if (orderedDimensions != null)
                return orderedDimensions;

            ImmutableBiMap.Builder<Integer, TblColRef> dimsBuilder = ImmutableBiMap.builder();

            for (int colId : colOrder) {
                if (colId < DataModel.MEASURE_ID_BASE)
                    dimsBuilder.put(colId, indexEntity.getEffectiveDimCols().get(colId));
            }

            orderedDimensions = dimsBuilder.build();
            return orderedDimensions;
        }
    }

    public ImmutableBiMap<Integer, DataModel.Measure> getOrderedMeasures() { // measure order abides by column family
        if (orderedMeasures != null)
            return orderedMeasures;

        synchronized (this) {
            if (orderedMeasures != null)
                return orderedMeasures;

            ImmutableBiMap.Builder<Integer, DataModel.Measure> measureBuilder = ImmutableBiMap.builder();

            for (int colId : colOrder) {
                if (colId >= DataModel.MEASURE_ID_BASE)
                    measureBuilder.put(colId, indexEntity.getEffectiveMeasures().get(colId));
            }

            orderedMeasures = measureBuilder.build();
            return orderedMeasures;
        }
    }

    @Override
    public int getStorageType() {
        return this.storageType;
    }

    public IndexEntity getIndexEntity() {
        return indexEntity;
    }

    public void setIndexEntity(IndexEntity indexEntity) {
        this.indexEntity = indexEntity;
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

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<Integer> getShardByColumns() {
        return shardByColumns;
    }
}
