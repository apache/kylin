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
package io.kyligence.kap.secondstorage.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.cube.model.LayoutEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.secondstorage.metadata.annotation.TableDefinition;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@TableDefinition
public class TableEntity implements Serializable, WithLayout {

    public static final int DEFAULT_SHARD = 100;

    public static final class Builder {

        private LayoutEntity layoutEntity;
        private List<Integer> primaryIndexColumns = Lists.newArrayList();
        private Set<Integer> secondaryIndexColumns = Sets.newHashSet();

        public Builder setLayoutEntity(LayoutEntity layoutEntity) {
            this.layoutEntity = layoutEntity;
            return this;
        }

        public Builder setPrimaryIndexColumns(List<Integer> primaryIndexColumns) {
            this.primaryIndexColumns = primaryIndexColumns;
            return this;
        }

        public Builder setSecondaryIndexColumns(Set<Integer> secondaryIndexColumns) {
            this.secondaryIndexColumns = secondaryIndexColumns;
            return this;
        }

        public TableEntity build() {
            TableEntity table = new TableEntity();
            table.layoutID = layoutEntity.getId();
            if (primaryIndexColumns != null) {
                table.primaryIndexColumns = primaryIndexColumns;
            }
            if (secondaryIndexColumns != null) {
                table.secondaryIndexColumns = secondaryIndexColumns;
            }
            return table;
        }
    }
    public static Builder builder() {
        return new Builder();
    }

    @JsonBackReference
    private TablePlan tablePlan;

    @JsonProperty("layout_id")
    private long layoutID;

    @JsonProperty("shard_numbers")
    private int shardNumbers = DEFAULT_SHARD;

    @JsonProperty("primary_index_columns")
    private List<Integer> primaryIndexColumns = Lists.newArrayList();

    @JsonProperty("primary_index_last_modified")
    private long primaryIndexLastModified;

    @JsonProperty("secondary_index_columns")
    private Set<Integer> secondaryIndexColumns = Sets.newHashSet();

    @JsonProperty("secondary_index_last_modified")
    private long secondaryIndexLastModified;

    public void checkIsNotCachedAndShared() {
        if (tablePlan != null)
            tablePlan.checkIsNotCachedAndShared();
    }

    public void setShardNumbers(int shardNumbers) {
        checkIsNotCachedAndShared();
        this.shardNumbers = shardNumbers;
    }

    public void setPrimaryIndexColumns(List<Integer> primaryIndexColumns) {
        checkIsNotCachedAndShared();
        this.primaryIndexColumns = primaryIndexColumns;
        this.primaryIndexLastModified = System.currentTimeMillis();
    }

    public void setSecondaryIndexColumns(Set<Integer> secondaryIndexColumns) {
        checkIsNotCachedAndShared();
        this.secondaryIndexColumns = secondaryIndexColumns;
        this.secondaryIndexLastModified = System.currentTimeMillis();
    }

    public TablePlan getTablePlan() {
        return tablePlan;
    }

    public long getLayoutID() {
        return layoutID;
    }

    public int getShardNumbers() {
        return shardNumbers;
    }

    public List<Integer> getPrimaryIndexColumns() {
        return this.primaryIndexColumns;
    }

    public Set<Integer> getSecondaryIndexColumns() {
        return this.secondaryIndexColumns;
    }

    public long getPrimaryIndexLastModified() {
        return this.primaryIndexLastModified;
    }

    public long getSecondaryIndexLastModified() {
        return this.secondaryIndexLastModified;
    }
}
