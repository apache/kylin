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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;

// CALL FROM CORE
@DataDefinition
public class TableFlow extends RootPersistentEntity
        implements Serializable,
        HasLayoutElement<TableData>,
        IManagerAware<TableFlow> {

    public static final class Builder {
        private String model;

        public Builder setModel(String model) {
            this.model = model;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        private String description;

        public TableFlow build() {
            TableFlow result = new TableFlow();
            result.setUuid(model);
            result.setDescription(description);
            return result;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final String TABLEFLOW_RESOURCE_ROOT = "/clickhouse_data";

    protected transient Manager<TableFlow> manager;
    @Override
    public void setManager(Manager<TableFlow> manager) {
        this.manager = manager;
    }

    @Override
    public void verify() {
        // Here we check everything is ok
    }

    @JsonProperty("description")
    private String description;

    @JsonManagedReference
    @JsonProperty("data_list")
    private final List<TableData> tableDataList = Lists.newArrayList();

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public List<TableData> getTableDataList() {
        return Collections.unmodifiableList(tableDataList);
    }

    @Override
    public List<TableData> all() {
        return tableDataList;
    }

    public void upsertTableData(LayoutEntity layoutEntity, Consumer<TableData> updater, Supplier<TableData> creator) {
        checkIsNotCachedAndShared();
        TableData data = getEntity(layoutEntity)
                .map(tableData -> {
                    Preconditions.checkArgument(containIndex(layoutEntity, true));
                    updater.accept(tableData);
                    return tableData;})
                .orElseGet(() -> {
                    TableData newData = creator.get();
                    tableDataList.add(newData);
                    updater.accept(newData);
                    return newData;});
        Preconditions.checkArgument(HasLayoutElement.sameLayout(data, layoutEntity));
    }

    public void cleanTableData(Predicate<? super TableData> filter) {
        if (filter == null) {
            return;
        }

        checkIsNotCachedAndShared();
        this.tableDataList.removeIf(filter);
    }

    public void cleanTableData() {
        checkIsNotCachedAndShared();
        this.tableDataList.clear();
    }

    public void removeNodes(List<String> nodeNames) {
        if (CollectionUtils.isEmpty(nodeNames)) {
            return;
        }

        checkIsNotCachedAndShared();
        this.tableDataList.forEach(tableData -> tableData.removeNodes(nodeNames));
    }

    public Set<Long> getLayoutBySegment(String segmentId) {
        return getTableDataList().stream()
                .filter(tableData -> tableData.containSegments(Collections.singleton(segmentId)))
                .map(TableData::getLayoutID)
                .collect(Collectors.toSet());
    }

    public boolean containsLayout(long layoutId) {
        return getTableDataList().stream()
                .anyMatch(tableData -> tableData.getLayoutID() == layoutId);
    }

    public List<TableData> getTableData(long layoutId) {
        return getTableDataList().stream().filter(tableData -> tableData.getLayoutID() == layoutId)
                .collect(Collectors.toList());
    }

    public void updateSecondaryIndex(long layoutId, Set<Integer> addColumns, Set<Integer> removeColumns) {
        this.tableDataList.forEach(tableData -> {
            if (tableData.getLayoutID() == layoutId) {
                tableData.updateSecondaryIndex(addColumns, removeColumns);
            }
        });
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), manager.project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(TABLEFLOW_RESOURCE_ROOT)
                .append("/").append(name).append(MetadataConstants.FILE_SURFIX).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableFlow)) return false;
        if (!super.equals(o)) return false;

        TableFlow tableFlow = (TableFlow) o;

        if (!Objects.equals(description, tableFlow.description))
            return false;
        return Objects.equals(tableDataList, tableFlow.tableDataList);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + tableDataList.hashCode();
        return result;
    }

    public TableFlow update(Consumer<TableFlow> updater) {
        Preconditions.checkArgument(manager != null);
        return manager.update(uuid, updater);
    }
}
