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

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.kylin.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.metadata.annotation.TableDefinition;

@TableDefinition
public class TablePlan extends RootPersistentEntity
        implements Serializable,
        HasLayoutElement<TableEntity>,
        IManagerAware<TablePlan> {

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

        public TablePlan build() {
            TablePlan result = new TablePlan();
            result.setUuid(model);
            result.setDescription(description);
            return result;
        }
    }
    public static Builder builder() {
        return new Builder();
    }

    protected transient Manager<TablePlan> manager;

    @Override
    public void setManager(Manager<TablePlan> manager) {
        this.manager = manager;
    }

    @Override
    public void verify() {
        // Here we check everything is ok
    }

    @JsonProperty("description")
    private String description;

    @JsonManagedReference
    @JsonProperty("table_metas")
    private final List<TableEntity> tableMetas = Lists.newArrayList();

    public List<TableEntity> getTableMetas() {
        return Collections.unmodifiableList(tableMetas);
    }

    @Override
    public List<TableEntity> all() {
        return tableMetas;
    }

    void addTable(TableEntity entity) {
        checkIsNotCachedAndShared();
        tableMetas.add(entity);
    }

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public void cleanTable() {
        checkIsNotCachedAndShared();
        this.tableMetas.clear();
    }

    public void cleanTable(Set<Long> layoutIds) {
        if (layoutIds== null || layoutIds.isEmpty()) {
            return;
        }

        checkIsNotCachedAndShared();
        this.tableMetas.removeIf(tableEntity -> layoutIds.contains(tableEntity.getLayoutID()));
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TablePlan)) return false;
        if (!super.equals(o)) return false;

        TablePlan tablePlan = (TablePlan) o;

        if (!Objects.equals(description, tablePlan.description))
            return false;
        return Objects.equals(tableMetas, tablePlan.tableMetas);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + tableMetas.hashCode();
        return result;
    }

    // update
    public TablePlan
    createTableEntityIfNotExists(LayoutEntity layoutEntity, boolean throwOnDifferentLayout){
        Preconditions.checkArgument(manager != null);
        if (containIndex(layoutEntity, throwOnDifferentLayout))
            return this;
        TableEntity entity = TableEntity.builder()
                .setLayoutEntity(layoutEntity)
                .build();
        return manager.update(uuid, copyForWrite -> copyForWrite.addTable(entity));
    }

    public TablePlan update(Consumer<TablePlan> updater) {
        Preconditions.checkArgument(manager != null);
        return manager.update(uuid, updater);
    }
}
