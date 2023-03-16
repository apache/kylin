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
package org.apache.kylin.metadata.model.schema;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.val;

@Data
@ToString
public class ReloadTableContext {

    private Map<String, AffectedModelContext> removeAffectedModels = Maps.newHashMap();

    private Map<String, AffectedModelContext> changeTypeAffectedModels = Maps.newHashMap();

    private Set<String> favoriteQueries = Sets.newHashSet();

    private Set<String> addColumns = Sets.newHashSet();

    private Set<String> removeColumns = Sets.newHashSet();

    private Set<String> changeTypeColumns = Sets.newHashSet();

    private Set<String> duplicatedColumns = Sets.newHashSet();

    private Set<String> effectedJobs = Sets.newHashSet();

    private TableDesc tableDesc;

    private TableExtDesc tableExtDesc;

    public AffectedModelContext getRemoveAffectedModel(String project, String modelId) {
        return removeAffectedModels.getOrDefault(modelId,
                new AffectedModelContext(project, modelId, Sets.newHashSet(), true));
    }

    public AffectedModelContext getChangeTypeAffectedModel(String project, String modelId) {
        return changeTypeAffectedModels.getOrDefault(modelId,
                new AffectedModelContext(project, modelId, Sets.newHashSet(), false));
    }

    @Getter(lazy = true)
    private final Set<String> removeColumnFullnames = initRemoveColumnFullNames();

    private Set<String> initRemoveColumnFullNames() {
        return removeColumns.stream().map(col -> {
            assert tableDesc != null;
            return tableDesc.getName() + "." + col;
        }).collect(Collectors.toSet());
    }

    public boolean isChanged(TableDesc originTableDesc) {
        if (tableDesc.getColumns().length != originTableDesc.getColumns().length) {
            return true;
        } else {
            for (int i = 0; i < tableDesc.getColumns().length; i++) {
                val newCol = tableDesc.getColumns()[i];
                val originCol = originTableDesc.getColumns()[i];
                if (!Objects.equals(newCol.getName(), originCol.getName())
                        || !Objects.equals(newCol.getDatatype(), originCol.getDatatype())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isOnlyAddCols() {
        return removeColumns.isEmpty() && changeTypeColumns.isEmpty();
    }

    public boolean isNeedProcess() {
        return CollectionUtils.isNotEmpty(addColumns) || CollectionUtils.isNotEmpty(removeColumns)
                || CollectionUtils.isNotEmpty(changeTypeColumns);
    }
}
