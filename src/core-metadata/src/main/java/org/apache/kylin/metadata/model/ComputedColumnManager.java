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

package org.apache.kylin.metadata.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.Manager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputedColumnManager extends Manager<ComputedColumnDesc> {
    private static final Logger logger = LoggerFactory.getLogger(ComputedColumnManager.class);

    protected ComputedColumnManager(KylinConfig cfg, String project, MetadataType type) {
        super(cfg, project, type);
    }

    // called by reflection
    static ComputedColumnManager newInstance(KylinConfig config, String project) {
        return new ComputedColumnManager(config, project, MetadataType.COMPUTE_COLUMN);
    }

    public static ComputedColumnManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, ComputedColumnManager.class);
    }

    @Override
    public Logger logger() {
        return logger;
    }

    @Override
    public String name() {
        return "ComputedColumnManager";
    }

    @Override
    public Class<ComputedColumnDesc> entityType() {
        return ComputedColumnDesc.class;
    }

    public ComputedColumnDesc saveCCWithCheck(ComputedColumnDesc entity) {
        ComputedColumnDesc existing = getByName(entity.getTableIdentity(), entity.getColumnName());
        if (existing == null) {
            existing = get(entity.resourceName()).orElse(null);
        }
        if (existing != null) {
            if (noNeedToUpdate(existing, entity)) {
                return existing;
            }
            return super.update(existing.getUuid(), copyForWrite -> {
                copyForWrite.setColumnName(entity.getColumnName());
                copyForWrite.setDatatype(entity.getDatatype());
                copyForWrite.setExpression(entity.getExpression());
                copyForWrite.setInnerExpression(entity.getInnerExpression());
                copyForWrite.setTableAlias(entity.getTableAlias());
                copyForWrite.setTableIdentity(entity.getTableIdentity());
                copyForWrite.setComment(entity.getComment());
            });
        }

        // reset mvcc and uuid for new cc
        ComputedColumnDesc copied = this.copy(entity);
        copied.setUuid(RandomUtil.randomUUIDStr());
        copied.setMvcc(-1);
        return super.createAS(copied);
    }

    private boolean noNeedToUpdate(ComputedColumnDesc existing, ComputedColumnDesc entity) {
        return Objects.equals(existing.getDatatype(), entity.getDatatype())
                && Objects.equals(existing.getTableAlias(), entity.getTableAlias())
                && Objects.equals(existing.getTableIdentity(), entity.getTableIdentity())
                && Objects.equals(existing.getInnerExpression(), entity.getInnerExpression())
                && Objects.equals(existing.getExpression(), entity.getExpression())
                && Objects.equals(existing.getComment(), entity.getComment())
                && Objects.equals(existing.getColumnName(), entity.getColumnName());
    }

    public ComputedColumnDesc getByName(String tableIdentity, String columnName) {
        List<ComputedColumnDesc> result = listByFilter(
                RawResourceFilter.equalFilter("columnName", columnName).addConditions("tableIdentity",
                        Collections.singletonList(tableIdentity), RawResourceFilter.Operator.EQUAL));
        Preconditions.checkState(result.size() <= 1,
                "Exist more than one cc with same name: " + tableIdentity + "." + columnName);
        return result.isEmpty() ? null : result.get(0);
    }

    @Override
    public ComputedColumnDesc createAS(ComputedColumnDesc entity) {
        throw new UnsupportedOperationException("Please use saveCCWithCheck.");
    }

    @Override
    public ComputedColumnDesc update(String resourceName, Consumer<ComputedColumnDesc> updater) {
        throw new UnsupportedOperationException("Please use saveCCWithCheck.");
    }

    @Override
    public ComputedColumnDesc upsert(String resourceName, Consumer<ComputedColumnDesc> updater,
            Supplier<ComputedColumnDesc> creator) {
        throw new UnsupportedOperationException("Please use saveCCWithCheck.");
    }
}
