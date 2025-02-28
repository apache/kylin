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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
public class ColExcludedChecker {

    @Getter
    private final Set<ColumnDesc> excludedCols = Sets.newHashSet();
    @Getter
    private final Set<String> excludedColNames = Sets.newHashSet();
    private final Map<String, TblColRef> colIdentityMap = Maps.newHashMap();

    public ColExcludedChecker(KylinConfig config, String project, NDataModel model) {
        ProjectInstance prjInstance = NProjectManager.getInstance(config).getProject(project);
        if (!prjInstance.getConfig().isTableExclusionEnabled()) {
            return;
        }
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, project);
        List<TableDesc> allTables = tableMgr.listAllTables();
        Map<String, TableRef> identityToRefMap = Maps.newHashMap();
        if (model != null && !model.isBroken()) {
            for (JoinTableDesc joinTableDesc : model.getJoinTables()) {
                TableRef tableRef = joinTableDesc.getTableRef();
                identityToRefMap.put(tableRef.getTableIdentity(), tableRef);
            }
        }
        Set<TableDesc> desiredTables = allTables.stream()
                .filter(table -> model == null || identityToRefMap.containsKey(table.getIdentity()))
                .collect(Collectors.toSet());
        for (TableDesc table : desiredTables) {
            TableExtDesc tableExt = tableMgr.getTableExtIfExists(table);
            String factTable = model == null ? "" : model.getRootFactTableName();
            if (StringUtils.equalsIgnoreCase(table.getIdentity(), factTable) || tableExt == null) {
                continue;
            }

            Set<String> excludedSet = tableExt.getExcludedColumns();
            List<ColumnDesc> list = Arrays.stream(table.getColumns()).filter(Objects::nonNull)
                    .filter(col -> tableExt.isExcluded() || excludedSet.contains(col.getName()))
                    .collect(Collectors.toList());
            excludedCols.addAll(list);
        }

        // add excluded column from cc
        collectExcludedComputedColumns(config, project, model);
    }

    private void collectExcludedComputedColumns(KylinConfig config, String project, NDataModel originModel) {
        if (originModel == null || originModel.isBroken()) {
            return;
        }
        NDataModel model = originModel;
        if (originModel.getAllTables().isEmpty() || originModel.getEffectiveCols() == null) {
            model = NDataModelManager.getInstance(config, project).copyForWrite(originModel);
            model.init(config, project, Lists.newArrayList());
        }
        model.getAllTables().stream().filter(Objects::nonNull) //
                .flatMap(tableRef -> tableRef.getColumns().stream())
                .filter(tblColRef -> excludedCols.contains(tblColRef.getColumnDesc()))
                .map(TblColRef::getBackTickIdentity).forEach(excludedColNames::add);
        model.getEffectiveCols().forEach((id, colRef) -> colIdentityMap.put(colRef.getIdentity(), colRef));
        model.getComputedColumnDescs().forEach(cc -> {
            TblColRef tblColRef = colIdentityMap.get(cc.getFullName());
            if (tblColRef == null) {
                return;
            }
            ColumnDesc columnDesc = tblColRef.getColumnDesc();
            if (isExcludedCC(cc.getInnerExpression())) {
                excludedCols.add(columnDesc);
                excludedColNames.add(columnDesc.getBackTickIdentity());
            }
        });
    }

    /**
     * This method gives all the excluded columns without considering columns of computed columns.
     * It is useful when using org.apache.kylin.query.util.ConvertToComputedColumn to transform query statement.
     * If ConvertToComputedColumn is removed, this method is useless.
     */
    public Set<String> filterRelatedExcludedColumn(NDataModel model) {
        String fact = model == null ? "" : model.getRootFactTableName();
        return getExcludedCols().stream() //
                .filter(col -> !col.getTable().getIdentity().equalsIgnoreCase(fact)) //
                .map(ColumnDesc::getIdentity).collect(Collectors.toSet());
    }

    public boolean anyExcludedColMatch(Collection<TblColRef> tblColRefs) {
        return tblColRefs.stream().anyMatch(this::isExcludedCol);
    }

    public boolean isExcludedCol(@NonNull TblColRef tblColRef) {
        return excludedCols.contains(tblColRef.getColumnDesc()) || isExcludedCC(tblColRef);
    }

    private boolean isExcludedCC(@NonNull TblColRef tblColRef) {
        ColumnDesc columnDesc = tblColRef.getColumnDesc();
        if (columnDesc != null && columnDesc.isComputedColumn()) {
            boolean value = isExcludedCC(columnDesc.getComputedColumnExpr());
            if (value) {
                return true;
            }
        }
        List<TblColRef> operands = tblColRef.getOperands();
        if (operands == null) {
            return false;
        }
        for (TblColRef colRef : operands) {
            if (isExcludedCol(colRef)) {
                return true;
            }
        }
        return false;
    }

    public boolean isExcludedCC(ComputedColumnDesc cc) {
        return isExcludedCC(cc.getInnerExpression());
    }

    public boolean isExcludedCC(String innerExpression) {
        if (StringUtils.isBlank(innerExpression)) {
            return false;
        }
        String[] splits = innerExpression.split("\\s");
        for (String split : splits) {
            int begin = split.indexOf('`');
            int end = split.lastIndexOf('`');
            if (begin != -1 && excludedColNames.contains(split.substring(begin, end + 1))) {
                return true;
            }
        }
        return false;
    }

    public boolean isExcludedMeasure(@NonNull FunctionDesc functionDesc) {
        List<TblColRef> colRefs = functionDesc.getColRefs();
        if (CollectionUtils.isEmpty(colRefs)) {
            return false;
        }
        for (TblColRef colRef : colRefs) {
            if (isExcludedCol(colRef)) {
                return true;
            }
        }
        return false;
    }
}
