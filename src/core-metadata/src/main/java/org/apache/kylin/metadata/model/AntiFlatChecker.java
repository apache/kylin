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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;

public class AntiFlatChecker {
    @Getter
    private final Set<String> antiFlattenLookups = Sets.newHashSet();
    private final Map<String, String> antiFlattenLookupCCs = Maps.newHashMap();
    private final Map<String, Set<String>> joinTableAliasMap = Maps.newHashMap();

    /**
     * Initialize a checker to handle lookup table which has not been built into index.
     * 
     * @param joinTables joinTables must get from a model which has been initialized.
     *                   This parameter equals to {@link NDataModel#getJoinTables()} of the
     *                   second parameter model at most case, however, if we add/update/remove
     *                   some join tables by editing the model, they are different.
     * @param model the model to check.
     */
    public AntiFlatChecker(List<JoinTableDesc> joinTables, NDataModel model) {
        if (model == null || model.isBroken() || CollectionUtils.isEmpty(model.getJoinTables())) {
            return;
        }

        model.getJoinTables().forEach(join -> {
            joinTableAliasMap.putIfAbsent(join.getTable(), Sets.newHashSet());
            joinTableAliasMap.get(join.getTable()).add(join.getAlias());
        });

        if (joinTables == null) {
            return;
        }
        Map<String, String> aliasToIdentityMap = Maps.newHashMap();
        joinTables.forEach(joinTable -> {
            aliasToIdentityMap.put(joinTable.getAlias(), joinTable.getTable());
            if (!joinTable.isFlattenable()) {
                antiFlattenLookups.add(joinTable.getTable());
            }
        });
        joinTables.forEach(joinTable -> {
            TblColRef[] fkColumns = joinTable.getJoin().getForeignKeyColumns();
            TableRef foreignTableRef = joinTable.getJoin().getForeignTableRef();
            if (fkColumns != null && fkColumns.length > 0) {
                TblColRef firstFK = fkColumns[0];
                String tableAlias = firstFK.getTableAlias();
                String tableWithSchema = firstFK.getTableWithSchema();
                if (canTreatAsAntiLookup(aliasToIdentityMap, joinTable, tableAlias, tableWithSchema)) {
                    antiFlattenLookups.add(joinTable.getTable());
                }
            } else if (foreignTableRef != null) {
                String tableIdentity = foreignTableRef.getTableIdentity();
                String tableAlias = foreignTableRef.getAlias();
                if (canTreatAsAntiLookup(aliasToIdentityMap, joinTable, tableAlias, tableIdentity)) {
                    antiFlattenLookups.add(joinTable.getTable());
                }
            }
        });
    }

    private boolean canTreatAsAntiLookup(Map<String, String> aliasToIdentityMap, JoinTableDesc joinTable,
            String fkTableAlias, String fkTableIdentity) {
        return !joinTable.isFlattenable() //
                || (aliasToIdentityMap.containsKey(fkTableAlias) && antiFlattenLookups.contains(fkTableIdentity));
    }

    public boolean isColOfAntiLookup(TblColRef colRef) {
        if (!colRef.getColumnDesc().isComputedColumn()) {
            return antiFlattenLookups.contains(colRef.getTableWithSchema());
        }
        String innerExpression = colRef.getColumnDesc().getComputedColumnExpr();
        return isCCOfAntiLookup(innerExpression);
    }

    public boolean isCCOfAntiLookup(TblColRef tblColRef) {
        List<TblColRef> operands = tblColRef.getOperands();
        if (operands == null) {
            if (tblColRef.getTable() == null) {
                return false;
            } else {
                return antiFlattenLookups.contains(tblColRef.getTableWithSchema());
            }
        }
        for (TblColRef colRef : operands) {
            if (isCCOfAntiLookup(colRef)) {
                return true;
            }
        }
        return false;
    }

    public boolean isCCOfAntiLookup(String innerExp) {
        if (antiFlattenLookupCCs.containsKey(innerExp)) {
            return true;
        }

        for (String table : antiFlattenLookups) {
            Set<String> aliasSet = joinTableAliasMap.get(table);
            if (aliasSet == null) {
                continue;
            }
            for (String alias : aliasSet) {
                String aliasWithBacktick = String.format(Locale.ROOT, "`%s`", alias);
                if (innerExp.contains(aliasWithBacktick)) {
                    antiFlattenLookupCCs.putIfAbsent(innerExp, alias);
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isMeasureOfAntiLookup(FunctionDesc functionDesc) {
        List<TblColRef> colRefs = functionDesc.getColRefs();
        if (colRefs == null || colRefs.isEmpty()) {
            return false;
        }
        for (TblColRef colRef : colRefs) {
            if (colRef.isInnerColumn()) {
                if (isCCOfAntiLookup(colRef)) {
                    return true;
                }
            } else if (isColOfAntiLookup(colRef)) {
                return true;
            }
        }
        return false;
    }

    public String detectAntiFlattenLookup(ComputedColumnDesc computedColumn) {
        String innerExp = computedColumn.getInnerExpression();
        if (isCCOfAntiLookup(innerExp)) {
            return antiFlattenLookupCCs.get(innerExp);
        }
        return null;
    }

    public List<ComputedColumnDesc> getInvalidComputedColumns(NDataModel model) {
        if (model.isBroken()) {
            return Lists.newArrayList();
        }

        List<ComputedColumnDesc> ccList = Lists.newArrayList();
        for (ComputedColumnDesc cc : model.getComputedColumnDescs()) {
            if (isCCOfAntiLookup(cc.getInnerExpression())) {
                ccList.add(JsonUtil.deepCopyQuietly(cc, ComputedColumnDesc.class));
            }
        }
        return ccList;
    }

    public Set<Integer> getInvalidDimensions(NDataModel model) {
        if (model.isBroken()) {
            return Sets.newHashSet();
        }

        Set<Integer> dimensions = Sets.newHashSet();
        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            if (!column.isDimension()) {
                continue;
            }
            TblColRef colRef = model.getEffectiveCols().get(column.getId());
            if (isColOfAntiLookup(colRef)) {
                dimensions.add(column.getId());
            }
        }
        return dimensions;
    }

    public Set<Integer> getInvalidMeasures(NDataModel model) {
        if (model.isBroken()) {
            return Sets.newHashSet();
        }

        Set<Integer> measures = Sets.newHashSet();
        for (NDataModel.Measure measure : model.getAllMeasures()) {
            if (measure.isTomb()) {
                continue;
            }
            List<ParameterDesc> parameters = measure.getFunction().getParameters();
            for (ParameterDesc parameter : parameters) {
                if (!parameter.isConstant() && isColOfAntiLookup(parameter.getColRef())) {
                    measures.add(measure.getId());
                    break;
                }
            }
        }
        return measures;
    }

    public Set<Long> getInvalidIndexes(IndexPlan indexPlan, Set<Integer> invalidScope) {
        if (indexPlan == null || indexPlan.isBroken()) {
            return Sets.newHashSet();
        }
        Set<Long> indexes = Sets.newHashSet();
        indexPlan.getAllLayoutsMap().forEach((layoutId, layout) -> {
            for (Integer id : layout.getColOrder()) {
                if (invalidScope.contains(id)) {
                    indexes.add(layoutId);
                    break;
                }
            }
        });
        return indexes;
    }

    public String detectFilterCondition(String exp) {
        for (String table : antiFlattenLookups) {
            Set<String> aliasSet = joinTableAliasMap.get(table);
            if (aliasSet == null) {
                continue;
            }
            for (String alias : aliasSet) {
                String aliasWithBacktick = String.format(Locale.ROOT, "`%s`", alias);
                if (exp.contains(aliasWithBacktick)) {
                    return alias;
                }
            }
        }
        return null;
    }
}
