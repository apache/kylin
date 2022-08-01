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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * There are two kinds of excluded lookup tables.
 * One kind is explicit excluded by project settings,
 * another is excluded by model join relations.
 */
public class ExcludedLookupChecker {
    private final String factTable;
    private final Set<String> excludedLookups = Sets.newHashSet();
    private final Set<String> antiFlattenLookups = Sets.newHashSet();
    private final Map<String, String> excludedLookupCCs = Maps.newHashMap();
    private final Map<String, String> antiFlattenLookupCCs = Maps.newHashMap();
    private final Map<String, Set<String>> joinTableAliasMap = Maps.newHashMap();

    public ExcludedLookupChecker(Set<String> excludedTables, List<JoinTableDesc> joinTables, NDataModel model) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv() && model == null) {
            factTable = null;
            return;
        }
        factTable = model.getRootFactTableName();
        excludedTables.forEach(table -> {
            if (table.equalsIgnoreCase(factTable)) {
                return;
            }
            excludedLookups.add(table);
        });
        if (model.isBroken() || CollectionUtils.isEmpty(model.getJoinTables())) {
            return;
        }

        model.getJoinTables().forEach(join -> {
            joinTableAliasMap.putIfAbsent(join.getTable(), Sets.newHashSet());
            joinTableAliasMap.get(join.getTable()).add(join.getAlias());
        });

        Map<String, String> aliasToIdentityMap = Maps.newHashMap();
        if (joinTables == null) {
            return;
        }
        joinTables.forEach(joinTable -> {
            aliasToIdentityMap.put(joinTable.getAlias(), joinTable.getTable());
            if (!joinTable.isFlattenable()) {
                antiFlattenLookups.add(joinTable.getTable());
            }
        });
        for (JoinTableDesc joinTable : joinTables) {
            TblColRef[] fkColumns = joinTable.getJoin().getForeignKeyColumns();
            TableRef foreignTableRef = joinTable.getJoin().getForeignTableRef();
            String fkTableAlias;
            if (fkColumns.length > 0) {
                TblColRef firstFK = fkColumns[0];
                fkTableAlias = firstFK.getTableAlias();
                if (canTreatAsAntiFlattenableLookup(aliasToIdentityMap, joinTable, firstFK.getTableAlias(),
                        firstFK.getTableWithSchema())) {
                    antiFlattenLookups.add(joinTable.getTable());
                    excludedLookups.add(joinTable.getTable());
                }
            } else if (foreignTableRef != null) {
                fkTableAlias = foreignTableRef.getAlias();
                if (canTreatAsAntiFlattenableLookup(aliasToIdentityMap, joinTable, foreignTableRef.getAlias(),
                        foreignTableRef.getTableIdentity())) {
                    antiFlattenLookups.add(joinTable.getTable());
                    excludedLookups.add(joinTable.getTable());
                }
            } else {
                fkTableAlias = null;
            }

            if (aliasToIdentityMap.containsKey(fkTableAlias)) {
                String fkTable = aliasToIdentityMap.get(fkTableAlias);
                if (excludedLookups.contains(fkTable)) {
                    excludedLookups.add(joinTable.getTable());
                }
            }
        }
    }

    private boolean canTreatAsAntiFlattenableLookup(Map<String, String> aliasToIdentityMap, JoinTableDesc joinTable,
            String fkTableAlias, String fkTableIdentity) {
        return !joinTable.isFlattenable() //
                || (aliasToIdentityMap.containsKey(fkTableAlias) && antiFlattenLookups.contains(fkTableIdentity));
    }

    /**
     * not very efficient for cc inner expression is a string
     */
    public boolean isColRefDependsLookupTable(TblColRef tblColRef) {
        return isColDependsLookups(tblColRef, excludedLookups, excludedLookupCCs);
    }

    public boolean isCCDependsLookupTable(TblColRef tblColRef) {
        List<TblColRef> operands = tblColRef.getOperands();
        if (operands == null) {
            if (tblColRef.getTable() == null) {
                return false;
            } else {
                return excludedLookups.contains(tblColRef.getTableWithSchema());
            }
        }
        for (TblColRef colRef : operands) {
            if (isCCDependsLookupTable(colRef)) {
                return true;
            }
        }
        return false;
    }

    public boolean isMeasureOnLookupTable(FunctionDesc functionDesc) {
        List<TblColRef> colRefs = functionDesc.getColRefs();
        if (colRefs == null || colRefs.isEmpty()) {
            return false;
        }
        for (TblColRef colRef : colRefs) {
            if (colRef.isInnerColumn()) {
                if (isCCDependsLookupTable(colRef)) {
                    return true;
                }
            } else if (isColRefDependsLookupTable(colRef)) {
                return true;
            }
        }
        return false;
    }

    /**
     * For computed column is very difficult to get the excluded lookup table, so handle
     * it in the step of IndexSuggester#replaceDimOfLookupTableWithFK.
     */
    public Set<String> getUsedExcludedLookupTable(Set<TblColRef> colRefs) {
        Set<String> usedExcludedLookupTables = Sets.newHashSet();
        for (TblColRef column : colRefs) {
            if (excludedLookups.contains(column.getTableWithSchema())) {
                usedExcludedLookupTables.add(column.getTableWithSchema());
            }
        }
        return usedExcludedLookupTables;
    }

    public String detectAntiFlattenLookup(ComputedColumnDesc computedColumn) {
        String innerExp = computedColumn.getInnerExpression();
        if (isInnerExpDependsLookups(innerExp, antiFlattenLookups, antiFlattenLookupCCs)) {
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
            if (isInnerExpDependsLookups(cc.getInnerExpression(), antiFlattenLookups, antiFlattenLookupCCs)) {
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
            if (isColDependsAntiFlattenLookup(colRef)) {
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
                if (parameter.isConstant()) {
                    continue;
                }
                if (isColDependsAntiFlattenLookup(parameter.getColRef())) {
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

    public List<String> getAntiFlattenLookups() {
        return Lists.newArrayList(antiFlattenLookups);
    }

    public Set<String> getExcludedLookups() {
        return excludedLookups;
    }

    private boolean isColDependsAntiFlattenLookup(TblColRef colRef) {
        return isColDependsLookups(colRef, antiFlattenLookups, antiFlattenLookupCCs);
    }

    private boolean isColDependsLookups(TblColRef colRef, Set<String> lookupTables, Map<String, String> cachedCC) {
        if (!colRef.getColumnDesc().isComputedColumn()) {
            return lookupTables.contains(colRef.getTableWithSchema());
        }
        String innerExpression = colRef.getColumnDesc().getComputedColumnExpr();
        return isInnerExpDependsLookups(innerExpression, lookupTables, cachedCC);
    }

    private boolean isInnerExpDependsLookups(String innerExp, Set<String> lookupTables, Map<String, String> cachedCC) {
        if (cachedCC.containsKey(innerExp)) {
            return true;
        }

        for (String table : lookupTables) {
            Set<String> aliasSet = joinTableAliasMap.get(table);
            if (aliasSet == null) {
                continue;
            }
            for (String alias : aliasSet) {
                String aliasWithBacktick = String.format(Locale.ROOT, "`%s`", alias);
                if (innerExp.contains(aliasWithBacktick)) {
                    cachedCC.putIfAbsent(innerExp, alias);
                    return true;
                }
            }
        }
        return false;
    }

    public String detectFilterConditionDependsLookups(String exp, Set<String> lookupTables) {
        for (String table : lookupTables) {
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
