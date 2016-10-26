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

package org.apache.kylin.query.routing;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ModelChooser {

    // select a model that satisfies all the contexts
    public static Set<IRealization> selectModel(List<OLAPContext> contexts) {
        Map<DataModelDesc, Set<IRealization>> modelMap = makeOrderedModelMap(contexts);

        for (DataModelDesc model : modelMap.keySet()) {
            Map<String, String> aliasMap = matches(model, contexts);
            if (aliasMap != null) {
                for (OLAPContext ctx : contexts)
                    fixModel(ctx, model, aliasMap);
                return modelMap.get(model);
            }
        }

        throw new NoRealizationFoundException("No model found for" + toErrorMsg(contexts));
    }

    private static String toErrorMsg(List<OLAPContext> contexts) {
        StringBuilder buf = new StringBuilder();
        for (OLAPContext ctx : contexts) {
            buf.append(", ").append(ctx.firstTableScan);
            for (JoinDesc join : ctx.joins)
                buf.append(", ").append(join);
        }
        return buf.toString();
    }

    private static Map<String, String> matches(DataModelDesc model, List<OLAPContext> contexts) {
        Map<String, String> result = Maps.newHashMap();

        // the greedy match is not perfect but works for the moment
        Map<String, List<JoinDesc>> modelJoinsMap = model.getJoinsMap();
        for (OLAPContext ctx : contexts) {
            for (JoinDesc queryJoin : ctx.joins) {
                String fkTable = queryJoin.getForeignKeyColumns()[0].getTable();
                List<JoinDesc> modelJoins = modelJoinsMap.get(fkTable);
                if (modelJoins == null)
                    return null;

                JoinDesc matchJoin = null;
                for (JoinDesc modelJoin : modelJoins) {
                    if (modelJoin.matches(queryJoin)) {
                        matchJoin = modelJoin;
                        break;
                    }
                }
                if (matchJoin == null)
                    return null;

                matchesAdd(queryJoin.getForeignKeyColumns()[0].getTableAlias(), matchJoin.getForeignKeyColumns()[0].getTableAlias(), result);
                matchesAdd(queryJoin.getPrimaryKeyColumns()[0].getTableAlias(), matchJoin.getPrimaryKeyColumns()[0].getTableAlias(), result);
            }
            
            OLAPTableScan firstTable = ctx.firstTableScan;
            String firstTableAlias = firstTable.getAlias();
            if (result.containsKey(firstTableAlias) == false) {
                TableRef tableRef = model.findFirstTable(firstTable.getOlapTable().getTableName());
                if (tableRef == null)
                    return null;
                matchesAdd(firstTableAlias, tableRef.getAlias(), result);
            }
        }
        
        return result;
    }

    private static void matchesAdd(String origAlias, String targetAlias, Map<String, String> result) {
        String existingTarget = result.put(origAlias, targetAlias);
        Preconditions.checkState(existingTarget == null || existingTarget.equals(targetAlias));
    }

    private static Map<DataModelDesc, Set<IRealization>> makeOrderedModelMap(List<OLAPContext> contexts) {
        // the first context, which is the top most context, contains all columns from all contexts
        OLAPContext first = contexts.get(0);
        KylinConfig kylinConfig = first.olapSchema.getConfig();
        String projectName = first.olapSchema.getProjectName();
        String factTableName = first.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = ProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName, factTableName);

        final Map<DataModelDesc, Set<IRealization>> models = Maps.newHashMap();
        final Map<DataModelDesc, RealizationCost> costs = Maps.newHashMap();
        for (IRealization real : realizations) {
            if (real.isReady() == false)
                continue;
            if (containsAll(real.getAllColumnDescs(), first.allColumns) == false)
                continue;
            if (RemoveBlackoutRealizationsRule.accept(real) == false)
                continue;

            RealizationCost cost = new RealizationCost(real);
            DataModelDesc m = real.getDataModelDesc();
            Set<IRealization> set = models.get(m);
            if (set == null) {
                set = Sets.newHashSet();
                set.add(real);
                models.put(m, set);
                costs.put(m, cost);
            } else {
                set.add(real);
                RealizationCost curCost = costs.get(m);
                if (cost.compareTo(curCost) < 0)
                    costs.put(m, cost);
            }
        }

        // order model by cheapest realization cost
        TreeMap<DataModelDesc, Set<IRealization>> result = Maps.newTreeMap(new Comparator<DataModelDesc>() {
            @Override
            public int compare(DataModelDesc o1, DataModelDesc o2) {
                return costs.get(o1).compareTo(costs.get(o2));
            }
        });
        result.putAll(models);

        return result;
    }

    private static boolean containsAll(Set<ColumnDesc> allColumnDescs, Set<TblColRef> allColumns) {
        for (TblColRef col : allColumns) {
            if (allColumnDescs.contains(col.getColumnDesc()) == false)
                return false;
        }
        return true;
    }

    private static void fixModel(OLAPContext context, DataModelDesc model, Map<String, String> aliasMap) {
        for (OLAPTableScan tableScan : context.allTableScans) {
            tableScan.fixColumnRowTypeWithModel(model, aliasMap);
        }
    }

    private static class RealizationCost implements Comparable<RealizationCost> {
        final public int priority;
        final public int cost;

        public RealizationCost(IRealization real) {
            // ref Candidate.PRIORITIES
            this.priority = Candidate.PRIORITIES.get(real.getType());

            // ref CubeInstance.getCost()
            int c = real.getAllDimensions().size() * CubeInstance.COST_WEIGHT_DIMENSION + real.getMeasures().size() * CubeInstance.COST_WEIGHT_MEASURE;
            for (LookupDesc lookup : real.getDataModelDesc().getLookups()) {
                if (lookup.getJoin().isInnerJoin())
                    c += CubeInstance.COST_WEIGHT_INNER_JOIN;
            }
            this.cost = c;
        }

        @Override
        public int compareTo(RealizationCost o) {
            int comp = this.priority - o.priority;
            if (comp != 0)
                return comp;
            else
                return this.cost - o.cost;
        }
    }
}
