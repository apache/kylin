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

package org.apache.kylin.rec.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryModelPriorities;

import lombok.Getter;

@Getter
public class ModelTree {
    private final Collection<OlapContext> olapContexts;
    private final Map<String, JoinTableDesc> joins;
    private final Map<TableRef, String> tableRefAliasMap;
    private final TableDesc rootFactTable;
    private final boolean hasModelPropertiesHint;

    public ModelTree(TableDesc rootFactTable, Collection<OlapContext> contexts, Map<String, JoinTableDesc> joins,
            Map<TableRef, String> tableRefAliasMap) {
        this.rootFactTable = rootFactTable;
        this.olapContexts = contexts;
        this.joins = joins;
        this.tableRefAliasMap = tableRefAliasMap;
        this.hasModelPropertiesHint = hasModelPropertiesHint(contexts);
    }

    public ModelTree(TableDesc rootFactTable, Collection<OlapContext> contexts, Map<String, JoinTableDesc> joins) {
        this.rootFactTable = rootFactTable;
        this.olapContexts = contexts;
        this.joins = joins;
        this.tableRefAliasMap = Maps.newHashMap();
        this.hasModelPropertiesHint = hasModelPropertiesHint(contexts);
    }

    public JoinsGraph getJoinGraph(TableRef defaultFactTable) {
        List<JoinDesc> modelTreeJoins = Lists.newArrayListWithExpectedSize(joins.size());
        TableRef factTblRef = null;
        if (joins.isEmpty()) {
            factTblRef = defaultFactTable;
        } else {
            Map<TableRef, TableRef> joinMap = Maps.newHashMap();
            joins.values().forEach(joinTableDesc -> {
                modelTreeJoins.add(joinTableDesc.getJoin());
                joinMap.put(joinTableDesc.getJoin().getPKSide(), joinTableDesc.getJoin().getFKSide());
            });

            for (Map.Entry<TableRef, TableRef> joinEntry : joinMap.entrySet()) {
                if (!joinMap.containsKey(joinEntry.getValue())) {
                    factTblRef = joinEntry.getValue();
                    break;
                }
            }
        }
        return new JoinsGraph(factTblRef, modelTreeJoins);
    }

    private boolean hasSameRootFactTable(TableRef tableRef) {
        return tableRef.getTableIdentity().equals(rootFactTable.getIdentity());
    }

    public boolean isExactlyMatch(NDataModel dataModel, boolean partialMatch, boolean partialMatchNonEqui) {
        TableRef tableRef = dataModel.getRootFactTable();
        return hasSameRootFactTable(tableRef) //
                && getJoinGraph(tableRef).match(dataModel.getJoinsGraph(), Maps.newHashMap(), partialMatch,
                        partialMatchNonEqui);
    }

    public boolean hasSameSubGraph(NDataModel dataModel) {
        final TableRef tableRef = dataModel.getRootFactTable();
        JoinsGraph joinsGraph = getJoinGraph(tableRef);
        return hasSameRootFactTable(tableRef) //
                && (dataModel.getJoinsGraph().match(joinsGraph, Maps.newHashMap())
                        || joinsGraph.match(dataModel.getJoinsGraph(), Maps.newHashMap()));
    }

    private boolean hasModelPropertiesHint(Collection<OlapContext> olapContexts) {
        for (OlapContext sqlContext : olapContexts) {
            String[] modelPriorities = QueryModelPriorities.getModelPrioritiesFromComment(sqlContext.getSql());
            if (!StringUtils.join(modelPriorities).equals("")) {
                return true;
            }
        }
        return false;
    }

    /*
     * This method is used only when SQL hints are present and for HashSet deduplication.
     * see https://olapio.atlassian.net/browse/AL-9662
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ModelTree)) {
            return false;
        }
        ModelTree other = (ModelTree) obj;
        // sqlHint must have
        if (!hasModelPropertiesHint || !other.hasModelPropertiesHint) {
            return false;
        }
        // olapContext
        if (olapContexts.size() != other.olapContexts.size()) {
            return false;
        } else {
            OlapContext[] contextsArray = olapContexts.toArray((new OlapContext[0]));
            OlapContext[] othContextsArray = other.olapContexts.toArray((new OlapContext[0]));
            for (int i = 0; i < contextsArray.length; i++) {
                if (!contextsArray[i].getSQLDigest().equalsWithoutAlias(othContextsArray[i].getSQLDigest())) {
                    return false;
                }
            }
        }
        // joins
        if (!(joins.equals(other.joins))) {
            return false;
        }
        // tableRefAliasMap
        if (tableRefAliasMap.size() != other.tableRefAliasMap.size()) {
            return false;
        } else {
            HashSet<Object> tableRefAliasSet = new HashSet<>(Arrays.asList(tableRefAliasMap.values().toArray()));
            HashSet<Object> othTableRefAliasSet = new HashSet<>(
                    Arrays.asList(other.tableRefAliasMap.values().toArray()));
            if (!tableRefAliasSet.equals(othTableRefAliasSet)) {
                return false;
            }
        }
        // rootFactTable
        return rootFactTable.equals(other.rootFactTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(olapContexts.size(), joins, tableRefAliasMap.size(), rootFactTable, hasModelPropertiesHint);
    }
}
