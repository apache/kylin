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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModel.TableKind;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.metadata.model.util.JoinDescUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryModelPriorities;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.TableAliasGenerator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GreedyModelTreesBuilder {

    private final Map<String, TableDesc> tableMap;
    KylinConfig kylinConfig;
    AbstractContext proposeContext;

    public GreedyModelTreesBuilder(KylinConfig kylinConfig, String project, AbstractContext proposeContext) {
        this.kylinConfig = kylinConfig;
        this.tableMap = NTableMetadataManager.getInstance(kylinConfig, project).getAllTablesMap();
        this.proposeContext = proposeContext;
    }

    public List<ModelTree> build(Map<String, Collection<OlapContext>> olapContexts, TableDesc expectedFactTable) {

        // 1. group by its factTable and model priority(sql hint)
        Map<TableDesc, Map<String, List<OlapContext>>> groupedMap = Maps.newHashMap();
        olapContexts.forEach((sql, sqlContexts) -> {
            Map<TableDesc, Map<String, List<OlapContext>>> groupingResult = sqlContexts.stream()
                    .filter(ctx -> isFactTableCompatible(ctx, expectedFactTable)) //
                    .collect(Collectors.groupingBy(ctx -> ctx.getFirstTableScan().getTableRef().getTableDesc(),
                            Collectors.groupingBy(ctx -> {
                                String[] modelPriorities = QueryModelPriorities
                                        .getModelPrioritiesFromComment(ctx.getSql());
                                return StringUtils.join(modelPriorities);
                            })));
            groupingResult.forEach((table, map) -> {
                groupedMap.putIfAbsent(table, Maps.newLinkedHashMap());
                Map<String, List<OlapContext>> hintOlapMap = groupedMap.get(table);
                map.forEach((hint, olapList) -> {
                    List<OlapContext> contexts = hintOlapMap.computeIfAbsent(hint, h -> Lists.newArrayList());
                    contexts.addAll(olapList);
                });
            });
        });

        // 2. each group produces at least one modelTree
        List<ModelTree> modelTreeList = groupedMap.values().stream()
                .flatMap(hintOlapMap -> hintOlapMap.values().stream()) //
                .filter(olapList -> !olapList.isEmpty()) //
                .map(olapList -> {
                    TableDesc table = olapList.get(0).getFirstTableScan().getTableRef().getTableDesc();
                    TreeBuilder builder = new TreeBuilder(table, tableMap, proposeContext);
                    builder.contextList.addAll(olapList);
                    return builder.build();
                }).flatMap(List::stream).collect(Collectors.toList());

        // 2.1 merge duplicate modelTree if needed
        Map<ModelTree, ModelTree> modelTreeMap = new LinkedHashMap<>();
        for (ModelTree modelTree : modelTreeList) {
            if (modelTreeMap.containsKey(modelTree)) {
                ModelTree existingTree = modelTreeMap.get(modelTree);
                existingTree.getOlapContexts().addAll(modelTree.getOlapContexts());
                existingTree.getTableRefAliasMap().putAll(modelTree.getTableRefAliasMap());
            } else {
                modelTreeMap.put(modelTree, modelTree);
            }
        }
        modelTreeList = new ArrayList<>(modelTreeMap.values());

        // 3. enable current root_fact's model exists
        if (expectedFactTable != null
                && modelTreeList.stream().noneMatch(tree -> tree.getRootFactTable() == expectedFactTable)) {
            log.debug("There is no modelTree relies on fact table({}), add a new one.",
                    expectedFactTable.getIdentity());
            ModelTree emptyTree = new ModelTree(expectedFactTable, ImmutableList.of(), ImmutableMap.of(),
                    ImmutableMap.of());
            modelTreeList.add(emptyTree);
        }
        return modelTreeList;
    }

    private boolean isFactTableCompatible(OlapContext ctx, TableDesc factTable) {
        if (ctx.getFirstTableScan() == null) {
            return false;
        }
        TableDesc ctxFactTable = ctx.getFirstTableScan().getTableRef().getTableDesc();
        return factTable == null || Objects.equals(ctxFactTable.getIdentity(), factTable.getIdentity());
    }

    public ModelTree build(Collection<OlapContext> olapContexts, TableDesc actualFactTbl) {
        TreeBuilder treeBuilder = new TreeBuilder(actualFactTbl, tableMap, proposeContext);
        treeBuilder.contextList.addAll(olapContexts);
        return treeBuilder.buildOne(olapContexts, false);
    }

    public static class TreeBuilder {
        private final TableDesc rootFact;
        private final TableAliasGenerator.TableAliasDict dict;
        private final AbstractContext proposeContext;
        private final List<OlapContext> contextList = Lists.newArrayList();

        public TreeBuilder(TableDesc rootFact, Map<String, TableDesc> tableMap, AbstractContext proposeContext) {
            this.rootFact = rootFact;
            this.dict = TableAliasGenerator.generateNewDict(tableMap.keySet().toArray(new String[0]));
            this.proposeContext = proposeContext;
        }

        /**
         * based on the path to root node in JoinGraph to produce table alias,
         * so that it can be unique in different ctx
         * but same position, even if the alias is not equaled in query.
         *
         * @param ctx OlapContext
         * @param dict dict
         * @return map from TableRef to alias
         */
        static Map<TableRef, String> getUniqueTblAliasBasedOnPosInGraph(OlapContext ctx,
                TableAliasGenerator.TableAliasDict dict) {
            JoinsGraph joinsGraph = ctx.getJoinsGraph();
            if (joinsGraph == null) {
                joinsGraph = new JoinsGraph(ctx.getFirstTableScan().getTableRef(), ctx.getJoins());
            }
            return TreeBuilder.getUniqueTblAliasBasedOnPosInGraph(joinsGraph, dict);
        }

        static Map<TableRef, String> getUniqueTblAliasBasedOnPosInGraph(JoinsGraph joinsGraph,
                TableAliasGenerator.TableAliasDict dict) {

            Map<TableRef, String> allTableAlias = new HashMap<>();
            for (TableRef tableRef : joinsGraph.getAllTblRefNodes()) {
                JoinDesc[] joinHierarchy = getJoinDescHierarchy(joinsGraph, tableRef);
                String tblAlias = joinHierarchy.length == 0 //
                        ? joinsGraph.getCenter().getTableName() //
                        : dict.getHierarchyAliasFromJoins(joinHierarchy);
                allTableAlias.put(tableRef, tblAlias);
            }
            return allTableAlias;
        }

        static Map<TableRef, String> correctTblAliasAndKeepOriginAlias(Map<TableRef, String> tblRef2TreePathName,
                TableDesc rootFact, Map<TableRef, String> originTblAlias) {
            Map<String, TableDesc> classifiedAlias = new HashMap<>();
            for (Map.Entry<TableRef, String> entry : tblRef2TreePathName.entrySet()) {
                classifiedAlias.put(entry.getValue(), entry.getKey().getTableDesc());
            }

            Map<String, String> orig2corrected = new HashMap<>();
            // correct fact table alias in 1st place
            String factTableName = rootFact.getName();
            orig2corrected.put(factTableName, factTableName);
            classifiedAlias.remove(factTableName);
            for (Map.Entry<TableRef, String> entry : originTblAlias.entrySet()) {
                orig2corrected.putIfAbsent(tblRef2TreePathName.get(entry.getKey()), entry.getValue());
                classifiedAlias.remove(tblRef2TreePathName.get(entry.getKey()));
            }

            for (Map.Entry<String, TableDesc> entry : classifiedAlias.entrySet()) {
                String tableName = entry.getValue().getName();
                String corrected = tableName;
                int i = 1;
                while (orig2corrected.containsValue(corrected)) {
                    corrected = tableName + "_" + i;
                    i++;
                }
                orig2corrected.put(entry.getKey(), corrected);
            }

            Map<TableRef, String> correctedTableAlias = Maps.newHashMap();
            for (Map.Entry<TableRef, String> entry : tblRef2TreePathName.entrySet()) {
                String corrected = orig2corrected.get(entry.getValue());
                correctedTableAlias.put(entry.getKey(), corrected);
            }

            return correctedTableAlias;
        }

        private static Map<TableRef, String> correctTableAlias(Map<TableRef, String> innerTableRefAlias,
                TableDesc rootFact) {
            return correctTblAliasAndKeepOriginAlias(innerTableRefAlias, rootFact, Maps.newHashMap());
        }

        private List<ModelTree> build() {
            List<ModelTree> result = Lists.newArrayList();
            while (!contextList.isEmpty()) {
                result.add(buildOne(contextList, false));
            }
            return result;
        }

        private ModelTree buildOne(Collection<OlapContext> inputCtxs, boolean forceMerge) {
            Map<TableRef, String> innerTableRefAlias = Maps.newHashMap();
            Map<TableRef, String> correctedTableAlias = Maps.newHashMap();
            List<OlapContext> usedCtxs = Lists.newArrayList();
            List<OlapContext> ctxsNeedMerge = Lists.newArrayList();
            inputCtxs.removeIf(Objects::isNull);
            inputCtxs.stream() //
                    .filter(ctx -> forceMerge || matchContext(usedCtxs, ctx)) //
                    .filter(ctx -> {
                        innerTableRefAlias.putAll(getUniqueTblAliasBasedOnPosInGraph(ctx, dict));
                        correctedTableAlias.putAll(correctTableAlias(innerTableRefAlias, rootFact));
                        usedCtxs.add(ctx);
                        // Digest single table contexts(no joins)
                        return !ctx.getJoins().isEmpty();
                    }) //
                    .forEach(ctxsNeedMerge::add);

            Map<String, TableRef> aliasRefMap = Maps.newHashMap();
            Map<String, JoinTableDesc> joinTables = new LinkedHashMap<>();

            // Merge matching contexts' joins
            Map<String, AccelerateInfo> accelerateInfoMap = proposeContext.getAccelerateInfoMap();
            for (OlapContext ctx : ctxsNeedMerge) {
                AccelerateInfo accelerateInfo = accelerateInfoMap.get(ctx.getSql());
                if (accelerateInfo.isNotSucceed()) {
                    inputCtxs.remove(ctx);
                    usedCtxs.remove(ctx);
                    continue;
                }

                try {
                    mergeContext(ctx, joinTables, correctedTableAlias, aliasRefMap);
                } catch (Exception e) {
                    log.debug("Failed to accelerate sql: \n{}\n", ctx.getSql(), e);
                    inputCtxs.remove(ctx);
                    usedCtxs.remove(ctx);
                    accelerateInfo.setFailedCause(e);
                }
            }

            inputCtxs.removeAll(usedCtxs);
            return new ModelTree(rootFact, usedCtxs, joinTables, correctedTableAlias);
        }

        public boolean matchContext(List<OlapContext> ctxs, OlapContext anotherCtx) {
            return ctxs.stream().allMatch(thisCtx -> matchContext(thisCtx, anotherCtx));
        }

        public boolean matchContext(OlapContext ctxA, OlapContext ctxB) {
            if (ctxA == ctxB) {
                return true;
            }
            if (ctxA == null || ctxB == null) {
                return false;
            }
            JoinsGraph graphA = new JoinsGraph(ctxA.getFirstTableScan().getTableRef(),
                    Lists.newArrayList(ctxA.getJoins()));
            JoinsGraph graphB = new JoinsGraph(ctxB.getFirstTableScan().getTableRef(),
                    Lists.newArrayList(ctxB.getJoins()));

            return graphA.match(graphB, Maps.newHashMap(), proposeContext.isPartialMatch(),
                    proposeContext.isPartialMatchNonEqui()) //
                    || graphB.match(graphA, Maps.newHashMap(), proposeContext.isPartialMatch(),
                            proposeContext.isPartialMatchNonEqui())
                    || (graphA.unmatched(graphB).stream().allMatch(e -> e.isLeftJoin() && !e.isNonEquiJoin())
                            && graphB.unmatched(graphA).stream().allMatch(e -> e.isLeftJoin() && !e.isNonEquiJoin()));
        }

        /**
         * @param ctx OlapContext
         * @param alias2JoinTables unique alias name, usually depend on io.kyligence.kap.smart.util.TableAliasGenerator
         * @param tableRef2Alias map of TableRef to alias
         * @param aliasRefMap map of alias to TableRef
         */
        static void mergeContext(OlapContext ctx, Map<String, JoinTableDesc> alias2JoinTables,
                Map<TableRef, String> tableRef2Alias, Map<String, TableRef> aliasRefMap) {
            mergeJoins(ctx.getJoins(), alias2JoinTables, tableRef2Alias, aliasRefMap);
        }

        public AbstractContext.ModelContext mergeModelContext(AbstractContext proposeContext,
                AbstractContext.ModelContext modelContext, AbstractContext.ModelContext another) {
            List<OlapContext> olapCtxs = Lists.newArrayList(modelContext.getModelTree().getOlapContexts());
            olapCtxs.addAll(another.getModelTree().getOlapContexts());
            return new AbstractContext.ModelContext(proposeContext, buildOne(olapCtxs, true));
        }

        private static void mergeJoins(List<JoinDesc> joins, Map<String, JoinTableDesc> alias2JoinTables,
                Map<TableRef, String> tableRef2Alias, Map<String, TableRef> aliasRefMap) {
            // Collect context updates and apply later
            Map<String, JoinTableDesc> alias2JoinTablesUpdates = new LinkedHashMap<>(alias2JoinTables);
            Map<TableRef, String> tableRef2AliasUpdates = new LinkedHashMap<>(tableRef2Alias);

            List<Pair<JoinDesc, TableKind>> tableKindByJoins = JoinDescUtil.resolveTableType(joins);
            for (Pair<JoinDesc, TableKind> pair : tableKindByJoins) {
                JoinDesc join = pair.getFirst();
                TableKind kind = pair.getSecond();
                String pkTblAlias = tableRef2AliasUpdates.get(join.getPKSide());
                String fkTblAlias = tableRef2AliasUpdates.get(join.getFKSide());

                String joinTableAlias = pkTblAlias;
                boolean isValidJoin = false;
                int loops = 0;
                while (!isValidJoin) {
                    JoinTableDesc newJoinTable = JoinDescUtil.convert(join, kind, joinTableAlias, fkTblAlias,
                            aliasRefMap);
                    JoinTableDesc oldJoinTable = alias2JoinTablesUpdates.computeIfAbsent(joinTableAlias,
                            alias -> newJoinTable);

                    if (JoinDescUtil.isJoinTableEqual(oldJoinTable, newJoinTable)) {
                        isValidJoin = true;
                    } else if (JoinDescUtil.isJoinKeysEqual(oldJoinTable.getJoin(), newJoinTable.getJoin())
                            && JoinDescUtil.isJoinTypeEqual(oldJoinTable.getJoin(), newJoinTable.getJoin())
                            && oldJoinTable.getKind() != newJoinTable.getKind()) {
                        // this case is deprecated, the join table is FACT by default
                        // same join info but table kind differ: LOOKUP vs FACT, use FACT
                        newJoinTable.setKind(NDataModel.TableKind.FACT);
                        alias2JoinTablesUpdates.put(joinTableAlias, newJoinTable);
                    } else {
                        // twin join table with different join info
                        // resolve and assign new alias
                        joinTableAlias = getNewAlias(join.getPKSide().getTableName(), newJoinTable.getAlias());
                    }
                    if (loops++ > 100) {
                        // in case of infinite loop
                        break;
                    }
                }
                Preconditions.checkState(isValidJoin, "Failed to merge table join: %s.", join);
                tableRef2AliasUpdates.put(join.getPKSide(), joinTableAlias);
            }
            alias2JoinTables.putAll(alias2JoinTablesUpdates);
            tableRef2Alias.putAll(tableRef2AliasUpdates);
        }

        private static JoinDesc[] getJoinDescHierarchy(JoinsGraph joinsTree, TableRef leaf) {
            Preconditions.checkState(leaf != null, "The TableRef cannot be null!");
            JoinDesc join = joinsTree.getJoinByPKSide(leaf);
            if (join == null) {
                return new JoinDesc[0];
            }

            return ArrayUtils.add(getJoinDescHierarchy(joinsTree, join.getFKSide()), join);
        }

        /**
         * get new alias by original table name, for table 'foo'
         * foo -> foo_1
         * foo_1 -> foo_2
         */
        private static String getNewAlias(String originalName, String oldAlias) {
            if (oldAlias.equals(originalName)) {
                return originalName + "_1";
            } else if (!oldAlias.startsWith(originalName + "_")) {
                return originalName;
            }

            String number = oldAlias.substring(originalName.length() + 1);
            try {
                int i = Integer.parseInt(number);
                return originalName + "_" + (i + 1);
            } catch (Exception e) {
                return originalName + "_1";
            }
        }
    }
}
