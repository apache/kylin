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

package org.apache.kylin.metadata.model.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

public class JoinsGraph implements Serializable {
    private static final long serialVersionUID = 1L;

    @Getter
    private final TableRef center;
    @Getter
    private final Map<String, TableRef> vertexMap = Maps.newLinkedHashMap();
    private final Map<TableRef, VertexInfo<Edge>> vertexInfoMap = Maps.newHashMap();
    private final Set<Edge> edges = Sets.newHashSet();

    /**
     * Creates a graph
     */
    public JoinsGraph(TableRef root, List<JoinDesc> joins) {
        this(root, joins, true);
    }

    public JoinsGraph(TableRef root, List<JoinDesc> joins, boolean needSwapJoin) {
        this.center = root;
        addVertex(root);

        List<Pair<JoinDesc, Boolean>> newJoinsPair = swapJoinDescs(joins, needSwapJoin);
        for (Pair<JoinDesc, Boolean> pair : newJoinsPair) {
            JoinDesc join = pair.getFirst();
            Boolean isSwap = pair.getSecond();

            Preconditions.checkState(Arrays.stream(join.getForeignKeyColumns()).allMatch(TblColRef::isQualified));
            Preconditions.checkState(Arrays.stream(join.getPrimaryKeyColumns()).allMatch(TblColRef::isQualified));
            addVertex(join.getPKSide());
            addVertex(join.getFKSide());
            addEdge(join, isSwap);
        }
        validate(joins);
    }

    static class VertexInfo<E> {
        final List<E> outEdges = new ArrayList<>();
        final List<E> inEdges = new ArrayList<>();
    }

    public void addVertex(TableRef table) {
        if (vertexMap.containsKey(table.getAlias())) {
            return;
        }
        vertexMap.put(table.getAlias(), table);
        vertexInfoMap.computeIfAbsent(table, f -> new VertexInfo<>());
    }

    public void addEdge(JoinDesc join, boolean swapJoin) {
        Edge edge = new Edge(join, swapJoin);
        vertexInfoMap.get(join.getPKSide()).inEdges.add(edge);
        vertexInfoMap.get(join.getFKSide()).outEdges.add(edge);
        edges.add(edge);
    }

    private void validate(List<JoinDesc> joins) {
        for (JoinDesc join : joins) {
            TableRef fkSide = join.getFKSide();
            Preconditions.checkNotNull(vertexMap.get(fkSide.getAlias()));
            Preconditions.checkState(vertexMap.get(fkSide.getAlias()).equals(fkSide));
        }
        Preconditions.checkState(vertexMap.size() == joins.size() + 1);
    }

    private List<Pair<JoinDesc, Boolean>> swapJoinDescs(List<JoinDesc> joins, boolean needSwapJoin) {
        List<Pair<JoinDesc, Boolean>> newJoins = Lists.newArrayList();
        for (JoinDesc join : joins) {
            // add origin joinDesc
            newJoins.add(Pair.newPair(join, false));
            // inner / leftOrInner => swap joinDesc
            if ((join.isInnerJoin() || join.isLeftOrInnerJoin()) && needSwapJoin) {
                newJoins.add(Pair.newPair(swapJoinDesc(join), true));
            }
        }
        return newJoins;
    }

    private JoinDesc swapJoinDesc(JoinDesc originJoinDesc) {
        JoinDesc swapedJoinDesc = new JoinDesc();
        swapedJoinDesc.setType(originJoinDesc.getType());
        swapedJoinDesc.setPrimaryKey(originJoinDesc.getForeignKey());
        swapedJoinDesc.setForeignKey(originJoinDesc.getPrimaryKey());
        swapedJoinDesc.setNonEquiJoinCondition(originJoinDesc.getNonEquiJoinCondition());
        swapedJoinDesc.setPrimaryTable(originJoinDesc.getForeignTable());
        swapedJoinDesc.setForeignTable(originJoinDesc.getPrimaryTable());
        swapedJoinDesc.setPrimaryKeyColumns(originJoinDesc.getForeignKeyColumns());
        swapedJoinDesc.setForeignKeyColumns(originJoinDesc.getPrimaryKeyColumns());
        swapedJoinDesc.setPrimaryTableRef(originJoinDesc.getForeignTableRef());
        swapedJoinDesc.setForeignTableRef(originJoinDesc.getPrimaryTableRef());
        // swap left join, the current join must be convertible to LeftOrInner
        swapedJoinDesc.setLeftOrInner(originJoinDesc.isLeftOrInnerJoin());
        return swapedJoinDesc;
    }

    public void setJoinEdgeMatcher(IJoinEdgeMatcher joinEdgeMatcher) {
        edges.forEach(edge -> edge.setJoinEdgeMatcher(joinEdgeMatcher));
    }

    public List<Edge> outwardEdges(TableRef table) {
        return Lists.newArrayList(vertexInfoMap.get(table).outEdges);
    }

    public List<Edge> inwardEdges(TableRef table) {
        return Lists.newArrayList(vertexInfoMap.get(table).inEdges);
    }

    public String getCenterTableIdentity() {
        return center.getTableIdentity();
    }

    public boolean match(JoinsGraph pattern, Map<String, String> matchAliasMap) {
        return match(pattern, matchAliasMap, false);
    }

    public boolean match(JoinsGraph pattern, Map<String, String> matchAliasMap, boolean matchPartial) {
        return match(pattern, matchAliasMap, matchPartial, false);
    }

    public boolean match(JoinsGraph pattern, Map<String, String> matchAliasMap, boolean matchPartial,
            boolean matchPartialNonEquiJoin) {
        if (Objects.isNull(pattern) || Objects.isNull(pattern.center)) {
            throw new IllegalArgumentException("pattern(model) should have a center: " + pattern);
        }

        List<TableRef> candidatesOfQCenter = searchCenter(pattern.getCenterTableIdentity());
        for (TableRef candidateCenter : candidatesOfQCenter) {
            List<Edge> unmatchedPatternOutEdges = Lists.newArrayList();
            Map<TableRef, TableRef> allMatchedTableMap = Maps.newHashMap();
            List<Pair<TableRef, TableRef>> toMatchTableList = Lists.newArrayList();
            toMatchTableList.add(Pair.newPair(candidateCenter, pattern.center));

            match0(pattern, toMatchTableList, unmatchedPatternOutEdges, allMatchedTableMap);

            if (!toMatchTableList.isEmpty() || allMatchedTableMap.size() != this.vertexMap.size()) {
                continue;
            }

            // There are three cases of match, the first two cases are exactly match.
            // 1. pattern out edges is matched.
            // 2. unmatched out edges of pattern is not empty, but all of them are left join.
            // 3. unmatched out edges of pattern is not empty, but match partial.
            if ((unmatchedPatternOutEdges.isEmpty() || unmatchedPatternOutEdges.stream().allMatch(Edge::isLeftJoin)
                    || matchPartial) && !allMatchedTableMap.isEmpty()
                    && (matchPartialNonEquiJoin || checkNonEquiJoinMatches(allMatchedTableMap, pattern))) {
                matchAliasMap.clear();
                matchAliasMap.putAll(allMatchedTableMap.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getAlias(), e -> e.getValue().getAlias())));
                return true;
            }
        }
        return false;
    }

    /**
     * Match this joinGraph with a pattern's joinGraph. All matched tables will be put
     * into the parameter {@code  matchedTableMap}. The pattern's unmatched out edges
     * will be put into the parameter {@code unmatchedOutEdgesOfP}.
     *
     * @param pattern                    the joinGraph to compare, usually the model's joinGraph.
     * @param toMatchTableList           tables to match, the keys from this graph, the values from the joinGraph of pattern.
     * @param unmatchedPatternOutEdges unmatched out edges of the pattern.
     * @param matchedTableMap            All matched tables come from the toMatchMap.
     */
    private void match0(JoinsGraph pattern, List<Pair<TableRef, TableRef>> toMatchTableList,
            List<Edge> unmatchedPatternOutEdges, Map<TableRef, TableRef> matchedTableMap) {

        // The toMatchTableList will add nodes to be matched in the loop.
        // Traverse toMatchTableList to match each table
        for (int i = 0; i < toMatchTableList.size(); i++) {

            Pair<TableRef, TableRef> pair = toMatchTableList.get(i);
            TableRef queryFKSide = pair.getFirst();
            TableRef patternFKSide = pair.getSecond();
            List<Edge> queryOutEdges = this.outwardEdges(queryFKSide);
            Set<Edge> patternOutEdges = Sets.newHashSet(pattern.outwardEdges(patternFKSide));

            // Traverse queryOutEdgesIter to match each edge from patternOutEdges
            // Edges that match successfully will be deleted from queryOutEdges and patternOutEdges
            // When all outgoing edges of the query table can be matched in the outgoing edges of the pattern table
            Iterator<Edge> queryOutEdgesIter = queryOutEdges.iterator();
            while (queryOutEdgesIter.hasNext()) {

                Edge queryOutEdge = queryOutEdgesIter.next();
                TableRef queryPKSide = queryOutEdge.otherSide(queryFKSide);
                Edge matchedPatternEdge = findOutEdgeFromDualTable(pattern, patternOutEdges, queryPKSide, queryOutEdge);
                boolean patternEdgeNotMatch = Objects.isNull(matchedPatternEdge);

                // query:   A leftOrInner B (left or inner -> leftOrInner)
                // pattern: A left B
                // edgeBA is not found in patternOutEdges, but this case can be matched
                if (queryOutEdge.isLeftOrInnerJoin() && (patternOutEdges.isEmpty() || patternEdgeNotMatch)) {
                    queryOutEdgesIter.remove();
                } else {
                    // can't find the same edge
                    if (patternEdgeNotMatch) {
                        break;
                    }
                    queryOutEdgesIter.remove();
                    patternOutEdges.remove(matchedPatternEdge);
                    // both out edge pk side tables add to toMatchTableList for the next round of the loop
                    TableRef patternPKSide = matchedPatternEdge.otherSide(patternFKSide);
                    addIfAbsent(toMatchTableList, Pair.newPair(queryPKSide, patternPKSide));
                }
            }

            // All queryOutEdges are not matched in patternOutEdges
            if (CollectionUtils.isNotEmpty(queryOutEdges)) {
                break;
            }
            // All queryOutEdges are successfully matched in patternOutEdges.
            // put queryFKSide and patternFKSide to matchedTableMap
            matchedTableMap.put(queryFKSide, patternFKSide);
            // The remaining edges in patternOutEdges are all put into unmatchedPatternOutEdges
            unmatchedPatternOutEdges.addAll(patternOutEdges);
        }
        // intersect the table to be matched with the table already matched
        val matchedList = matchedTableMap.entrySet().stream().map(e -> Pair.newPair(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        toMatchTableList.removeAll(matchedList);
    }

    /**
     * Find the out edge from the dual {@code TableRef}.
     *
     * @return the dual table's out edge in joinGraph of pattern.
     */
    private Edge findOutEdgeFromDualTable(JoinsGraph pattern, Set<Edge> patternOutEdges, TableRef queryPKSide,
            Edge queryOutEdge) {
        Set<Edge> matchedEdges = patternOutEdges.stream()
                .filter(outPatternEdge -> StringUtils.equals(queryPKSide.getTableIdentity(),
                        outPatternEdge.pkSide().getTableIdentity()) && queryOutEdge.equals(outPatternEdge))
                .collect(Collectors.toSet());
        if (matchedEdges.size() == 1) {
            return matchedEdges.iterator().next();
        }
        for (Edge matchedEdge : matchedEdges) {
            TableRef patternPKSide = matchedEdge.pkSide();
            int queryOutEdgeSize = this.vertexInfoMap.get(queryPKSide).outEdges.size();
            int patternOutEdgeSize = pattern.vertexInfoMap.get(patternPKSide).outEdges.size();
            if (queryOutEdgeSize != patternOutEdgeSize) {
                continue;
            }
            return matchedEdge;
        }
        return null;
    }

    /**
     * Sometimes one table may be joined more than one time.
     *
     * @param tableIdentity table identity(db.table_name) to search
     * @return desired references of the input table identity
     */
    private List<TableRef> searchCenter(String tableIdentity) {
        return vertexInfoMap.keySet().stream()
                .filter(table -> StringUtils.equals(table.getTableIdentity(), tableIdentity))
                .collect(Collectors.toList());
    }

    /**
     * Check if any non-equi join is missed in the pattern
     * if so, we cannot match the current graph with the pattern graph.
     * set `kylin.query.match-partial-non-equi-join-model` to skip this checking
     */
    public boolean checkNonEquiJoinMatches(Map<TableRef, TableRef> matches, JoinsGraph pattern) {
        for (Map.Entry<TableRef, VertexInfo<Edge>> entry : pattern.vertexInfoMap.entrySet()) {
            TableRef table = entry.getKey();
            List<Edge> outEdges = entry.getValue().outEdges;
            // for all outgoing non-equi join edges
            // if there is no match found for the right side table in the current graph
            // return false
            for (Edge outgoingEdge : outEdges) {
                if (outgoingEdge.isNonEquiJoin()
                        && (!matches.containsValue(table) || !matches.containsValue(outgoingEdge.pkSide()))) {
                    return false;
                }
            }
        }
        return true;
    }

    public List<TableRef> getAllTblRefNodes() {
        return Lists.newArrayList(vertexMap.values());
    }

    /**
     * Normalize joinsGraph. <p>
     * 1. Find a path to the Inner or LeftOrInner edge. <p>
     * 2. Recursively change all Left edges on the path to LeftOrInner edges.
     * <p>
     * Example:<p>
     * query1: A Left B, B Left C, C Inner D        =>  A LeftOrInner B, B LeftOrInner C, C Inner D <p>
     * query2: A Left B, B Left C, C LeftOrInner D  =>  A LeftOrInner B, B LeftOrInner C, C LeftOrInner D
     * <p>
     * If the PK table of Left Join has a non-null filter condition, then this Left Join has the same semantics as Inner Join <p>
     * query: A Left B ON A.a = B.b Where B.c1 IS NOT NULL   => A LeftOrInner B
     */
    public void normalize() {
        Set<Edge> edgeSet = edges.stream().filter(e -> !e.isSwapJoin()).collect(Collectors.toSet());
        for (Edge edge : edgeSet) {
            if (!edge.isLeftJoin() || edge.isLeftOrInnerJoin()) {
                TableRef fkSide = edge.fkSide();
                List<Edge> edgeList = inwardEdges(fkSide).stream().filter(e -> !e.isSwapJoin())
                        .collect(Collectors.toList());
                if (CollectionUtils.isEmpty(edgeList)) {
                    continue;
                }
                for (Edge targetEdge : edgeList) {
                    if (!edge.equals(targetEdge) && fkSide.equals(targetEdge.pkSide())
                            && !targetEdge.isLeftOrInnerJoin()) {
                        setJoinToLeftOrInner(targetEdge.join);
                        normalize();
                    }
                }
            }
        }
    }

    public void setJoinToLeftOrInner(JoinDesc join) {
        if (!join.isLeftJoin()) {
            join.setLeftOrInner(true);
            // inner -> leftOrInner, need swap another join
            Edge swapEdge = edges.stream().filter(e -> e.isJoinMatched(swapJoinDesc(join))).findFirst().orElse(null);
            if (swapEdge == null) {
                return;
            }
            swapEdge.join.setLeftOrInner(true);
            return;
        }

        Edge edge = edges.stream().filter(e -> e.isJoinMatched(join)).findFirst().orElse(null);
        if (edge == null) {
            return;
        }

        // change current join from left join to leftOrInner join
        join.setLeftOrInner(true);
        // left -> leftOrInner need swap join to create new edge
        JoinDesc swapJoin = swapJoinDesc(join);
        Edge swapEdge = new Edge(swapJoin, true);
        vertexInfoMap.computeIfAbsent(swapEdge.pkSide(), f -> new VertexInfo<>());
        vertexInfoMap.computeIfAbsent(swapEdge.fkSide(), f -> new VertexInfo<>());
        addIfAbsent(vertexInfoMap.get(swapEdge.fkSide()).outEdges, swapEdge);
        addIfAbsent(vertexInfoMap.get(swapEdge.pkSide()).inEdges, swapEdge);
        if (edges.stream().noneMatch(e -> e.isJoinMatched(swapJoin))) {
            edges.add(swapEdge);
        }
    }

    public Map<String, String> matchAlias(JoinsGraph joinsGraph, KylinConfig kylinConfig) {
        Map<String, String> matchAliasMap = Maps.newHashMap();
        match(joinsGraph, matchAliasMap, kylinConfig.isQueryMatchPartialInnerJoinModel(),
                kylinConfig.partialMatchNonEquiJoins());
        return matchAliasMap;
    }

    public Map<String, String> matchAlias(JoinsGraph joinsGraph, boolean matchPartial) {
        Map<String, String> matchAliasMap = Maps.newHashMap();
        match(joinsGraph, matchAliasMap, matchPartial);
        return matchAliasMap;
    }

    public JoinDesc getJoinByPKSide(TableRef pkTable) {
        Edge edge = getEdgeByPKSide(pkTable);
        return Objects.nonNull(edge) ? edge.join : null;
    }

    public List<Edge> getEdgesByFKSide(TableRef table) {
        if (!vertexInfoMap.containsKey(table)) {
            return Collections.emptyList();
        }
        return outwardEdges(table);
    }

    /**
     * Get edges by primary key side at most get one edge.
     *
     * @param pkTable the pkSide table
     * @return the obtained edge
     */
    private Edge getEdgeByPKSide(TableRef pkTable) {
        if (!vertexInfoMap.containsKey(pkTable)) {
            return null;
        }
        List<Edge> inEdges = inwardEdges(pkTable).stream().filter(edge -> !edge.isSwapJoin())
                .collect(Collectors.toList());
        if (inEdges.size() != 1) {
            return null;
        }
        return inEdges.get(0);
    }

    public List<JoinDesc> getJoinsPathByPKSide(TableRef table) {
        List<JoinDesc> pathToRoot = Lists.newArrayList();
        TableRef pkSide = table;
        while (pkSide != null) {
            JoinDesc subJoin = getJoinByPKSide(pkSide);
            if (Objects.isNull(subJoin)) {
                pkSide = null;
            } else {
                pathToRoot.add(subJoin);
                pkSide = subJoin.getFKSide();
            }
        }
        return Lists.reverse(pathToRoot);
    }

    public JoinsGraph getSubGraphByAlias(Set<String> aliasSets) {
        Set<JoinDesc> subJoins = Sets.newHashSet();
        for (String alias : aliasSets) {
            TableRef table = vertexMap.get(alias);
            subJoins.addAll(getJoinsPathByPKSide(table));
        }
        return new JoinsGraph(this.center, Lists.newArrayList(subJoins));
    }

    public List<Edge> unmatched(JoinsGraph pattern) {
        List<Edge> unmatched = Lists.newArrayList();
        Set<Edge> all = vertexInfoMap.values().stream().map(m -> m.outEdges).flatMap(List::stream)
                .collect(Collectors.toSet());
        for (Edge edge : all) {
            JoinsGraph subGraph = getSubGraphByAlias(Sets.newHashSet(edge.pkSide().getAlias()));
            if (!subGraph.match(pattern, Maps.newHashMap())) {
                unmatched.add(edge);
            }
        }
        return unmatched;
    }

    /**
     * Root: TableRef[TEST_KYLIN_FACT]
     *   Edge: TableRef[TEST_KYLIN_FACT] LEFT JOIN TableRef[TEST_ORDER] ON [ORDER_ID] = [ORDER_ID]
     *     Edge: TableRef[TEST_ORDER] LEFT JOIN TableRef[BUYER_ACCOUNT:TEST_ACCOUNT] ON [BUYER_ID] = [ACCOUNT_ID]
     *   Edge: TableRef[TEST_KYLIN_FACT] LEFT JOIN TableRef[TEST_SELLER_TYPE_DIM] ON [SLR_SEGMENT_CD] = [SELLER_TYPE_CD]
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Root: ").append(center);
        // build next edges
        getEdgesByFKSide(center).forEach(e -> buildGraphStr(sb, e, 1));
        return sb.toString();
    }

    private void buildGraphStr(StringBuilder sb, @NonNull Edge edge, int indent) {
        sb.append(IntStream.range(0, indent).mapToObj(i -> "  ")
                .collect(Collectors.joining("", "\n", String.valueOf(edge))));
        // build next edges
        getEdgesByFKSide(edge.pkSide()).forEach(e -> buildGraphStr(sb, e, indent + 1));
    }

    private <T> void addIfAbsent(List<T> edges, T edge) {
        if (edges.contains(edge)) {
            return;
        }
        edges.add(edge);
    }
}
