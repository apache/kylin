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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

public class JoinsGraph implements Serializable {

    public class Edge implements Serializable {

        @Getter
        private final JoinDesc join;
        private final ColumnDesc[] leftCols;
        private final ColumnDesc[] rightCols;
        private final NonEquiJoinCondition nonEquiJoinCondition;

        private Edge(JoinDesc join) {
            this.join = join;

            leftCols = new ColumnDesc[join.getForeignKeyColumns().length];
            int i = 0;
            for (TblColRef colRef : join.getForeignKeyColumns()) {
                leftCols[i++] = colRef.getColumnDesc();
            }

            rightCols = new ColumnDesc[join.getPrimaryKeyColumns().length];
            i = 0;
            for (TblColRef colRef : join.getPrimaryKeyColumns()) {
                rightCols[i++] = colRef.getColumnDesc();
            }

            nonEquiJoinCondition = join.getNonEquiJoinCondition();
        }

        public boolean isJoinMatched(JoinDesc other) {
            return join.equals(other);
        }

        public boolean isNonEquiJoin() {
            return nonEquiJoinCondition != null;
        }

        public boolean isLeftJoin() {
            return !join.isLeftOrInnerJoin() && join.isLeftJoin();
        }

        public boolean isLeftOrInnerJoin() {
            return join.isLeftOrInnerJoin();
        }

        public boolean isInnerJoin() {
            return !join.isLeftOrInnerJoin() && join.isInnerJoin();
        }

        private TableRef left() {
            return join.getFKSide();
        }

        private TableRef right() {
            return join.getPKSide();
        }

        private boolean isFkSide(TableRef tableRef) {
            return join.getFKSide().equals(tableRef);
        }

        private boolean isPkSide(TableRef tableRef) {
            return join.getPKSide().equals(tableRef);
        }

        private TableRef other(TableRef tableRef) {
            if (left().equals(tableRef)) {
                return right();
            } else if (right().equals(tableRef)) {
                return left();
            }
            throw new IllegalArgumentException("table " + tableRef + " is not on the edge " + this);
        }

        @Override
        public boolean equals(Object that) {
            if (that == null)
                return false;

            if (this.getClass() != that.getClass())
                return false;

            return joinEdgeMatcher.matches(this, (Edge) that);
        }

        @Override
        public int hashCode() {
            if (this.isLeftJoin()) {
                return Objects.hash(isLeftJoin(), leftCols, rightCols);
            } else {
                if (Arrays.hashCode(leftCols) < Arrays.hashCode(rightCols)) {
                    return Objects.hash(isLeftJoin(), leftCols, rightCols);
                } else {
                    return Objects.hash(isLeftJoin(), rightCols, leftCols);
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Edge: ");
            sb.append(left())
                    .append(isLeftJoin() ? " LEFT JOIN "
                            : isLeftOrInnerJoin() ? " LEFT OR INNER JOIN " : " INNER JOIN ")
                    .append(right()).append(" ON ")
                    .append(Arrays.toString(Arrays.stream(leftCols).map(ColumnDesc::getName).toArray())).append(" = ")
                    .append(Arrays.toString(Arrays.stream(rightCols).map(ColumnDesc::getName).toArray()));
            return sb.toString();
        }
    }

    private Edge edgeOf(JoinDesc join) {
        return new Edge(join);
    }

    private static final IJoinEdgeMatcher DEFAULT_JOIN_EDGE_MATCHER = new DefaultJoinEdgeMatcher();
    @Setter
    private IJoinEdgeMatcher joinEdgeMatcher = DEFAULT_JOIN_EDGE_MATCHER;

    /**
     * compare:
     * 1. JoinType
     * 2. Columns on both sides
     */
    public interface IJoinEdgeMatcher extends Serializable {
        boolean matches(@NonNull Edge join1, @NonNull Edge join2);
    }

    public static class DefaultJoinEdgeMatcher implements IJoinEdgeMatcher {
        @Override
        public boolean matches(@NonNull Edge join1, @NonNull Edge join2) {
            if (join1.isLeftJoin() != join2.isLeftJoin() && !join1.isLeftOrInnerJoin() && !join2.isLeftOrInnerJoin()) {
                return false;
            }

            if (!Objects.equals(join1.nonEquiJoinCondition, join2.nonEquiJoinCondition)) {
                return false;
            }

            if (join1.isLeftJoin()) {
                return columnDescEquals(join1.leftCols, join2.leftCols)
                        && columnDescEquals(join1.rightCols, join2.rightCols);
            } else {
                return (columnDescEquals(join1.leftCols, join2.leftCols)
                        && columnDescEquals(join1.rightCols, join2.rightCols))
                        || (columnDescEquals(join1.leftCols, join2.rightCols)
                                && columnDescEquals(join1.rightCols, join2.leftCols));
            }
        }

        private boolean columnDescEquals(ColumnDesc[] a, ColumnDesc[] b) {
            if (a.length != b.length)
                return false;

            for (int i = 0; i < a.length; i++) {
                if (!columnDescEquals(a[i], b[i]))
                    return false;
            }
            return true;
        }

        protected boolean columnDescEquals(ColumnDesc a, ColumnDesc b) {
            return Objects.equals(a, b);
        }
    }

    @Getter
    private final TableRef center;
    private final Map<String, TableRef> nodes = new HashMap<>();
    private final Set<Edge> edges = new HashSet<>();
    private final Map<TableRef, List<Edge>> edgesFromNode = new HashMap<>();
    private final Map<TableRef, List<Edge>> edgesToNode = new HashMap<>();

    /**
     * For model there's always a center, if there's only one tableScan it's the center.
     * Otherwise the center is not determined, it's a linked graph, hard to tell the center.
     */
    public JoinsGraph(TableRef root, List<JoinDesc> joins) {
        this.center = root;
        addNode(root);

        for (JoinDesc join : joins) {
            Preconditions.checkState(Arrays.stream(join.getForeignKeyColumns()).allMatch(TblColRef::isQualified));
            Preconditions.checkState(Arrays.stream(join.getPrimaryKeyColumns()).allMatch(TblColRef::isQualified));
            addAsEdge(join);
        }

        validate(joins);
    }

    private void addNode(TableRef table) {
        Preconditions.checkNotNull(table);
        String alias = table.getAlias();
        TableRef node = nodes.get(alias);
        if (node != null) {
            Preconditions.checkArgument(node.equals(table), "[%s]'s Alias \"%s\" has conflict with [%s].", table, alias,
                    node);
        } else {
            nodes.put(alias, table);
        }
    }

    private void addAsEdge(JoinDesc join) {
        TableRef fkTable = join.getFKSide();
        TableRef pkTable = join.getPKSide();
        addNode(pkTable);

        Edge edge = edgeOf(join);
        edgesFromNode.computeIfAbsent(fkTable, fk -> Lists.newArrayList());
        edgesFromNode.get(fkTable).add(edge);
        edgesToNode.computeIfAbsent(pkTable, pk -> Lists.newArrayList());
        edgesToNode.get(pkTable).add(edge);
        if (!edge.isLeftJoin()) {
            // inner join is reversible
            edgesFromNode.computeIfAbsent(pkTable, pk -> Lists.newArrayList());
            edgesFromNode.get(pkTable).add(edge);
            edgesToNode.computeIfAbsent(fkTable, fk -> Lists.newArrayList());
            edgesToNode.get(fkTable).add(edge);
        }
        edges.add(edge);
    }

    public void setJoinToLeftOrInner(JoinDesc join) {
        if (!join.isLeftJoin()) {
            join.setLeftOrInner(true);
            return;
        }

        join.setLeftOrInner(true);
        TableRef fkTable = join.getFKSide();
        TableRef pkTable = join.getPKSide();
        Edge edge = edges.stream().filter(e -> e.isJoinMatched(join)).findFirst().orElse(null);
        if (edge == null) {
            return;
        }
        edgesFromNode.computeIfAbsent(pkTable, pk -> Lists.newArrayList());
        edgesFromNode.get(pkTable).add(edge);
        edgesToNode.computeIfAbsent(fkTable, fk -> Lists.newArrayList());
        edgesToNode.get(fkTable).add(edge);
    }

    private void validate(List<JoinDesc> joins) {
        for (JoinDesc join : joins) {
            TableRef fkTable = join.getFKSide();
            Preconditions.checkNotNull(nodes.get(fkTable.getAlias()));
            Preconditions.checkState(nodes.get(fkTable.getAlias()).equals(fkTable));
        }
        Preconditions.checkState(nodes.size() == joins.size() + 1);
    }

    public boolean match(JoinsGraph pattern, Map<String, String> matchAlias) {
        return match(pattern, matchAlias, false);
    }

    public boolean match(JoinsGraph pattern, Map<String, String> matchAlias, boolean matchPatial) {
        return match(pattern, matchAlias, matchPatial, false);
    }

    public boolean match(JoinsGraph pattern, Map<String, String> matchAlias, boolean matchPatial,
            boolean matchPartialNonEquiJoin) {
        if (pattern == null || pattern.center == null) {
            throw new IllegalArgumentException("pattern(model) should have a center: " + pattern);
        }

        List<TableRef> candidatesOfQCenter = searchCenterByIdentity(pattern.center);
        if (CollectionUtils.isEmpty(candidatesOfQCenter)) {
            return false;
        }

        for (TableRef queryCenter : candidatesOfQCenter) {
            // query <-> pattern
            Map<TableRef, TableRef> trialMatch = Maps.newHashMap();
            trialMatch.put(queryCenter, pattern.center);

            if (!checkInnerJoinNum(pattern, queryCenter, pattern.center, matchPatial)) {
                continue;
            }

            AtomicReference<Map<TableRef, TableRef>> finalMatchRef = new AtomicReference<>();
            innerMatch(pattern, trialMatch, matchPatial, finalMatchRef);
            if (finalMatchRef.get() != null
                    && (matchPartialNonEquiJoin || checkNonEquiJoinMatches(finalMatchRef.get(), pattern))) {
                matchAlias.clear();
                matchAlias.putAll(finalMatchRef.get().entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getAlias(), e -> e.getValue().getAlias())));
                return true;
            }
        }
        return false;
    }

    public static JoinsGraph normalizeJoinGraph(JoinsGraph joinsGraph) {
        for (Edge edge : joinsGraph.edges) {
            if (!edge.isLeftJoin() || edge.isLeftOrInnerJoin()) {
                TableRef leftTable = edge.left();
                List<Edge> edgeList = joinsGraph.edgesToNode.get(leftTable);
                if (CollectionUtils.isEmpty(edgeList)) {
                    continue;
                }
                for (Edge targetEdge : edgeList) {
                    if (!edge.equals(targetEdge) && leftTable.equals(targetEdge.right())
                            && !targetEdge.isLeftOrInnerJoin()) {
                        joinsGraph.setJoinToLeftOrInner(targetEdge.join);
                        normalizeJoinGraph(joinsGraph);
                    }
                }
            }
        }
        return joinsGraph;
    }

    public List<TableRef> getAllTblRefNodes() {
        return Lists.newArrayList(nodes.values());
    }

    /**
     * check if any non-equi join is missed in the pattern
     * if so, we cannot match the current graph with the the pattern graph.
     * set `kylin.query.match-partial-non-equi-join-model` to skip this checking
     * @param matches
     * @return
     */
    private boolean checkNonEquiJoinMatches(Map<TableRef, TableRef> matches, JoinsGraph pattern) {
        HashSet<TableRef> patternGraphTables = new HashSet<>(pattern.nodes.values());

        for (TableRef patternTable : patternGraphTables) {
            List<Edge> outgoingEdges = pattern.getEdgesByFKSide(patternTable);
            // for all outgoing non-equi join edges
            // if there is no match found for the right side table in the current graph
            // return false
            for (Edge outgoingEdge : outgoingEdges) {
                if (outgoingEdge.isNonEquiJoin()) {
                    if (!matches.containsValue(patternTable) || !matches.containsValue(outgoingEdge.right())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean isAllJoinInner(JoinsGraph joinsGraph, TableRef tableRef) {
        List<Edge> edgesFromNode = joinsGraph.edgesFromNode.get(tableRef);
        List<Edge> edgesToNode = joinsGraph.edgesToNode.get(tableRef);

        if (edgesFromNode == null) {
            return false;
        }

        if (edgesToNode == null) {
            return false;
        }

        if (edgesToNode.size() != edgesFromNode.size()) {
            return false;
        }

        for (Edge edge : edgesFromNode) {
            if (edge.join.isLeftJoin()) {
                return false;
            }
        }

        return true;
    }

    private boolean checkInnerJoinNum(JoinsGraph pattern, TableRef queryTableRef, TableRef patternTableRef,
            boolean matchPartial) {
        if (matchPartial) {
            return true;
        }
        // fully match: unmatched if extra inner join edge on either graph
        //  matched if:   query graph join count:   model graph join count:
        //  1)                        inner join <= inner join
        //  2)   inner join + left or inner join >= inner join
        List<Edge> innerQueryEdges = this.edgesFrom(queryTableRef).stream().filter(Edge::isInnerJoin)
                .collect(Collectors.toList());
        List<Edge> notLeftQueryEdges = this.edgesFrom(queryTableRef).stream().filter(e -> !e.isLeftJoin())
                .collect(Collectors.toList());
        List<Edge> innerPatternEdges = pattern.edgesFrom(patternTableRef).stream().filter(Edge::isInnerJoin)
                .collect(Collectors.toList());

        // if all joins are inner joins, compare sum of both sides
        if (isAllJoinInner(this, queryTableRef) && isAllJoinInner(pattern, patternTableRef)) {
            int cntInnerQueryEdges = innerQueryEdges.size();
            int cntNotLeftQueryEdges = notLeftQueryEdges.size();
            int cntInnerPatternEdges = innerPatternEdges.size();
            return cntInnerQueryEdges <= cntInnerPatternEdges && cntNotLeftQueryEdges >= cntInnerPatternEdges;
        }

        // if not all joins are inner, compare left side and right side separately
        //  Calculate join count in query graph
        int cntLeftSideInnerQueryEdges = (int) innerQueryEdges.stream()
                .filter(edge -> edge.right().equals(queryTableRef)).count();
        int cntRightSideInnerQueryEdges = (int) innerQueryEdges.stream()
                .filter(edge -> edge.left().equals(queryTableRef)).count();
        int cntLeftSideNotLeftQueryEdges = (int) notLeftQueryEdges.stream()
                .filter(edge -> edge.right().equals(queryTableRef)).count();
        int cntRightSideNotLeftQueryEdges = (int) notLeftQueryEdges.stream()
                .filter(edge -> edge.left().equals(queryTableRef)).count();
        // Calculate join count in model graph
        int cntLeftSideInnerPatternEdges = (int) innerPatternEdges.stream()
                .filter(edge -> edge.right().equals(patternTableRef)).count();
        int cntRightSideInnerPatternEdges = (int) innerPatternEdges.stream()
                .filter(edge -> edge.left().equals(patternTableRef)).count();

        boolean isLeftEqual = cntLeftSideInnerQueryEdges <= cntLeftSideInnerPatternEdges
                && cntLeftSideNotLeftQueryEdges >= cntLeftSideInnerPatternEdges;
        boolean isRightEqual = cntRightSideInnerQueryEdges <= cntRightSideInnerPatternEdges
                && cntRightSideNotLeftQueryEdges >= cntRightSideInnerPatternEdges;
        return isLeftEqual && isRightEqual;
    }

    private void innerMatch(JoinsGraph pattern, Map<TableRef, TableRef> trialMatches, boolean matchPartial,
            AtomicReference<Map<TableRef, TableRef>> finalMatch) {
        if (trialMatches.size() == nodes.size()) {
            //match is found
            finalMatch.set(trialMatches);
            return;
        }

        Preconditions.checkState(nodes.size() > trialMatches.size());
        Optional<Pair<Edge, TableRef>> toMatch = trialMatches.keySet().stream()
                .map(t -> edgesFrom(t).stream().filter(e -> !trialMatches.containsKey(e.other(t))).findFirst()
                        .map(edge -> new Pair<>(edge, edge.other(t))).orElse(null))
                .filter(Objects::nonNull).findFirst();

        Preconditions.checkState(toMatch.isPresent());
        Edge toMatchQueryEdge = toMatch.get().getFirst();
        TableRef toMatchQueryNode = toMatch.get().getSecond();
        TableRef matchedQueryNode = toMatchQueryEdge.other(toMatchQueryNode);
        TableRef matchedPatternNode = trialMatches.get(matchedQueryNode);

        List<TableRef> toMatchPatternNodeCandidates = Lists.newArrayList();
        for (Edge patternEdge : pattern.edgesFrom(matchedPatternNode)) {
            TableRef toMatchPatternNode = patternEdge.other(matchedPatternNode);
            if (!toMatchQueryNode.getTableIdentity().equals(toMatchPatternNode.getTableIdentity())
                    || !toMatchQueryEdge.equals(patternEdge) || trialMatches.containsValue(toMatchPatternNode)
                    || !checkInnerJoinNum(pattern, toMatchQueryNode, toMatchPatternNode, matchPartial)) {
                continue;
            }
            toMatchPatternNodeCandidates.add(toMatchPatternNode);
        }

        for (TableRef toMatchPatternNode : toMatchPatternNodeCandidates) {
            Map<TableRef, TableRef> newTrialMatches = Maps.newHashMap();
            newTrialMatches.putAll(trialMatches);
            newTrialMatches.put(toMatchQueryNode, toMatchPatternNode);
            innerMatch(pattern, newTrialMatches, matchPartial, finalMatch);
            if (finalMatch.get() != null) {
                //get out of recursive invoke chain straightly
                return;
            }
        }
    }

    public List<Edge> unmatched(JoinsGraph pattern) {
        List<Edge> unmatched = Lists.newArrayList();
        Set<Edge> all = edgesFromNode.values().stream().flatMap(List::stream).collect(Collectors.toSet());
        for (Edge edge : all) {
            List<JoinDesc> joins = getJoinsPathByPKSide(edge.right());
            JoinsGraph subGraph = new JoinsGraph(center, joins);
            if (subGraph.match(pattern, Maps.newHashMap())) {
                continue;
            }
            unmatched.add(edge);
        }
        return unmatched;
    }

    private List<TableRef> searchCenterByIdentity(final TableRef table) {
        // special case: several same nodes in a JoinGraph
        return nodes.values().stream().filter(node -> node.getTableIdentity().equals(table.getTableIdentity()))
                .filter(node -> {
                    List<JoinDesc> path2Center = getJoinsPathByPKSide(node);
                    return path2Center.stream().noneMatch(JoinDesc::isLeftJoin);
                }).collect(Collectors.toList());
    }

    private List<Edge> edgesFrom(TableRef thisSide) {
        return edgesFromNode.getOrDefault(thisSide, Lists.newArrayList());
    }

    public Map<String, String> matchAlias(JoinsGraph joinsGraph, KylinConfig kylinConfig) {
        Map<String, String> matchAlias = Maps.newHashMap();
        match(joinsGraph, matchAlias, kylinConfig.isQueryMatchPartialInnerJoinModel(),
                kylinConfig.partialMatchNonEquiJoins());
        return matchAlias;
    }

    public Map<String, String> matchAlias(JoinsGraph joinsGraph, boolean matchPartial) {
        Map<String, String> matchAlias = Maps.newHashMap();
        match(joinsGraph, matchAlias, matchPartial);
        return matchAlias;
    }

    public List<Edge> getEdgesByFKSide(TableRef table) {
        if (!edgesFromNode.containsKey(table)) {
            return Lists.newArrayList();
        }
        return edgesFromNode.get(table).stream().filter(e -> e.isFkSide(table)).collect(Collectors.toList());
    }

    private Edge getEdgeByPKSide(TableRef table) {
        if (!edgesToNode.containsKey(table)) {
            return null;
        }
        List<Edge> edgesByPkSide = edgesToNode.get(table).stream().filter(e -> e.isPkSide(table))
                .collect(Collectors.toList());
        if (edgesByPkSide.isEmpty()) {
            return null;
        }
        Preconditions.checkState(edgesByPkSide.size() == 1, "%s is allowed to be Join PK side once", table);
        return edgesByPkSide.get(0);
    }

    public JoinDesc getJoinByPKSide(TableRef table) {
        Edge edge = getEdgeByPKSide(table);
        return edge != null ? edge.join : null;
    }

    private List<JoinDesc> getJoinsPathByPKSide(TableRef table) {
        List<JoinDesc> pathToRoot = Lists.newArrayList();
        TableRef pkSide = table; // start from leaf
        while (pkSide != null) {
            JoinDesc subJoin = getJoinByPKSide(pkSide);
            if (subJoin != null) {
                pathToRoot.add(subJoin);
                pkSide = subJoin.getFKSide();
            } else {
                pkSide = null;
            }
        }
        return Lists.reverse(pathToRoot);
    }

    public JoinsGraph getSubgraphByAlias(Set<String> aliasSets) {
        TableRef subGraphRoot = this.center;
        Set<JoinDesc> subGraphJoin = Sets.newHashSet();
        for (String alias : aliasSets) {
            subGraphJoin.addAll(getJoinsPathByPKSide(nodes.get(alias)));
        }
        return new JoinsGraph(subGraphRoot, Lists.newArrayList(subGraphJoin));
    }

    @Override
    public String toString() {
        StringBuilder graphStrBuilder = new StringBuilder();
        graphStrBuilder.append("Root: ").append(center);
        List<Edge> nextEdges = getEdgesByFKSide(center);
        nextEdges.forEach(e -> buildGraphStr(graphStrBuilder, e, 1));
        return graphStrBuilder.toString();
    }

    private void buildGraphStr(StringBuilder sb, @NonNull Edge edge, int indent) {
        sb.append('\n');
        for (int i = 0; i < indent; i++) {
            sb.append("  ");
        }
        sb.append(edge);
        List<Edge> nextEdges = getEdgesByFKSide(edge.right());
        nextEdges.forEach(e -> buildGraphStr(sb, e, indent + 1));
    }
}
