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

package org.apache.kylin.query.routing.rules;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.MultiPartitionKeyMapping;
import org.apache.kylin.metadata.model.MultiPartitionKeyMappingImpl;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RoutingRule;
import org.apache.kylin.query.util.RexUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PartitionPruningRule extends RoutingRule {

    private static final String NEED_PUSH_DOWN = "NULL";

    @Override
    public void apply(Candidate candidate) {
        if (nonBatchRealizationSkipPartitionsPruning(candidate)) {
            log.info("{}({}/{}): only batch model support multi-partitions pruning.", this.getClass().getName(),
                    candidate.getRealization().getProject(), candidate.getRealization().getCanonicalName());
            return;
        }

        if (noQueryableSegmentsCanAnswer(candidate)) {
            log.debug("{}({}/{}): no queryable(READY|WARNING) segments can answer", this.getClass().getName(),
                    candidate.getRealization().getProject(), candidate.getRealization().getCanonicalName());
            return;
        }

        if (noMultiPartitionColumnsExist(candidate)) {
            log.debug("{}({}/{}): there is no multi-partition columns.", this.getClass().getName(),
                    candidate.getRealization().getProject(), candidate.getRealization().getCanonicalName());
            return;
        }

        NDataModel model = candidate.getRealization().getModel();
        Map<String, List<Long>> matchedPartitions = matchPartitions(candidate);
        if (needPushDown(matchedPartitions)) {
            log.debug("{}({}/{}): cannot match multi-partitions of segments.", this.getClass().getName(),
                    candidate.getRealization().getProject(), candidate.getRealization().getCanonicalName());
            CapabilityResult capability = new CapabilityResult();
            capability.setCapable(false);
            candidate.setCapability(capability);
            return;
        }

        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .getDataflow(model.getId());
        boolean allPartitionsEmpty = matchedPartitions.entrySet().stream().allMatch(entry -> {
            NDataSegment segment = dataflow.getSegment(entry.getKey());
            List<Long> partitionIds = entry.getValue();
            return CollectionUtils.isNotEmpty(segment.getMultiPartitionIds()) && CollectionUtils.isEmpty(partitionIds);
        });

        // return empty result
        if (allPartitionsEmpty) {
            log.info("there is no sub-partitions to answer sql");
            CapabilityResult capability = new CapabilityResult();
            capability.setCapable(true);
            capability.setSelectedCandidate(NLayoutCandidate.EMPTY);
            candidate.setCapability(capability);
            return;
        }
        candidate.setPrunedPartitions(matchedPartitions);
    }

    private boolean needPushDown(Map<String, List<Long>> matchedPartitions) {
        return matchedPartitions.size() == 1 && matchedPartitions.containsKey(NEED_PUSH_DOWN);
    }

    private boolean noMultiPartitionColumnsExist(Candidate candidate) {
        NDataModel model = candidate.getRealization().getModel();
        MultiPartitionDesc multiPartitionDesc = model.getMultiPartitionDesc();
        return multiPartitionDesc == null || CollectionUtils.isEmpty(multiPartitionDesc.getColumns());
    }

    private boolean noQueryableSegmentsCanAnswer(Candidate candidate) {
        return CollectionUtils.isEmpty(candidate.getQueryableSeg().getBatchSegments());
    }

    private boolean nonBatchRealizationSkipPartitionsPruning(Candidate candidate) {
        return CollectionUtils.isNotEmpty(candidate.getQueryableSeg().getStreamingSegments());
    }

    private Map<String, List<Long>> matchPartitions(Candidate candidate) {
        NDataModel model = candidate.getRealization().getModel();
        OLAPContext olapContext = candidate.getCtx();

        Map<String, List<Long>> segPartitionMap = candidate.getQueryableSeg().getBatchSegments().stream()
                .collect(Collectors.toMap(NDataSegment::getId, NDataSegment::getMultiPartitionIds));
        if (filtersContainPartOfMultiPartitionKeyMappingCols(model, olapContext.filterColumns)) {
            return segPartitionMap;
        }

        RelOptCluster relOptCluster = olapContext.firstTableScan.getCluster();
        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexSimplify rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(), RelOptPredicateList.EMPTY, true,
                relOptCluster.getPlanner().getExecutor());
        RexNode simplifiedFilters = rexSimplify.simplifyAnds(olapContext.getExpandedFilterConditions());
        if (simplifiedFilters.isAlwaysFalse()) {
            log.info("The SQL filter condition is always false, and all partitions are filtered out.");
            return Maps.newHashMap();
        }
        if (simplifiedFilters.isAlwaysTrue()) {
            log.info("The SQL filter condition is always true, and all partitions are reserved.");
            return segPartitionMap;
        }

        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .getDataflow(model.getId());
        List<TblColRef> partitionColRefs = model.getMultiPartitionDesc().getColumnRefs();
        for (MultiPartitionDesc.PartitionInfo partition : model.getMultiPartitionDesc().getPartitions()) {
            try {
                RexNode partitionRex = partitionToRexCall(partitionColRefs, partition.getValues(), rexBuilder,
                        olapContext.allTableScans);
                RexNode mappingColRex = multiPartitionKeyMappingToRex(rexBuilder, partition.getValues(),
                        model.getMultiPartitionKeyMapping(), olapContext.allTableScans);

                // simplifyAnds method can handle NOT_EQUAL operation
                List<RexNode> nodes = Lists.newArrayList(simplifiedFilters, partitionRex, mappingColRex);
                RexNode simplifyAnds = rexSimplify.simplifyAnds(nodes);
                RelOptPredicateList predicate = RelOptPredicateList.of(rexBuilder,
                        Lists.newArrayList(partitionRex, mappingColRex));

                // simplifyWithPredicates can handle OR operation
                RexNode simplifiedWithPredicate = rexSimplify.withPredicates(predicate).simplify(simplifiedFilters);

                if (simplifyAnds.isAlwaysFalse() || simplifiedWithPredicate.isAlwaysFalse()) {
                    // prune this partition
                    segPartitionMap.forEach((dataSegment, partitionIds) -> partitionIds.remove(partition.getId()));
                    continue;
                }
            } catch (Exception ex) {
                log.warn("Multi-partition pruning error: ", ex);
            }

            // if any segment does not contain this selected model-defined partition, push down
            for (Map.Entry<String, List<Long>> entry : segPartitionMap.entrySet()) {
                List<Long> partitionIds = entry.getValue();
                if (!partitionIds.contains(partition.getId())) {
                    NDataSegment segment = dataflow.getSegment(entry.getKey());
                    log.info("segment {} does not have partition {}", segment.displayIdName(), partition.getId());
                    return ImmutableMap.of(NEED_PUSH_DOWN, Lists.newArrayList());
                }
            }
        }

        return segPartitionMap;
    }

    private RexNode partitionToRexCall(List<TblColRef> partitionCols, String[] partitionValues, RexBuilder rexBuilder,
            Set<OLAPTableScan> tableScans) {
        return transformColumns2RexCall(partitionCols, Collections.singletonList(Lists.newArrayList(partitionValues)),
                rexBuilder, tableScans);
    }

    private RexNode multiPartitionKeyMappingToRex(RexBuilder rexBuilder, String[] partitionValues,
            MultiPartitionKeyMapping multiPartitionKeyMapping, Set<OLAPTableScan> tableScans) {
        if (multiPartitionKeyMapping == null) {
            return rexBuilder.makeLiteral(true);
        }
        List<TblColRef> mappedColumns = multiPartitionKeyMapping.getAliasColumns();
        Collection<List<String>> mappedValues = multiPartitionKeyMapping
                .getAliasValue(Lists.newArrayList(partitionValues));
        if (CollectionUtils.isEmpty(mappedColumns) || CollectionUtils.isEmpty(mappedValues)) {
            return rexBuilder.makeLiteral(true);
        }
        return transformColumns2RexCall(mappedColumns, mappedValues, rexBuilder, tableScans);
    }

    private RexNode transformColumns2RexCall(List<TblColRef> columns, Collection<List<String>> values,
            RexBuilder rexBuilder, Set<OLAPTableScan> tableScans) {
        List<RexNode> orRexCalls = Lists.newArrayList();
        for (List<String> columnValue : values) {
            int size = columns.size();
            List<RexNode> equalRexCalls = Lists.newArrayList();
            for (int i = 0; i < size; i++) {
                String value = columnValue.get(i);
                TblColRef columnRef = columns.get(i);
                RexInputRef columnRexInputRef = RexUtils.transformColumn2RexInputRef(columnRef, tableScans);
                RexNode valueLiteral = RexUtils.transformValue2RexLiteral(rexBuilder, value, columnRef.getType());
                RexNode equalRexCall = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        Lists.newArrayList(columnRexInputRef, valueLiteral));
                equalRexCalls.add(equalRexCall);
            }

            RexNode andRexCall = equalRexCalls.size() == 1 ? equalRexCalls.get(0)
                    : rexBuilder.makeCall(SqlStdOperatorTable.AND, equalRexCalls);
            orRexCalls.add(andRexCall);
        }
        return orRexCalls.size() == 1 ? orRexCalls.get(0) : rexBuilder.makeCall(SqlStdOperatorTable.OR, orRexCalls);
    }

    private boolean filtersContainPartOfMultiPartitionKeyMappingCols(NDataModel model, Set<TblColRef> filterCols) {
        if (filterCols.containsAll(model.getMultiPartitionDesc().getColumnRefs())) {
            return false;
        }

        if (model.isEmptyMultiPartitionKeyMapping()) {
            return true;
        }

        MultiPartitionKeyMappingImpl mapping = model.getMultiPartitionKeyMapping();
        Set<String> filterColumnIdentities = filterCols.stream().map(TblColRef::getCanonicalName)
                .collect(Collectors.toSet());
        Set<String> aliasColumnIdentities = mapping.getAliasColumns().stream().map(TblColRef::getCanonicalName)
                .collect(Collectors.toSet());
        return !filterColumnIdentities.containsAll(aliasColumnIdentities);
    }
}
