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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.MultiPartitionKeyMapping;
import org.apache.kylin.metadata.model.MultiPartitionKeyMappingProvider;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RealizationPruner {

    private static final String DATE = "date";
    private static final String TIMESTAMP = "timestamp";
    private static final String VARCHAR = "varchar";
    private static final String STRING = "string";
    private static final String INTEGER = "integer";
    private static final String BIGINT = "bigint";
    private static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");
    private static final Set<SqlKind> COMPARISON_OP_KIND_SET = ImmutableSet.of(SqlKind.GREATER_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL, //
            SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, //
            SqlKind.IN, SqlKind.NOT_IN, //
            SqlKind.EQUALS, SqlKind.NOT_EQUALS);

    private RealizationPruner() {
    }

    public static List<NDataSegment> pruneSegments(NDataflow dataflow, OLAPContext olapContext) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val projectName = dataflow.getProject();

        val projectInstance = NProjectManager.getInstance(kylinConfig).getProject(projectName);
        val allReadySegments = dataflow.getQueryableSegments();
        if (!projectInstance.getConfig().isHeterogeneousSegmentEnabled()) {
            return allReadySegments;
        }
        val isStreamingFactTable = olapContext.firstTableScan.getOlapTable().getSourceTable()
                .getSourceType() == ISourceAware.ID_STREAMING;
        val isBatchFusionModel = isStreamingFactTable && dataflow.getModel().isFusionModel() && !dataflow.isStreaming();
        val partitionDesc = isBatchFusionModel
                ? getStreamingPartitionDesc(dataflow.getModel(), kylinConfig, projectName)
                : dataflow.getModel().getPartitionDesc();
        // no partition column
        if (PartitionDesc.isEmptyPartitionDesc(partitionDesc)) {
            log.info("No partition column");
            return allReadySegments;
        }

        val partitionColumn = partitionDesc.getPartitionDateColumnRef();
        val dateFormat = partitionDesc.getPartitionDateFormat();
        val filterColumns = olapContext.filterColumns;
        // sql filter columns do not include partition column
        if (!filterColumns.contains(partitionColumn)) {
            log.info("Filter columns do not contain partition column");
            return allReadySegments;
        }

        val selectedSegments = Lists.<NDataSegment> newArrayList();
        var filterConditions = olapContext.getExpandedFilterConditions();
        val relOptCluster = olapContext.firstTableScan.getCluster();
        val rexBuilder = relOptCluster.getRexBuilder();
        val rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(), RelOptPredicateList.EMPTY, true,
                relOptCluster.getPlanner().getExecutor());

        val partitionColInputRef = transformColumn2RexInputRef(partitionColumn, olapContext.allTableScans);
        if (allReadySegments.size() > 0 && dateFormat != null) {
            val firstSegmentRanges = transformSegment2RexCall(allReadySegments.get(0), dateFormat, rexBuilder,
                    partitionColInputRef, partitionColumn.getType(), dataflow.isStreaming());
            RelDataTypeFamily segmentLiteralTypeFamily = getSegmentLiteralTypeFamily(firstSegmentRanges.getFirst());
            filterConditions = filterConditions.stream().map(filterCondition -> rewriteRexCall(filterCondition,
                    rexBuilder, segmentLiteralTypeFamily, partitionColInputRef, dateFormat))
                    .collect(Collectors.toList());
        }
        var simplifiedSqlFilter = rexSimplify.simplifyAnds(filterConditions);

        // sql filter condition is always false
        if (simplifiedSqlFilter.isAlwaysFalse()) {
            log.info("SQL filter condition is always false, pruning all ready segments");
            olapContext.storageContext.setFilterCondAlwaysFalse(true);
            return selectedSegments;
        }
        // sql filter condition is always true
        if (simplifiedSqlFilter.isAlwaysTrue()) {
            log.info("SQL filter condition is always true, pruning no segment");
            return allReadySegments;
        }

        if (dateFormat == null) {
            return allReadySegments;
        }

        for (NDataSegment dataSegment : allReadySegments) {
            try {
                val segmentRanges = transformSegment2RexCall(dataSegment, dateFormat, rexBuilder, partitionColInputRef,
                        partitionColumn.getType(), dataflow.isStreaming());
                // compare with segment start
                val segmentStartPredicate = RelOptPredicateList.of(rexBuilder,
                        Lists.newArrayList(segmentRanges.getFirst()));
                var simplifiedWithPredicate = rexSimplify.withPredicates(segmentStartPredicate)
                        .simplify(simplifiedSqlFilter);
                if (simplifiedWithPredicate.isAlwaysFalse()) {
                    continue;
                }
                // compare with segment end
                val segmentEndPredicate = RelOptPredicateList.of(rexBuilder,
                        Lists.newArrayList(segmentRanges.getSecond()));
                simplifiedWithPredicate = rexSimplify.withPredicates(segmentEndPredicate)
                        .simplify(simplifiedWithPredicate);
                if (!simplifiedWithPredicate.isAlwaysFalse()) {
                    selectedSegments.add(dataSegment);
                }
            } catch (Exception ex) {
                log.warn("Segment pruning error: ", ex);
                selectedSegments.add(dataSegment);
            }
        }
        log.info("Scan segment.size: {} after segment pruning", selectedSegments.size());
        return selectedSegments;
    }

    public static RexNode rewriteRexCall(RexNode rexNode, RexBuilder rexBuilder, RelDataTypeFamily relDataTypeFamily,
            RexInputRef partitionColInputRef, String dateFormat) {
        if (!(rexNode instanceof RexCall)) {
            return rexNode;
        }

        RexCall rewriteRexCall = (RexCall) rexNode;

        if (COMPARISON_OP_KIND_SET.contains(rewriteRexCall.getOperator().kind)) {
            return needRewrite(partitionColInputRef, rewriteRexCall)
                    ? rewriteRexNodeLiteral(rexNode, rexBuilder, relDataTypeFamily, dateFormat)
                    : rexNode;
        } else {
            List<RexNode> opList = rewriteRexCall.getOperands().stream()
                    .map(rex -> rewriteRexCall(rex, rexBuilder, relDataTypeFamily, partitionColInputRef, dateFormat))
                    .collect(Collectors.toList());
            return rexBuilder.makeCall(rewriteRexCall.getOperator(), opList);
        }
    }

    private static boolean needRewrite(RexInputRef partitionColInputRef, RexCall rewriteRexCall) {
        boolean isContainsPartitionColumn = false;
        boolean isContainsLiteral = false;
        for (RexNode sonRexNode : rewriteRexCall.getOperands()) {
            if (sonRexNode instanceof RexInputRef) {
                RexInputRef rexInputRef = (RexInputRef) sonRexNode;
                String columnName = rexInputRef.getName();
                if (partitionColInputRef.getName().contains(columnName)) {
                    isContainsPartitionColumn = true;
                }
            } else if (sonRexNode instanceof RexLiteral) {
                isContainsLiteral = true;
            }
        }
        return isContainsPartitionColumn && isContainsLiteral;
    }

    public static RexNode rewriteRexNodeLiteral(RexNode rexNodeLiteral, RexBuilder rexBuilder,
            RelDataTypeFamily relDataTypeFamily, String dateFormat) {
        if (rexNodeLiteral instanceof RexCall) {
            try {
                RexCall rexCall = (RexCall) rexNodeLiteral;
                List<RexNode> oldRexNodes = rexCall.getOperands();
                List<RexNode> newRexNodes = new ArrayList<>();
                for (RexNode rexNode : oldRexNodes) {
                    newRexNodes.add(transform(rexNode, rexBuilder, relDataTypeFamily, dateFormat));
                }
                rexNodeLiteral = rexBuilder.makeCall(rexCall.getOperator(), newRexNodes);
            } catch (Exception e) {
                log.warn("RewriteRexNodeLiteral failed rexNodeLiteral:{} relDataTypeFamily:{} dateFormat:{}",
                        rexNodeLiteral, relDataTypeFamily.toString(), dateFormat, e);
            }
        }
        return rexNodeLiteral;
    }

    private static RexNode transform(RexNode rexNode, RexBuilder rexBuilder, RelDataTypeFamily relDataTypeFamily,
            String dateFormat) {
        if (!(rexNode instanceof RexLiteral)) {
            return rexNode;
        }

        RexLiteral rexLiteral = (RexLiteral) rexNode;
        RexNode newLiteral;
        if (SqlTypeFamily.DATE == relDataTypeFamily) {
            String dateStr = normalization(dateFormat, rexLiteral);
            newLiteral = rexBuilder.makeLiteral(new DateString(dateStr),
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DATE), true);
        } else if (SqlTypeFamily.CHARACTER == relDataTypeFamily) {
            String dateStr = normalization(dateFormat, rexLiteral);
            newLiteral = rexBuilder.makeLiteral(new NlsString(dateStr, "UTF-16LE", SqlCollation.IMPLICIT),
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), true);
        } else {
            newLiteral = rexLiteral;
        }
        return newLiteral;
    }

    private static String normalization(String dateFormat, RexLiteral rexLiteral) {
        RelDataTypeFamily typeFamily = rexLiteral.getType().getFamily();
        if (SqlTypeFamily.DATE == typeFamily) {
            // Calendar uses UTC timezone, just to keep RexLiteral's value(an instanceof DateString)
            long timeInMillis = ((Calendar) rexLiteral.getValue()).getTimeInMillis();
            String dateStr = DateFormat.formatToDateStr(timeInMillis, dateFormat, RealizationPruner.UTC_ZONE);
            if (!rexLiteral.toString().equals(dateStr)) {
                log.warn("Normalize RexLiteral({}) to {}", rexLiteral, dateStr);
            }
            return dateStr;
        }
        return rexLiteral.getValue2().toString();
    }

    public static RelDataTypeFamily getSegmentLiteralTypeFamily(RexNode rangeRexNode) {
        if (rangeRexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rangeRexNode;
            List<RexNode> oldRexNodes = rexCall.getOperands();
            for (RexNode rexNode : oldRexNodes) {
                if (rexNode instanceof RexLiteral) {
                    return rexNode.getType().getFamily();
                }
            }
        }
        return null;
    }

    private static PartitionDesc getStreamingPartitionDesc(NDataModel model, KylinConfig kylinConfig, String project) {
        NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
        NDataModel streamingModel = modelManager.getDataModelDesc(model.getFusionId());
        return streamingModel.getPartitionDesc();
    }

    private static Pair<RexNode, RexNode> transformSegment2RexCall(NDataSegment dataSegment, String dateFormat,
            RexBuilder rexBuilder, RexInputRef partitionColInputRef, DataType partitionColType, boolean isStreaming) {
        String start;
        String end;
        if (dataSegment.isOffsetCube()) {
            start = DateFormat.formatToDateStr(dataSegment.getKSRange().getStart(), dateFormat);
            end = DateFormat.formatToDateStr(dataSegment.getKSRange().getEnd(), dateFormat);
        } else {
            start = DateFormat.formatToDateStr(dataSegment.getTSRange().getStart(), dateFormat);
            end = DateFormat.formatToDateStr(dataSegment.getTSRange().getEnd(), dateFormat);
        }

        val startRexLiteral = transformValue2RexLiteral(rexBuilder, start, partitionColType);
        val endRexLiteral = transformValue2RexLiteral(rexBuilder, end, partitionColType);
        val greaterThanOrEqualCall = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                Lists.newArrayList(partitionColInputRef, startRexLiteral));

        // the right side of streaming segments is closed, like [start, end], while batch segment is [start, end)
        val sqlOperator = isStreaming ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN;
        val lessCall = rexBuilder.makeCall(sqlOperator, Lists.newArrayList(partitionColInputRef, endRexLiteral));
        return Pair.newPair(greaterThanOrEqualCall, lessCall);
    }

    private static RexNode transformValue2RexLiteral(RexBuilder rexBuilder, String value, DataType colType) {
        switch (colType.getName()) {
        case DATE:
            return rexBuilder.makeDateLiteral(new DateString(value));
        case TIMESTAMP:
            var relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
            return rexBuilder.makeTimestampLiteral(new TimestampString(value), relDataType.getPrecision());
        case VARCHAR:
        case STRING:
            return rexBuilder.makeLiteral(value);
        case INTEGER:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
            return rexBuilder.makeLiteral(Integer.parseInt(value), relDataType, false);
        case BIGINT:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
            return rexBuilder.makeLiteral(Long.parseLong(value), relDataType, false);
        default:
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s data type is not supported for partition column", colType));
        }
    }

    private static RexInputRef transformColumn2RexInputRef(TblColRef partitionCol, Set<OLAPTableScan> tableScans) {
        for (OLAPTableScan tableScan : tableScans) {
            val tableIdentity = tableScan.getTableName();
            if (tableIdentity.equals(partitionCol.getTable())) {
                val index = tableScan.getColumnRowType().getAllColumns().indexOf(partitionCol);
                if (index >= 0) {
                    return OLAPContext.createUniqueInputRefAmongTables(tableScan, index, tableScans);
                }
                throw new IllegalStateException(String.format(Locale.ROOT, "Cannot find column %s in all tableScans",
                        partitionCol.getIdentity()));
            }
        }

        throw new IllegalStateException(
                String.format(Locale.ROOT, "Cannot find column %s in all tableScans", partitionCol.getIdentity()));
    }

    public static Map<String, List<Long>> matchPartitions(List<NDataSegment> dataSegments, NDataModel model,
            OLAPContext olapContext) {
        val multiPartitionDesc = model.getMultiPartitionDesc();
        val filterConditions = olapContext.getExpandedFilterConditions();
        val filterCols = olapContext.filterColumns;

        val partitionColRefs = multiPartitionDesc.getColumnRefs();
        val mapping = getMapping(model.getProject(), model.getId());
        Map<String, List<Long>> segPartitionMap = dataSegments.stream()
                .collect(Collectors.toMap(NDataSegment::getId, NDataSegment::getMultiPartitionIds));
        if (!filterCols.containsAll(partitionColRefs) && !containsMappingColumns(mapping, filterCols)) {
            return segPartitionMap;
        }

        val relOptCluster = olapContext.firstTableScan.getCluster();
        val rexBuilder = relOptCluster.getRexBuilder();
        val rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(), RelOptPredicateList.EMPTY, true,
                relOptCluster.getPlanner().getExecutor());
        val simplifiedSqlFilter = rexSimplify.simplifyAnds(filterConditions);
        // sql filter condition is always false
        if (simplifiedSqlFilter.isAlwaysFalse()) {
            log.info("SQL filter condition is always false, pruning all partitions");
            return Maps.newHashMap();
        }
        // sql filter condition is always true
        if (simplifiedSqlFilter.isAlwaysTrue()) {
            log.info("SQL filter condition is always true, pruning no partition");
            return segPartitionMap;
        }

        for (MultiPartitionDesc.PartitionInfo partition : multiPartitionDesc.getPartitions()) {
            try {
                val partitionCall = transformPartition2RexCall(partitionColRefs, partition.getValues(), rexBuilder,
                        olapContext.allTableScans);
                RexNode mappingColumnRexCall = rexBuilder.makeLiteral(true);
                if (mapping != null && CollectionUtils.isNotEmpty(mapping.getMultiPartitionCols())) {
                    mappingColumnRexCall = transformPartitionMapping2RexCall(partition.getValues(), mapping, rexBuilder,
                            olapContext.allTableScans);
                }

                if (isAlwaysFalse(simplifiedSqlFilter, partitionCall, mappingColumnRexCall, rexSimplify, rexBuilder)) {
                    // prune this partition
                    segPartitionMap.forEach((dataSegment, partitionIds) -> partitionIds.remove(partition.getId()));
                    continue;
                }
            } catch (Exception ex) {
                log.warn("Multi-partition pruning error: ", ex);
            }

            // if any segment does not contain this selected model-defined partition, push down
            for (Map.Entry<String, List<Long>> entry : segPartitionMap.entrySet()) {
                val partitionIds = entry.getValue();
                if (!partitionIds.contains(partition.getId())) {
                    val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                            .getDataflow(model.getId());
                    val segment = dataflow.getSegment(entry.getKey());
                    log.info("segment {} does not have partition {}", segment.displayIdName(), partition.getId());
                    return null;
                }
            }
        }

        return segPartitionMap;
    }

    private static boolean isAlwaysFalse(RexNode simplifiedSqlFilter, RexNode partitionNode, RexNode mappingNode,
            RexSimplify rexSimplify, RexBuilder rexBuilder) {
        // simplifyAnds method can handle NOT_EQUAL operation
        val simplifyAnds = rexSimplify
                .simplifyAnds(Lists.newArrayList(simplifiedSqlFilter, partitionNode, mappingNode));
        val predicate = RelOptPredicateList.of(rexBuilder, Lists.newArrayList(partitionNode, mappingNode));
        // simplifyWithPredicates can handle OR operation
        var simplifiedWithPredicate = rexSimplify.withPredicates(predicate).simplify(simplifiedSqlFilter);

        return simplifyAnds.isAlwaysFalse() || simplifiedWithPredicate.isAlwaysFalse();
    }

    private static boolean containsMappingColumns(MultiPartitionKeyMapping mapping, Set<TblColRef> filterColumnRefs) {
        if (mapping == null || CollectionUtils.isEmpty(mapping.getMultiPartitionCols())) {
            return false;
        }
        val filterColumnIdentities = filterColumnRefs.stream().map(TblColRef::getCanonicalName)
                .collect(Collectors.toSet());
        val aliasColumnIdentities = mapping.getAliasColumns().stream().map(TblColRef::getCanonicalName)
                .collect(Collectors.toSet());
        return filterColumnIdentities.containsAll(aliasColumnIdentities);
    }

    private static MultiPartitionKeyMapping getMapping(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        try {
            MultiPartitionKeyMappingProvider provider = (MultiPartitionKeyMappingProvider) ClassUtil
                    .newInstance(kylinConfig.getMultiPartitionKeyMappingProvider());
            return provider.getMapping(project, model);
        } catch (Exception | NoClassDefFoundError e) {
            log.error("Failed to create multi-partition key mapping provider", e);
        }

        return null;
    }

    private static RexNode transformPartition2RexCall(List<TblColRef> partitionCols, String[] partitionValues,
            RexBuilder rexBuilder, Set<OLAPTableScan> tableScans) {
        return transformColumns2RexCall(partitionCols, Collections.singletonList(Lists.newArrayList(partitionValues)),
                rexBuilder, tableScans);
    }

    private static RexNode transformPartitionMapping2RexCall(String[] partitionValues, MultiPartitionKeyMapping mapping,
            RexBuilder rexBuilder, Set<OLAPTableScan> tableScans) {
        val mappedColumns = mapping.getAliasColumns();
        val mappedValues = mapping.getAliasValue(Lists.newArrayList(partitionValues));
        if (CollectionUtils.isEmpty(mappedColumns) || CollectionUtils.isEmpty(mappedValues)) {
            return rexBuilder.makeLiteral(true);
        }
        return transformColumns2RexCall(mappedColumns, mappedValues, rexBuilder, tableScans);
    }

    private static RexNode transformColumns2RexCall(List<TblColRef> columns, Collection<List<String>> values,
            RexBuilder rexBuilder, Set<OLAPTableScan> tableScans) {
        val orRexCalls = Lists.<RexNode> newArrayList();
        for (List<String> columnValue : values) {
            int size = columns.size();
            val equalRexCalls = Lists.<RexNode> newArrayList();
            for (int i = 0; i < size; i++) {
                val value = columnValue.get(i);
                val columnRef = columns.get(i);
                val columnRexInputRef = transformColumn2RexInputRef(columnRef, tableScans);
                val valueLiteral = transformValue2RexLiteral(rexBuilder, value, columnRef.getType());
                val equalRexCall = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        Lists.newArrayList(columnRexInputRef, valueLiteral));
                equalRexCalls.add(equalRexCall);
            }

            val andRexCall = equalRexCalls.size() == 1 ? equalRexCalls.get(0)
                    : rexBuilder.makeCall(SqlStdOperatorTable.AND, equalRexCalls);
            orRexCalls.add(andRexCall);
        }
        return orRexCalls.size() == 1 ? orRexCalls.get(0) : rexBuilder.makeCall(SqlStdOperatorTable.OR, orRexCalls);
    }
}
