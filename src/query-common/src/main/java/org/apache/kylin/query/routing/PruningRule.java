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

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.RexUtils;

/**
 * The pruning rule is responsible for matching the indexes that can be used for querying. 
 * Classes extend this class do not have any attributes and only needs to implement 
 * the {@link PruningRule#apply(Candidate)} method.
 */
public abstract class PruningRule {

    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern TIMESTAMP_PATTERN = Pattern
            .compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d*[1-9])?");

    @Override
    public String toString() {
        return this.getClass().getName();
    }

    public abstract void apply(Candidate candidate);

    public abstract boolean isStorageMatch(Candidate candidate);

    PartitionDesc getPartitionDesc(NDataflow dataflow, OlapContext olapContext) {
        NDataModel model = dataflow.getModel();
        boolean isStreamingFactTable = olapContext.getFirstTableScan().getOlapTable().getSourceTable()
                .getSourceType() == ISourceAware.ID_STREAMING;
        boolean isBatchFusionModel = isStreamingFactTable && dataflow.getModel().isFusionModel()
                && !dataflow.isStreaming();
        if (!isBatchFusionModel) {
            return model.getPartitionDesc();
        }
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), dataflow.getProject())
                .getDataModelDesc(model.getFusionId()).getPartitionDesc();
    }

    boolean isFullBuildModel(PartitionDesc partitionCol) {
        return PartitionDesc.isEmptyPartitionDesc(partitionCol) || partitionCol.getPartitionDateFormat() == null;
    }

    List<RexNode> transformValue2RexCall(RexBuilder rexBuilder, RexInputRef colInputRef, DataType colType, String left,
            String right, boolean closedRight) {
        RexNode startRexLiteral = RexUtils.transformValue2RexLiteral(rexBuilder, left, colType);
        RexNode endRexLiteral = RexUtils.transformValue2RexLiteral(rexBuilder, right, colType);
        RexNode greaterThanOrEqualCall = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                Lists.newArrayList(colInputRef, startRexLiteral));
        SqlBinaryOperator sqlOperator = closedRight ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL
                : SqlStdOperatorTable.LESS_THAN;
        RexNode lessCall = rexBuilder.makeCall(sqlOperator, Lists.newArrayList(colInputRef, endRexLiteral));
        return Lists.newArrayList(greaterThanOrEqualCall, lessCall);
    }

    static String checkAndReformatDateType(String formattedValue, long segmentTs, DataType colType) {
        switch (colType.getName()) {
        case DataType.DATE:
            if (DATE_PATTERN.matcher(formattedValue).matches()) {
                return formattedValue;
            }
            return DateFormat.formatToDateStr(segmentTs, DateFormat.DEFAULT_DATE_PATTERN);
        case DataType.TIMESTAMP:
            if (TIMESTAMP_PATTERN.matcher(formattedValue).matches()) {
                return formattedValue;
            }
            return DateFormat.formatToDateStr(segmentTs, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        case DataType.VARCHAR:
        case DataType.STRING:
        case DataType.INTEGER:
        case DataType.BIGINT:
            return formattedValue;
        default:
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s data type is not supported for partition column", colType));
        }
    }
}
