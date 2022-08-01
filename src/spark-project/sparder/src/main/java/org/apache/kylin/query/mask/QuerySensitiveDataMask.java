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

package org.apache.kylin.query.mask;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.acl.SensitiveDataMask;
import org.apache.kylin.metadata.acl.SensitiveDataMaskInfo;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.KapTableScan;
import org.apache.kylin.query.relnode.KapWindowRel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import scala.Option;

public class QuerySensitiveDataMask implements QueryResultMask {

    private RelNode rootRelNode;

    private String defaultDatabase;

    private SensitiveDataMaskInfo maskInfo;

    private List<SensitiveDataMask.MaskType> resultMasks;

    public QuerySensitiveDataMask(String project, KylinConfig kylinConfig) {
        defaultDatabase = NProjectManager.getInstance(kylinConfig).getProject(project).getDefaultDatabase();
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        if (aclInfo != null) {
            maskInfo = AclTCRManager.getInstance(kylinConfig, project).getSensitiveDataMaskInfo(aclInfo.getUsername(),
                    aclInfo.getGroups());
        }
    }

    // for testing
    public QuerySensitiveDataMask(String defaultDatabase, SensitiveDataMaskInfo maskInfo) {
        this.defaultDatabase = defaultDatabase;
        this.maskInfo = maskInfo;
    }

    public void doSetRootRelNode(RelNode relNode) {
        this.rootRelNode = relNode;
    }

    public void init() {
        assert rootRelNode != null;
        resultMasks = getSensitiveCols(rootRelNode);
    }

    public Dataset<Row> doMaskResult(Dataset<Row> df) {
        if (maskInfo == null || rootRelNode == null || !maskInfo.hasMask()) {
            return df;
        }
        if (resultMasks == null) {
            init();
        }

        Column[] columns = new Column[df.columns().length];
        boolean masked = false;
        Dataset<Row> dfWithIndexedCol = MaskUtil.dFToDFWithIndexedColumns(df);
        for (int i = 0; i < dfWithIndexedCol.columns().length; i++) {
            if (resultMasks.get(i) == null
                    || !SensitiveDataMask.isValidDataType(getResultColumnDataType(i).getSqlTypeName().getName())) {
                columns[i] = dfWithIndexedCol.col(dfWithIndexedCol.columns()[i]);
                continue;
            }

            switch (resultMasks.get(i)) {
            case DEFAULT:
                columns[i] = new Column(
                        new Cast(new Literal(UTF8String.fromString(defaultMaskResultToString(i)), DataTypes.StringType),
                                dfWithIndexedCol.schema().fields()[i].dataType(),
                                Option.apply(TimeZone.getDefault().toZoneId().getId())))
                                        .as(dfWithIndexedCol.columns()[i]);
                masked = true;
                break;
            case AS_NULL:
                columns[i] = new Column(new Literal(null, dfWithIndexedCol.schema().fields()[i].dataType()))
                        .as(dfWithIndexedCol.columns()[i]);
                masked = true;
                break;
            default:
                columns[i] = dfWithIndexedCol.col(dfWithIndexedCol.columns()[i]);
                break;
            }
        }
        return masked ? dfWithIndexedCol.select(columns).toDF(df.columns()) : df;
    }

    private RelDataType getResultColumnDataType(int columnIdx) {
        return rootRelNode.getRowType().getFieldList().get(columnIdx).getType();
    }

    private String defaultMaskResultToString(int columnIdx) {
        return defaultMaskResultToString(getResultColumnDataType(columnIdx));
    }

    // for testing
    public String defaultMaskResultToString(RelDataType type) {
        switch (type.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
            return (type.getPrecision() > 0 && type.getPrecision() < 4) ? Strings.repeat("*", type.getPrecision())
                    : "****";
        case INTEGER:
        case BIGINT:
        case TINYINT:
        case SMALLINT:
            return "0";
        case DOUBLE:
        case FLOAT:
        case DECIMAL:
        case REAL:
            return "0.0";
        case DATE:
            return "1970-01-01";
        case TIMESTAMP:
            return "1970-01-01 00:00:00";
        default:
            return null;
        }
    }

    /**
     * Search relNodes from bottom-up, and collect masks of the result columns
     * The mask of a column in the top relNode will be the merged masks of all the columns it references
     *
     * @param relNode
     * @return
     */
    private List<SensitiveDataMask.MaskType> getSensitiveCols(RelNode relNode) {
        if (relNode instanceof TableScan) {
            return getTableSensitiveCols((TableScan) relNode);
        } else if (relNode instanceof Values) {
            return Lists.newArrayList(new SensitiveDataMask.MaskType[relNode.getRowType().getFieldList().size()]);
        } else if (relNode instanceof Aggregate) {
            return getAggregateSensitiveCols((Aggregate) relNode);
        } else if (relNode instanceof Project) {
            return getProjectSensitiveCols((Project) relNode);
        } else if (relNode instanceof SetOp) {
            return getUnionSensitiveCols((SetOp) relNode);
        } else if (relNode instanceof KapWindowRel) {
            return getWindowSensitiveCols((Window) relNode);
        } else {
            List<SensitiveDataMask.MaskType> masks = new ArrayList<>();
            for (RelNode input : relNode.getInputs()) {
                masks.addAll(getSensitiveCols(input));
            }
            return masks;
        }
    }

    private List<SensitiveDataMask.MaskType> getWindowSensitiveCols(Window window) {
        List<SensitiveDataMask.MaskType> inputMasks = getSensitiveCols(window.getInput(0));
        SensitiveDataMask.MaskType[] masks = new SensitiveDataMask.MaskType[window.getRowType().getFieldList().size()];
        int i = 0;
        for (; i < inputMasks.size(); i++) {
            masks[i] = inputMasks.get(i);
        }
        List<RexNode> aggCalls = window.groups.stream().flatMap(group -> group.aggCalls.stream())
                .collect(Collectors.toList());
        for (RexNode aggCall : aggCalls) {
            SensitiveDataMask.MaskType mask = null;
            for (Integer bit : RelOptUtil.InputFinder.bits(aggCall)) {
                if (bit < inputMasks.size() && inputMasks.get(bit) != null) { // skip constants
                    mask = mask == null ? inputMasks.get(bit) : inputMasks.get(bit).merge(mask);
                }
            }
            masks[i++] = mask;
        }
        return Lists.newArrayList(masks);
    }

    private List<SensitiveDataMask.MaskType> getUnionSensitiveCols(SetOp setOp) {
        SensitiveDataMask.MaskType[] masks = new SensitiveDataMask.MaskType[setOp.getRowType().getFieldList().size()];
        for (RelNode input : setOp.getInputs()) {
            List<SensitiveDataMask.MaskType> inputMasks = getSensitiveCols(input);
            for (int i = 0; i < masks.length; i++) {
                if (inputMasks.get(i) != null) {
                    masks[i] = inputMasks.get(i).merge(masks[i]);
                }
            }
        }
        return Lists.newArrayList(masks);
    }

    private List<SensitiveDataMask.MaskType> getProjectSensitiveCols(Project project) {
        List<SensitiveDataMask.MaskType> inputMasks = getSensitiveCols(project.getInput(0));
        SensitiveDataMask.MaskType[] masks = new SensitiveDataMask.MaskType[project.getChildExps().size()];
        for (int i = 0; i < project.getChildExps().size(); i++) {
            RexNode expr = project.getChildExps().get(i);
            for (Integer input : RelOptUtil.InputFinder.bits(expr)) {
                if (inputMasks.get(input) != null) {
                    masks[i] = inputMasks.get(input).merge(masks[i]);
                }
            }
        }
        return Lists.newArrayList(masks);
    }

    private List<SensitiveDataMask.MaskType> getAggregateSensitiveCols(Aggregate aggregate) {
        List<SensitiveDataMask.MaskType> inputMasks = getSensitiveCols(aggregate.getInput(0));
        SensitiveDataMask.MaskType[] masks = new SensitiveDataMask.MaskType[aggregate.getRowType().getFieldList()
                .size()];
        int idx = 0;
        for (Integer groupInputIdx : aggregate.getGroupSet()) {
            masks[idx++] = inputMasks.get(groupInputIdx);
        }
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            for (Integer argInputIdx : aggregateCall.getArgList()) {
                if (inputMasks.get(argInputIdx) != null) {
                    masks[idx] = inputMasks.get(argInputIdx).merge(masks[idx]);
                }
            }
            idx++;
        }
        return Lists.newArrayList(masks);
    }

    /**
     * get masks of all columns on table, including computed columns
     *
     * @param tableScan
     * @return
     */
    private List<SensitiveDataMask.MaskType> getTableSensitiveCols(TableScan tableScan) {
        assert tableScan.getTable().getQualifiedName().size() == 2;
        String dbName = tableScan.getTable().getQualifiedName().get(0);
        String tableName = tableScan.getTable().getQualifiedName().get(1);
        List<SensitiveDataMask.MaskType> masks = new ArrayList<>();
        for (RelDataTypeField field : tableScan.getRowType().getFieldList()) {
            ColumnDesc columnDesc = ((KapTableScan) tableScan).getOlapTable().getSourceColumns().get(field.getIndex());
            if (columnDesc.isComputedColumn()) {
                masks.add(getCCMask(columnDesc.getComputedColumnExpr()));
            } else {
                SensitiveDataMask mask = maskInfo.getMask(dbName, tableName, field.getName());
                masks.add(mask == null ? null : mask.getType());
            }
        }
        return masks;
    }

    /**
     * parse cc expr, extract sql identifiers and search identifiers' mask in maskInfo
     *
     * @param ccExpr
     * @return
     */
    private SensitiveDataMask.MaskType getCCMask(String ccExpr) {
        List<SqlIdentifier> ids = MaskUtil.getCCCols(ccExpr);
        SensitiveDataMask.MaskType mask = null;
        for (SqlIdentifier id : ids) {
            SensitiveDataMask inputMask = null;
            if (id.names.size() == 2) {
                inputMask = maskInfo.getMask(defaultDatabase, id.names.get(0), id.names.get(1));
            } else if (id.names.size() == 3) {
                inputMask = maskInfo.getMask(id.names.get(0), id.names.get(1), id.names.get(2));
            }
            if (inputMask != null) {
                mask = inputMask.getType().merge(mask);
            }
        }
        return mask;
    }

    public List<SensitiveDataMask.MaskType> getResultMasks() {
        return resultMasks;
    }
}
