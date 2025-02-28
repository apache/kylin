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
package org.apache.kylin.query.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.relnode.OlapProjectRel;
import org.apache.kylin.query.relnode.OlapTableScan;

import lombok.val;

public class RexUtils {

    private RexUtils() {
    }

    /**
     * check if there are more than two tables get involved in the join condition
     * @param join
     * @return
     */
    public static boolean joinMoreThanOneTable(Join join) {
        Set<Integer> left = new HashSet<>();
        Set<Integer> right = new HashSet<>();
        Set<Integer> indexes = getAllInputRefs(join.getCondition()).stream().map(RexSlot::getIndex)
                .collect(Collectors.toSet());
        splitJoinInputIndex(join, indexes, left, right);
        return !(colsComeFromSameSideOfJoin(join.getLeft(), left)
                && colsComeFromSameSideOfJoin(join.getRight(), right));
    }

    private static boolean colsComeFromSameSideOfJoin(RelNode rel, Set<Integer> indexes) {
        if (rel instanceof Join) {
            Join join = (Join) rel;
            Set<Integer> left = new HashSet<>();
            Set<Integer> right = new HashSet<>();
            splitJoinInputIndex(join, indexes, left, right);
            if (left.isEmpty()) {
                return colsComeFromSameSideOfJoin(join.getRight(), right);
            } else if (right.isEmpty()) {
                return colsComeFromSameSideOfJoin(join.getLeft(), left);
            } else {
                return false;
            }
        } else if (rel instanceof Project) {
            Set<Integer> inputIndexes = indexes.stream().map(idx -> ((Project) rel).getProjects().get(idx))
                    .flatMap(rex -> getAllInputRefs(rex).stream()).map(RexSlot::getIndex).collect(Collectors.toSet());
            return colsComeFromSameSideOfJoin(((Project) rel).getInput(), inputIndexes);
        } else if (rel instanceof TableScan || rel instanceof Values) {
            return true;
        } else {
            return colsComeFromSameSideOfJoin(rel.getInput(0), indexes);
        }
    }

    public static void splitJoinInputIndex(Join joinRel, Collection<Integer> indexes, Set<Integer> leftInputIndexes,
            Set<Integer> rightInputIndexes) {
        indexes.forEach(idx -> {
            if (idx < joinRel.getLeft().getRowType().getFieldCount()) {
                leftInputIndexes.add(idx);
            } else {
                rightInputIndexes.add(idx - joinRel.getLeft().getRowType().getFieldCount());
            }
        });
    }

    public static int countOperatorCall(RexNode condition, final Class<? extends SqlOperator> sqlOperator) {
        final AtomicInteger likeCount = new AtomicInteger(0);
        RexVisitor<Void> likeVisitor = new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCall(RexCall call) {

                if (call.getOperator().getClass().equals(sqlOperator)) {
                    likeCount.incrementAndGet();
                }
                return super.visitCall(call);
            }
        };
        condition.accept(likeVisitor);
        return likeCount.get();
    }

    public static Set<RexInputRef> getAllInputRefs(RexNode rexNode) {
        if (rexNode instanceof RexInputRef) {
            return Collections.singleton((RexInputRef) rexNode);
        } else if (rexNode instanceof RexCall) {
            return getAllInputRefsCall((RexCall) rexNode);
        } else {
            return Collections.emptySet();
        }
    }

    private static Set<RexInputRef> getAllInputRefsCall(RexCall rexCall) {
        return rexCall.getOperands().stream().flatMap(rexNode -> getAllInputRefs(rexNode).stream())
                .collect(Collectors.toSet());
    }

    /**
     * check if the columns on the given rel, are referencing the table column directly,
     * instead of referencing some rexCall
     * @param rel
     * @param columnIndexes
     * @return true if the columns on the given rel are directly referencing the underneath table columns
     * false if any of the columns points to a rexCall in the child rels
     */
    public static boolean isMerelyTableColumnReference(RelNode rel, Collection<Integer> columnIndexes) {
        // project and aggregations may change the columns
        if (rel instanceof OlapProjectRel) {
            return isProjectMerelyTableColumnReference((OlapProjectRel) rel, columnIndexes);
        } else if (rel instanceof OlapAggregateRel) {
            return isAggMerelyTableColumnReference((OlapAggregateRel) rel, columnIndexes);
        } else if (rel instanceof OlapJoinRel) { // test each sub queries of a join
            return isJoinMerelyTableColumnReference(rel, columnIndexes);
        } else { // other rel nodes won't changes the columns, just pass column idx down
            for (RelNode inputRel : rel.getInputs()) {
                if (!isMerelyTableColumnReference(inputRel, columnIndexes)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static boolean isJoinMerelyTableColumnReference(RelNode rel, Collection<Integer> columnIndexes) {
        int offset = 0;
        for (RelNode inputRel : rel.getInputs()) {
            Set<Integer> nextInputRefKeys = new HashSet<>();
            for (Integer columnIdx : columnIndexes) {
                if (columnIdx - offset >= 0 && columnIdx - offset < inputRel.getRowType().getFieldCount()) {
                    nextInputRefKeys.add(columnIdx - offset);
                }
            }
            if (!isMerelyTableColumnReference(inputRel, nextInputRefKeys)) {
                return false;
            }
            offset += inputRel.getRowType().getFieldCount();
        }
        return true;
    }

    private static boolean isAggMerelyTableColumnReference(OlapAggregateRel rel, Collection<Integer> columnIndexes) {
        Set<Integer> nextInputRefKeys = new HashSet<>();
        OlapAggregateRel agg = rel;
        for (Integer columnIdx : columnIndexes) {
            if (columnIdx >= agg.getRewriteGroupKeys().size()) { // pointing to agg calls
                return false;
            } else {
                nextInputRefKeys.add(agg.getRewriteGroupKeys().get(columnIdx));
            }
        }
        return isMerelyTableColumnReference(agg.getInput(), nextInputRefKeys);
    }

    private static boolean isProjectMerelyTableColumnReference(OlapProjectRel rel, Collection<Integer> columnIndexes) {
        Set<Integer> nextInputRefKeys = new HashSet<>();
        OlapProjectRel project = rel;
        for (Integer columnIdx : columnIndexes) {
            RexNode projExp = project.getProjects().get(columnIdx);
            if (projExp.getKind() == SqlKind.CAST) {
                projExp = ((RexCall) projExp).getOperands().get(0);
            }
            if (!(projExp instanceof RexInputRef)) {
                return false;
            }
            nextInputRefKeys.add(((RexInputRef) projExp).getIndex());
        }
        return isMerelyTableColumnReference(project.getInput(), nextInputRefKeys);
    }

    public static boolean isMerelyTableColumnReference(OlapJoinRel rel, RexNode condition) {
        // since join relNode's columns are just consist of the all the columns from all sub queries
        // we can simply use the input ref index extracted from the condition rex node as the column idx of the join rel
        return isMerelyTableColumnReference(rel,
                getAllInputRefs(condition).stream().map(RexSlot::getIndex).collect(Collectors.toSet()));
    }

    /**
     * remove cast clause in a column equal predicate
     * replace predicate of pattern cast(col1 as ...) = col2 with col1 = col2
     * @param predicateNode
     * @return
     */
    public static RexNode stripOffCastInColumnEqualPredicate(RexNode predicateNode) {
        if (!(predicateNode instanceof RexCall)) {
            return predicateNode;
        }
        RexCall predicate = (RexCall) predicateNode;
        // search and replace rex node with exact pattern of cast(col1 as ...) = col2
        if (predicate.getKind() == SqlKind.EQUALS) {
            boolean colEqualPredWithCast = false;
            List<RexNode> predicateOperands = Lists.newArrayList(predicate.getOperands());
            for (int predicateOpIdx = 0; predicateOpIdx < predicateOperands.size(); predicateOpIdx++) {
                RexNode predicateChild = predicateOperands.get(predicateOpIdx);

                // input ref
                if (predicateChild instanceof RexInputRef) {
                    continue;
                }
                // cast(col1 as ...)
                if (predicateChild instanceof RexCall && predicateChild.getKind() == SqlKind.CAST
                        && ((RexCall) predicateChild).getOperands().get(0) instanceof RexInputRef) {
                    predicateOperands.set(predicateOpIdx,
                            ((RexCall) predicateOperands.get(predicateOpIdx)).getOperands().get(0));
                    colEqualPredWithCast = true;
                }
            }

            if (colEqualPredWithCast) {
                return predicate.clone(predicate.getType(), predicateOperands);
            }
        }

        return predicate;
    }

    public static RexNode transformValue2RexLiteral(RexBuilder rexBuilder, String value, DataType colType) {
        RelDataType relDataType;
        String[] splits;
        Object parsedValue = colType.parseValue(value);
        switch (colType.getName()) {
        case DataType.DATE:
            // In order to support the column type is date, but the value is timestamp string.
            // for example: DEFAULT.TEST_KYLIN_FACT.CAL_DT with type date,
            // the filter condition is: cast("cal_dt" as timestamp) >= timestamp '2012-01-01 00:00:00',
            // the FilterConditionExpander will translate it to compare CAL_DT >= date '2012-01-01'
            // This seems like an unsafe operation.
            splits = StringUtils.split(value.trim(), " ");
            Preconditions.checkArgument(splits.length >= 1, "split %s with error", value);
            return rexBuilder.makeDateLiteral(new DateString(splits[0]));
        case DataType.DATETIME:
        case DataType.TIMESTAMP:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);
            // If the value with format yyyy-MM-dd, then pad with ` 00:00:00`,
            // if with format `yyyy-MM-dd HH:mm:ss`, use this value directly,
            // otherwise, wrong format, making literal will throw exception by Calcite.
            // Convert yyyy-MM-dd HH:mm:ss.0 to yyyy-MM-dd HH:mm:ss
            // because this format is not supported in calcite TimestampString
            int dotIndex = value.indexOf(".");
            if (dotIndex != -1 && Integer.parseInt(value.substring(dotIndex + 1)) == 0) {
                value = value.substring(0, dotIndex);
            }
            splits = StringUtils.split(value.trim(), " ");
            String ts = splits.length == 1 ? value + " 00:00:00" : value;
            return rexBuilder.makeTimestampLiteral(new TimestampString(ts), relDataType.getPrecision());
        case DataType.STRING:
        case DataType.VARCHAR:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, colType.getPrecision());
            return rexBuilder.makeLiteral(parsedValue, relDataType, false);
        case DataType.NUMERIC:
        case DataType.DECIMAL:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, colType.getPrecision());
            return rexBuilder.makeLiteral(parsedValue, relDataType, false);
        case DataType.BYTE:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TINYINT, colType.getPrecision());
            return rexBuilder.makeLiteral(parsedValue, relDataType, false);
        case DataType.INT:
        case DataType.INT4:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER, colType.getPrecision());
            return rexBuilder.makeLiteral(parsedValue, relDataType, false);
        case DataType.SHORT:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.SMALLINT, colType.getPrecision());
            return rexBuilder.makeLiteral(parsedValue, relDataType, false);
        case DataType.LONG:
        case DataType.LONG8:
            relDataType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT, colType.getPrecision());
            return rexBuilder.makeLiteral(parsedValue, relDataType, false);
        default:
            try {
                SqlTypeName sqlTypeName = SqlTypeName.get(colType.getName().toUpperCase(Locale.ROOT));
                int precision = colType.getPrecision();
                if (sqlTypeName == null) {
                    throw new IllegalArgumentException(colType + " data type is not supported for filter column");
                }
                relDataType = precision == -1 ? rexBuilder.getTypeFactory().createSqlType(sqlTypeName)
                        : rexBuilder.getTypeFactory().createSqlType(sqlTypeName, precision);
                return rexBuilder.makeLiteral(parsedValue, relDataType, false);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "%s data type is not supported for filter column", colType), e);
            }
        }
    }

    public static RexInputRef transformColumn2RexInputRef(TblColRef tblColRef, Set<OlapTableScan> tableScans) {
        for (OlapTableScan tableScan : tableScans) {
            val tableIdentity = tableScan.getTableName();
            if (tableIdentity.equals(tblColRef.getTable())) {
                val index = tableScan.getColumnRowType().getAllColumns().indexOf(tblColRef);
                if (index >= 0) {
                    return ContextUtil.createUniqueInputRefAmongTables(tableScan, index, tableScans);
                }
                throw new IllegalStateException(
                        String.format(Locale.ROOT, "Cannot find column %s in all tableScans", tblColRef.getIdentity()));
            }
        }

        throw new IllegalStateException(
                String.format(Locale.ROOT, "Cannot find column %s in all tableScans", tblColRef.getIdentity()));
    }

    // At present: `a * b * c` equals to `b * a * c`, but not equals to `a * c * b`.
    public static RexNode symmetricalExchange(RexBuilder rexBuilder, RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return rexNode;
        }
        RexCall call = (RexCall) rexNode;
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();
        final SqlKind kind = operator.getKind();
        final SqlKind reversedKind = kind.reverse();
        final int x = reversedKind.compareTo(kind);
        if (operands.size() == 2) {
            RexNode operand0 = operands.get(0);
            RexNode operand1 = operands.get(1);
            RexNode newOperand0 = symmetricalExchange(rexBuilder, operand0);
            RexNode newOperand1 = symmetricalExchange(rexBuilder, operand1);
            if (x < 0) {
                SqlOperator reverseOp = operator.reverse();
                if (reverseOp == null) {
                    return call.clone(call.getType(), Arrays.asList(newOperand0, newOperand1));
                }
                return rexBuilder.makeCall(call.getType(), reverseOp, Arrays.asList(newOperand1, newOperand0));
            } else if (rexNode.isA(SqlKind.SYMMETRICAL_SAME_ARG_TYPE)) {
                if (reorderOperands(operand0, operand1) < 0) {
                    return call.clone(call.getType(), Arrays.asList(newOperand1, newOperand0));
                }
            } else {
                return call.clone(call.getType(), Arrays.asList(newOperand0, newOperand1));
            }
        }
        List<RexNode> newOperands = operands.stream() //
                .map(rex -> symmetricalExchange(rexBuilder, rex)).collect(Collectors.toList());
        return call.clone(call.getType(), newOperands);
    }

    // copy from calcite
    private static int reorderOperands(RexNode operand0, RexNode operand1) {
        // Reorder the operands based on the SqlKind enumeration sequence,
        // smaller is in the behind, e.g. the literal is behind of input ref and AND, OR.
        int x = operand0.getKind().compareTo(operand1.getKind());
        // If the operands are same kind, use the hashcode to reorder.
        // Note: the RexInputRef's hash code is its index.
        return x != 0 ? x : operand1.hashCode() - operand0.hashCode();
    }
}
