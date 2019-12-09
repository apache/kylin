/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kylin.engine.spark.metadata.cube.model.tool;

import org.apache.calcite.sql.SqlKind;
import org.apache.kylin.engine.spark.metadata.cube.model.NonEquiJoinCondition;
import org.apache.kylin.engine.spark.metadata.cube.model.NonEquiJoinConditionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class NonEquiJoinConditionComparator {

    private static Logger logger = LoggerFactory.getLogger(NonEquiJoinConditionComparator.class);

    private NonEquiJoinConditionComparator() {
    }

    public static boolean equals(NonEquiJoinCondition cond1, NonEquiJoinCondition cond2) {
        try {
            return TruthTable.equals(createTruthTable(cond1), createTruthTable(cond2));
        } catch (Throwable e) {
            logger.error("Error on compareing cond1 {}, cond2 {}", cond1, cond2, e);
            return false;
        }
    }

    private static TruthTable createTruthTable(NonEquiJoinCondition nonEquiJoinCondition) {
        TruthTable.TruthTableBuilder<NonEquiJoinCondition> builder = new TruthTable.TruthTableBuilder<>(new NonEquiJoinConditionOperandComparator());
        buildExpr(nonEquiJoinCondition, builder);
        return builder.build();
    }

    private static Map<SqlKind, TruthTable.Operator> compositeOperatorMapping = new HashMap<>();
    static {
        compositeOperatorMapping.put(SqlKind.AND, TruthTable.Operator.AND);
        compositeOperatorMapping.put(SqlKind.OR, TruthTable.Operator.OR);
        compositeOperatorMapping.put(SqlKind.NOT, TruthTable.Operator.NOT);
    }
    private static void buildExpr(NonEquiJoinCondition nonEquiJoinCondition, TruthTable.TruthTableBuilder<NonEquiJoinCondition> builder) {
        switch (nonEquiJoinCondition.getOp()) {
            case AND:
            case NOT:
            case OR:
                builder.compositeStart(compositeOperatorMapping.get(nonEquiJoinCondition.getOp()));
                for (int i = 0; i < nonEquiJoinCondition.getOperands().length; i++) {
                    buildExpr(nonEquiJoinCondition.getOperands()[i], builder);
                }
                builder.compositeEnd();
                break;
            default:
                addOperand(nonEquiJoinCondition, builder);
                break;
        }
    }

    private static void addOperand(NonEquiJoinCondition nonEquiJoinCondition, TruthTable.TruthTableBuilder<NonEquiJoinCondition> builder) {
        NonEquiJoinCondition normalized = nonEquiJoinCondition.copy();
        if (inverseCondOperator(normalized)) {
            builder.compositeStart(TruthTable.Operator.NOT);
            normalizedCondOperandOrderings(normalized);
            builder.addOperand(normalized);
            builder.compositeEnd();
        } else {
            normalizedCondOperandOrderings(normalized);
            builder.addOperand(normalized);
        }
    }

    private static Map<SqlKind, SqlKind> operatorInverseMapping = new HashMap<>();
    static {
        // NOT_OP -> NOT(OP)
        operatorInverseMapping.put(SqlKind.NOT_EQUALS, SqlKind.EQUALS);
        operatorInverseMapping.put(SqlKind.NOT_IN, SqlKind.IN);
        operatorInverseMapping.put(SqlKind.IS_NOT_NULL, SqlKind.IS_NULL);
        operatorInverseMapping.put(SqlKind.IS_NOT_FALSE, SqlKind.IS_FALSE);
        operatorInverseMapping.put(SqlKind.IS_NOT_TRUE, SqlKind.IS_TRUE);
        operatorInverseMapping.put(SqlKind.IS_NOT_DISTINCT_FROM, SqlKind.IS_DISTINCT_FROM);
        // < -> NOT(>=), <= -> NOT(>)
        operatorInverseMapping.put(SqlKind.LESS_THAN, SqlKind.GREATER_THAN_OR_EQUAL);
        operatorInverseMapping.put(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN);
    }
    private static boolean inverseCondOperator(NonEquiJoinCondition cond) {
        if (operatorInverseMapping.containsKey(cond.getOp())) {
            cond.setOp(operatorInverseMapping.get(cond.getOp()));
            cond.setOpName(cond.getOp().sql);
            return true;
        }
        return false;
    }

    private static void normalizedCondOperandOrderings(NonEquiJoinCondition nonEquiJoinCondition) {
        // if operands ordering does not matter, sort by operands' digest
        // case =, <>
        if (nonEquiJoinCondition.getOp() == SqlKind.EQUALS) {
            Arrays.sort(nonEquiJoinCondition.getOperands(), Comparator.comparing(NonEquiJoinCondition::toString));
        }

        // sort IN args
        if (nonEquiJoinCondition.getOp() == SqlKind.IN) {
            Arrays.sort(nonEquiJoinCondition.getOperands(), 1, nonEquiJoinCondition.getOperands().length, Comparator.comparing(NonEquiJoinCondition::toString));
        }
    }

    public static class NonEquiJoinConditionOperandComparator implements Comparator<NonEquiJoinCondition> {

        @Override
        public int compare(NonEquiJoinCondition cond1, NonEquiJoinCondition cond2) {
            if(!(Objects.equals(cond1.getOp(), cond2.getOp()) &&
                    cond1.getOperands().length == cond2.getOperands().length &&
                    Objects.equals(cond1.getType(), cond2.getType()) &&
                    Objects.equals(cond1.getDataType(), cond2.getDataType()))) {
                return 1;
            }

            // compare opName on for SqlKind OTHER
            if (cond1.getOp() == SqlKind.OTHER || cond1.getOp() == SqlKind.OTHER_FUNCTION) {
                if (!Objects.equals(cond1.getOpName(), cond2.getOpName())) {
                    return 1;
                }
            }

            if (cond1.getType() == NonEquiJoinConditionType.LITERAL) {
                return Objects.equals(cond1.getValue(), cond2.getValue()) ? 0 : 1;
            } else if (cond1.getType() == NonEquiJoinConditionType.COLUMN) {
                return Objects.equals(cond1.getColRef().getColumnDesc(), cond2.getColRef().getColumnDesc()) ? 0 : 1;
            } else {
                for (int i = 0; i < cond1.getOperands().length; i++) {
                    if (compare(cond1.getOperands()[i], cond2.getOperands()[i]) != 0) {
                        return 1;
                    }
                }
                return 0;
            }
        }

    }

}
