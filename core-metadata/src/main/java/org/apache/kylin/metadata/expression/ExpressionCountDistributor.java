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

package org.apache.kylin.metadata.expression;

import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.filter.TupleFilter;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ExpressionCountDistributor implements ExpressionVisitor {

    private final TupleExpression cntExpr;
    private boolean ifToCnt;
    private boolean ifCntSet;

    public ExpressionCountDistributor(TupleExpression cntExpr) {
        this.cntExpr = cntExpr;
        this.ifToCnt = true;
        this.ifCntSet = false;
    }

    @Override
    public TupleExpression visitConstant(ConstantTupleExpression constExpr) {
        TupleExpression copyExpr = new ConstantTupleExpression(constExpr.getDataType(), constExpr.getValue());
        if (copyExpr.getDataType().isNumberFamily() && ifToCnt) {
            copyExpr = new BinaryTupleExpression(TupleExpression.ExpressionOperatorEnum.MULTIPLE, cntExpr, copyExpr);
            ifCntSet = true;
        }
        return copyExpr;
    }

    @Override
    public TupleExpression visitColumn(ColumnTupleExpression colExpr) {
        return new ColumnTupleExpression(colExpr.getDataType(), colExpr.getColumn());
    }

    @Override
    public TupleExpression visitBinary(BinaryTupleExpression binaryExpr) {
        TupleExpression leftCopy;
        TupleExpression rightCopy;

        boolean ifToCntO = ifToCnt;
        switch (binaryExpr.getOperator()) {
        case PLUS:
        case MINUS:
            boolean ifCntSetO = ifCntSet;
            leftCopy = binaryExpr.getLeft().accept(this);
            ifToCnt = ifToCntO;
            ifCntSet = ifCntSetO;
            rightCopy = binaryExpr.getRight().accept(this);
            break;
        case MULTIPLE:
        case DIVIDE:
            if (ifToCntO) {
                ifToCnt = ExpressionColCollector.collectMeasureColumns(binaryExpr.getRight()).isEmpty();
            }
            leftCopy = binaryExpr.getLeft().accept(this);

            ifToCnt = ifToCntO && !ifCntSet;
            if (ifToCnt) {
                ifToCnt = ExpressionColCollector.collectMeasureColumns(binaryExpr.getLeft()).isEmpty();
            }
            ifCntSet = false;

            rightCopy = binaryExpr.getRight().accept(this);
            ifCntSet = ifToCntO && (ifCntSet || !ifToCnt);
            break;
        default:
            throw new IllegalArgumentException("Unsupported operator " + binaryExpr.getOperator());
        }
        return new BinaryTupleExpression(binaryExpr.getDataType(), binaryExpr.getOperator(),
                Lists.newArrayList(leftCopy, rightCopy));
    }

    @Override
    public TupleExpression visitCaseCall(CaseTupleExpression caseExpr) {
        List<Pair<TupleFilter, TupleExpression>> whenList = Lists
                .newArrayListWithExpectedSize(caseExpr.getWhenList().size());
        for (Pair<TupleFilter, TupleExpression> entry : caseExpr.getWhenList()) {
            TupleFilter filter = entry.getFirst();
            TupleExpression expression = visitIndependent(entry.getSecond());
            whenList.add(new Pair<>(filter, expression));
        }
        TupleExpression elseExpr = null;
        if (caseExpr.getElseExpr() != null) {
            elseExpr = visitIndependent(caseExpr.getElseExpr());
        }

        if (ifToCnt) {
            ifToCnt = ExpressionColCollector.collectMeasureColumns(caseExpr).isEmpty();
        }
        return new CaseTupleExpression(caseExpr.getDataType(), whenList, elseExpr);
    }

    @Override
    public TupleExpression visitRexCall(RexCallTupleExpression rexCallExpr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleExpression visitNone(NoneTupleExpression noneExpr) {
        return noneExpr;
    }

    private TupleExpression visitIndependent(TupleExpression expression) {
        boolean ifToCntO = ifToCnt;
        boolean ifCntSetO = ifCntSet;
        TupleExpression ret = expression.accept(this);
        ifToCnt = ifToCntO;
        ifCntSet = ifCntSetO;
        return ret;
    }

    public boolean ifCntSet() {
        return ifCntSet;
    }
}