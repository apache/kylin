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

import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ExpressionColCollector implements ExpressionVisitor {

    public static Set<TblColRef> collectColumns(TupleExpression tupleExpression) {
        Pair<Set<TblColRef>, Set<TblColRef>> pairRet = collectColumnsPair(tupleExpression);
        Set<TblColRef> ret = Sets.newHashSet();
        ret.addAll(pairRet.getFirst());
        ret.addAll(pairRet.getSecond());
        return ret;
    }

    public static Pair<Set<TblColRef>, Set<TblColRef>> collectColumnsPair(TupleExpression tupleExpression) {
        ExpressionColCollector collector = new ExpressionColCollector();
        tupleExpression.accept(collector);
        return new Pair<>(collector.filterColumns, collector.measureColumns);
    }

    public static Set<TblColRef> collectFilterColumns(TupleExpression tupleExpression) {
        ExpressionColCollector collector = new ExpressionColCollector();
        collector.ifMCols = false;
        tupleExpression.accept(collector);
        return collector.filterColumns;
    }

    public static Set<TblColRef> collectMeasureColumns(TupleExpression tupleExpression) {
        ExpressionColCollector collector = new ExpressionColCollector();
        collector.ifFCols = false;
        tupleExpression.accept(collector);
        return collector.measureColumns;
    }

    private final Set<TblColRef> filterColumns = Sets.newHashSet();
    private final Set<TblColRef> measureColumns = Sets.newHashSet();
    private boolean ifFCols = true;
    private boolean ifMCols = true;

    private ExpressionColCollector() {
    }

    @Override
    public TupleExpression visitNumber(NumberTupleExpression numExpr) {
        return numExpr;
    }

    @Override
    public TupleExpression visitString(StringTupleExpression strExpr) {
        return strExpr;
    }

    @Override
    public TupleExpression visitColumn(ColumnTupleExpression colExpr) {
        if (ifMCols) {
            measureColumns.add(colExpr.getColumn());
        }
        return colExpr;
    }

    @Override
    public TupleExpression visitBinary(BinaryTupleExpression binaryExpr) {
        binaryExpr.getLeft().accept(this);
        binaryExpr.getRight().accept(this);
        return binaryExpr;
    }

    @Override
    public TupleExpression visitCaseCall(CaseTupleExpression caseExpr) {
        for (Pair<TupleFilter, TupleExpression> entry : caseExpr.getWhenList()) {
            TupleFilter filter = entry.getFirst();
            if (ifFCols) {
                TupleFilter.collectColumns(filter, filterColumns);
            }

            entry.getSecond().accept(this);
        }
        if (caseExpr.getElseExpr() != null) {
            caseExpr.getElseExpr().accept(this);
        }
        return caseExpr;
    }

    @Override
    public TupleExpression visitRexCall(RexCallTupleExpression rexCallExpr) {
        for (TupleExpression child : rexCallExpr.getChildren()) {
            child.accept(this);
        }
        return rexCallExpr;
    }

    @Override
    public TupleExpression visitNone(NoneTupleExpression noneExpr) {
        return noneExpr;
    }
}
