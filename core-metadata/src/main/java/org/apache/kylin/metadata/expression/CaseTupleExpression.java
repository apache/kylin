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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class CaseTupleExpression extends TupleExpression {

    private List<Pair<TupleFilter, TupleExpression>> whenList;
    private TupleExpression elseExpr;

    public CaseTupleExpression(List<Pair<TupleFilter, TupleExpression>> whenList, TupleExpression elseExpr) {
        super(ExpressionOperatorEnum.CASE, Collections.<TupleExpression> emptyList());
        this.whenList = whenList;
        this.elseExpr = elseExpr;
    }

    @Override
    protected boolean ifAbleToPushDown() {
        if (ifAbleToPushDown == null) {
            for (Pair<TupleFilter, TupleExpression> whenEntry : whenList) {
                ifAbleToPushDown = TupleFilter.isEvaluableRecursively(whenEntry.getFirst())
                        && whenEntry.getSecond().ifAbleToPushDown();
                if (!ifAbleToPushDown) {
                    break;
                }
            }
            if (elseExpr != null && Boolean.TRUE.equals(ifAbleToPushDown)) {
                ifAbleToPushDown = elseExpr.ifAbleToPushDown();
            }
            if (ifAbleToPushDown == null) {
                ifAbleToPushDown = true;
            }
        }
        return ifAbleToPushDown;
    }

    @Override
    public boolean ifForDynamicColumn() {
        return ifAbleToPushDown();
    }

    //TODO
    @Override
    public void verify() {
    }

    @Override
    public Object calculate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        for (Pair<TupleFilter, TupleExpression> entry : whenList) {
            if (entry.getFirst().evaluate(tuple, cs)) {
                return entry.getSecond().calculate(tuple, cs);
            }
        }
        if (elseExpr != null) {
            return elseExpr.calculate(tuple, cs);
        }
        return null;
    }

    @Override
    public TupleExpression accept(ExpressionVisitor visitor) {
        return visitor.visitCaseCall(this);
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        BytesUtil.writeVInt(whenList.size(), buffer);
        for (Pair<TupleFilter, TupleExpression> whenEntry : whenList) {
            byte[] whenBytes = TupleFilterSerializer.serialize(whenEntry.getFirst(), cs);
            BytesUtil.writeByteArray(whenBytes, buffer);

            byte[] thenBytes = TupleExpressionSerializer.serialize(whenEntry.getSecond(), cs);
            BytesUtil.writeByteArray(thenBytes, buffer);
        }
        if (elseExpr != null) {
            BytesUtil.writeVInt(1, buffer);
            byte[] elseBytes = TupleExpressionSerializer.serialize(elseExpr, cs);
            BytesUtil.writeByteArray(elseBytes, buffer);
        } else {
            BytesUtil.writeVInt(-1, buffer);
        }
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        int nWhenEntries = BytesUtil.readVInt(buffer);
        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayListWithExpectedSize(nWhenEntries);
        for (int i = 0; i < nWhenEntries; i++) {
            TupleFilter tupleFilter = TupleFilterSerializer.deserialize(BytesUtil.readByteArray(buffer), cs);
            TupleExpression tupleExpression = TupleExpressionSerializer.deserialize(BytesUtil.readByteArray(buffer),
                    cs);
            whenList.add(new Pair<>(tupleFilter, tupleExpression));
        }
        this.whenList = whenList;
        int flag = BytesUtil.readVInt(buffer);
        if (flag == 1) {
            this.elseExpr = TupleExpressionSerializer.deserialize(BytesUtil.readByteArray(buffer), cs);
        }
    }

    public List<Pair<TupleFilter, TupleExpression>> getWhenList() {
        return ImmutableList.copyOf(whenList);
    }

    public TupleExpression getElseExpr() {
        return elseExpr;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(operator.toString());
        sb.append("(");
        boolean ifFirst = true;
        for (Pair<TupleFilter, TupleExpression> whenEntry : whenList) {
            if (ifFirst) {
                ifFirst = false;
            } else {
                sb.append(",");
            }
            sb.append(whenEntry.getFirst().toString());
            sb.append(",");
            sb.append(whenEntry.getSecond().toString());
        }
        if (elseExpr != null) {
            sb.append(",");
            sb.append(elseExpr.toString());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CaseTupleExpression that = (CaseTupleExpression) o;

        if (whenList != null ? !whenList.equals(that.whenList) : that.whenList != null)
            return false;
        return elseExpr != null ? elseExpr.equals(that.elseExpr) : that.elseExpr == null;
    }

    @Override
    public int hashCode() {
        int result = whenList != null ? whenList.hashCode() : 0;
        result = 31 * result + (elseExpr != null ? elseExpr.hashCode() : 0);
        return result;
    }
}
