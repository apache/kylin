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

package org.apache.kylin.metadata.filter;

import com.google.common.base.Preconditions;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Collection;
import java.util.List;

public class TupleFilters {

    public static TupleFilter eq(TblColRef column, Object value) {
        return compareSingle(column, TupleFilter.FilterOperatorEnum.EQ, value);
    }

    private static TupleFilter compareSingle(TblColRef column, TupleFilter.FilterOperatorEnum op, Object value) {
        CompareTupleFilter filter = new CompareTupleFilter(op);
        filter.addChild(new ColumnTupleFilter(column));
        filter.addChild(new ConstantTupleFilter(value));
        return filter;
    }

    public static TupleFilter compare(TblColRef column, TupleFilter.FilterOperatorEnum op, Collection<?> values) {
        CompareTupleFilter filter = new CompareTupleFilter(op);
        filter.addChild(new ColumnTupleFilter(column));
        filter.addChild(new ConstantTupleFilter(values));
        return filter;
    }

    public static TupleFilter like(TblColRef column, String pattern, boolean reversed) {
        BuiltInFunctionTupleFilter filter = new BuiltInFunctionTupleFilter("LIKE");
        filter.addChild(new ColumnTupleFilter(column));
        filter.addChild(new ConstantTupleFilter(pattern));
        filter.setReversed(reversed);
        return filter;
    }

    public static TupleFilter and(List<? extends TupleFilter> children) {
        Preconditions.checkNotNull(children);

        if (children.isEmpty()) {
            return ConstantTupleFilter.TRUE;
        }

        if (children.size() == 1) {
            return children.get(0);
        }

        LogicalTupleFilter result = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        for (TupleFilter child : children) {
            // AND (true,..) => ignore
            // AND (false,..) => false, short circuit
            if (child == ConstantTupleFilter.TRUE) {
                continue;
            }
            if (child == ConstantTupleFilter.FALSE) {
                return child;
            }

            if (child.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                // child = and(child.getChildren()); calcite may have already flatten "and"
                result.addChildren(child.getChildren());
            } else {
                result.addChild(child);
            }
        }

        if (result.getChildren().isEmpty()) {
            return ConstantTupleFilter.TRUE; // AND(true, true) => true
        }
        return result;
    }

    public static TupleFilter or(List<? extends TupleFilter> children) {
        Preconditions.checkNotNull(children);

        if (children.isEmpty()) {
            return ConstantTupleFilter.TRUE;
        }

        if (children.size() == 1) {
            return children.get(0);
        }

        LogicalTupleFilter result = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        for (TupleFilter child : children) {
            // OR (true,..) => true, short circuit
            // OR (false,..) => ignore
            if (child == ConstantTupleFilter.TRUE) {
                return child;
            }
            if (child == ConstantTupleFilter.FALSE) {
                continue;
            }

            if (child.getOperator() == TupleFilter.FilterOperatorEnum.OR) {
                // child = or(child.getChildren()); calcite may have already flatten "or"
                result.addChildren(child.getChildren());
            } else {
                result.addChild(child);
            }
        }

        if (result.getChildren().isEmpty()) {
            return ConstantTupleFilter.FALSE; // OR(false, false) => false
        }
        return result;
    }

    public static TupleFilter not(TupleFilter filter) {
        Preconditions.checkNotNull(filter);

        if (filter == ConstantTupleFilter.TRUE) {
            return ConstantTupleFilter.FALSE;
        }

        if (filter == ConstantTupleFilter.FALSE) {
            return ConstantTupleFilter.TRUE;
        }

        if (filter.getOperator() == TupleFilter.FilterOperatorEnum.NOT) {
            return filter.getChildren().get(0);
        }

        LogicalTupleFilter notFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.NOT);
        notFilter.addChild(filter);
        return notFilter;
    }
}
