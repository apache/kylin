/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.storage.filter;

import com.kylinolap.storage.tuple.Tuple;
import org.apache.commons.lang3.Validate;

import java.util.*;

public class LogicalTupleFilter extends TupleFilter {

    public LogicalTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);
        Validate.isTrue(op == FilterOperatorEnum.AND || op == FilterOperatorEnum.OR
                || op == FilterOperatorEnum.NOT);
    }

    private LogicalTupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
        super(filters, op);
    }

    @Override
    public TupleFilter copy() {
        List<TupleFilter> cloneChildren = new LinkedList<TupleFilter>(children);
        TupleFilter cloneTuple = new LogicalTupleFilter(cloneChildren, operator);
        return cloneTuple;
    }

    @Override
    public TupleFilter reverse() {
        switch (operator) {
            case NOT:
                Validate.isTrue(children.size() == 1);
                return children.get(0);
            case AND:
            case OR:
                LogicalTupleFilter reverse = new LogicalTupleFilter(REVERSE_OP_MAP.get(operator));
                for (TupleFilter child : children) {
                    reverse.addChild(child.reverse());
                }
                return reverse;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public String toString() {
        return "LogicalFilter [operator=" + operator + ", children=" + children + "]";
    }

    @Override
    public boolean evaluate(Tuple tuple) {
        switch (this.operator) {
            case AND:
                return evalAnd(tuple);
            case OR:
                return evalOr(tuple);
            case NOT:
                return evalNot(tuple);
            default:
                return false;
        }
    }

    private boolean evalAnd(Tuple tuple) {
        for (TupleFilter filter : this.children) {
            if (!filter.evaluate(tuple)) {
                return false;
            }
        }
        return true;
    }

    private boolean evalOr(Tuple tuple) {
        for (TupleFilter filter : this.children) {
            if (filter.evaluate(tuple)) {
                return true;
            }
        }
        return false;
    }

    private boolean evalNot(Tuple tuple) {
        return !this.children.get(0).evaluate(tuple);
    }

    @Override
    public Collection<String> getValues() {
        return Collections.emptyList();
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] bytes) {
    }

}
