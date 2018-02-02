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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import com.google.common.collect.Lists;

public class LogicalTupleFilter extends TupleFilter implements IOptimizeableTupleFilter {

    public LogicalTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);

        boolean opGood = (op == FilterOperatorEnum.AND || op == FilterOperatorEnum.OR || op == FilterOperatorEnum.NOT);
        if (opGood == false)
            throw new IllegalArgumentException("Unsupported operator " + op);
    }

    //private
    private LogicalTupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
        super(filters, op);
    }

    private void reinitWithChildren(List<TupleFilter> newTupleFilter) {
        this.children.clear();
        this.addChildren(newTupleFilter);
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
            throw new IllegalStateException("NOT will be replaced in org.apache.kylin.query.relnode.OLAPFilterRel.TupleFilterVisitor");
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
        return operator + " " + children;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        switch (this.operator) {
        case AND:
            return evalAnd(tuple, cs);
        case OR:
            return evalOr(tuple, cs);
        case NOT:
            return evalNot(tuple, cs);
        default:
            return false;
        }
    }

    private boolean evalAnd(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        for (TupleFilter filter : this.children) {
            if (!filter.evaluate(tuple, cs)) {
                return false;
            }
        }
        return true;
    }

    private boolean evalOr(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        for (TupleFilter filter : this.children) {
            if (filter.evaluate(tuple, cs)) {
                return true;
            }
        }
        return false;
    }

    private boolean evalNot(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        return !this.children.get(0).evaluate(tuple, cs);
    }

    @Override
    public Collection<?> getValues() {
        return Collections.emptyList();
    }

    @Override
    public boolean isEvaluable() {
        switch (operator) {
        case NOT:
            // Un-evaluatable branch will be pruned and be replaced with TRUE.
            // And this must happen at the top NOT, otherwise NOT (TRUE) becomes false.
            for (TupleFilter child : children) {
                if (TupleFilter.isEvaluableRecursively(child) == false)
                    return false;
            }
            return true;
        case OR:
            // (anything OR un-evaluable) will become (anything or TRUE) which is effectively TRUE.
            // The "anything" is not evaluated, kinda disabled, by the un-evaluable part.
            // If it's partially un-evaluable, then "anything" is partially disabled, and the OR is still not fully evaluatable.
            for (TupleFilter child : children) {
                if (TupleFilter.isEvaluableRecursively(child) == false)
                    return false;
            }
            return true;
        default:
            return true;
        }
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        //do nothing
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {

    }

    @Override
    public TupleFilter acceptOptimizeTransformer(FilterOptimizeTransformer transformer) {
        List<TupleFilter> newChildren = Lists.newArrayList();
        for (TupleFilter child : this.getChildren()) {
            if (child instanceof IOptimizeableTupleFilter) {
                newChildren.add(((IOptimizeableTupleFilter) child).acceptOptimizeTransformer(transformer));
            } else {
                newChildren.add(child);
            }
        }

        this.reinitWithChildren(newChildren);

        return transformer.visit(this);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof LogicalTupleFilter)) {
            return false;
        }
        final LogicalTupleFilter otherFilter = (LogicalTupleFilter) other;
        if (otherFilter.operator != this.operator || otherFilter.children.size() != this.children.size()) {
            return false;
        }

        for (int i = 0; i < otherFilter.children.size(); i++) {
            if (!otherFilter.children.get(i).equals(this.children.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (operator == null ? 0 : operator.hashCode()) + 31 * this.children.hashCode();
    }
}
