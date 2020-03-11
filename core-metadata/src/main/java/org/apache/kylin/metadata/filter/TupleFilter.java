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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * 
 * @author xjiang
 * 
 */
public abstract class TupleFilter {

    static final Logger logger = LoggerFactory.getLogger(TupleFilter.class);

    public enum FilterOperatorEnum {
        EQ(1), NEQ(2), GT(3), LT(4), GTE(5), LTE(6), ISNULL(7), ISNOTNULL(8), IN(9), NOTIN(10), AND(20), OR(21), NOT(22), COLUMN(30), CONSTANT(31), DYNAMIC(32), EXTRACT(33), CASE(34), FUNCTION(35), MASSIN(36), EVAL_FUNC(37), UNSUPPORTED(38);

        private final int value;

        private FilterOperatorEnum(int v) {
            this.value = v;
        }

        public int getValue() {
            return this.value;
        }
    }

    public static final int BUFFER_SIZE = 10240;

    protected static final Map<FilterOperatorEnum, FilterOperatorEnum> REVERSE_OP_MAP = Maps.newHashMap();
    protected static final Map<FilterOperatorEnum, FilterOperatorEnum> SWAP_OP_MAP = Maps.newHashMap();

    static {
        REVERSE_OP_MAP.put(FilterOperatorEnum.EQ, FilterOperatorEnum.NEQ);
        REVERSE_OP_MAP.put(FilterOperatorEnum.NEQ, FilterOperatorEnum.EQ);
        REVERSE_OP_MAP.put(FilterOperatorEnum.GT, FilterOperatorEnum.LTE);
        REVERSE_OP_MAP.put(FilterOperatorEnum.LTE, FilterOperatorEnum.GT);
        REVERSE_OP_MAP.put(FilterOperatorEnum.LT, FilterOperatorEnum.GTE);
        REVERSE_OP_MAP.put(FilterOperatorEnum.GTE, FilterOperatorEnum.LT);
        REVERSE_OP_MAP.put(FilterOperatorEnum.IN, FilterOperatorEnum.NOTIN);
        REVERSE_OP_MAP.put(FilterOperatorEnum.NOTIN, FilterOperatorEnum.IN);
        REVERSE_OP_MAP.put(FilterOperatorEnum.ISNULL, FilterOperatorEnum.ISNOTNULL);
        REVERSE_OP_MAP.put(FilterOperatorEnum.ISNOTNULL, FilterOperatorEnum.ISNULL);
        REVERSE_OP_MAP.put(FilterOperatorEnum.AND, FilterOperatorEnum.OR);
        REVERSE_OP_MAP.put(FilterOperatorEnum.OR, FilterOperatorEnum.AND);

        SWAP_OP_MAP.put(FilterOperatorEnum.EQ, FilterOperatorEnum.EQ);
        SWAP_OP_MAP.put(FilterOperatorEnum.NEQ, FilterOperatorEnum.NEQ);
        SWAP_OP_MAP.put(FilterOperatorEnum.GT, FilterOperatorEnum.LT);
        SWAP_OP_MAP.put(FilterOperatorEnum.LTE, FilterOperatorEnum.GTE);
        SWAP_OP_MAP.put(FilterOperatorEnum.LT, FilterOperatorEnum.GT);
        SWAP_OP_MAP.put(FilterOperatorEnum.GTE, FilterOperatorEnum.LTE);
    }
    
    public static CompareTupleFilter compare(TblColRef col, FilterOperatorEnum op) {
        CompareTupleFilter r = new CompareTupleFilter(op);
        r.addChild(new ColumnTupleFilter(col));
        return r;
    }
    
    public static CompareTupleFilter compare(TblColRef col, FilterOperatorEnum op, Object val) {
        CompareTupleFilter r = new CompareTupleFilter(op);
        r.addChild(new ColumnTupleFilter(col));
        if (val instanceof TupleFilter)
            r.addChild((TupleFilter) val);
        else if (val instanceof TblColRef)
            r.addChild(new ColumnTupleFilter((TblColRef) col));
        else
            r.addChild(new ConstantTupleFilter(val));
        return r;
    }

    public static LogicalTupleFilter and(TupleFilter... children) {
        LogicalTupleFilter r = new LogicalTupleFilter(FilterOperatorEnum.AND);
        r.addChildren(children);
        return r;
    }
    
    public static LogicalTupleFilter or(TupleFilter... children) {
        LogicalTupleFilter r = new LogicalTupleFilter(FilterOperatorEnum.OR);
        r.addChildren(children);
        return r;
    }
    
    public static LogicalTupleFilter not(TupleFilter child) {
        LogicalTupleFilter r = new LogicalTupleFilter(FilterOperatorEnum.NOT);
        r.addChild(child);
        return r;
    }
    
    // ============================================================================

    protected final List<TupleFilter> children;
    protected FilterOperatorEnum operator;

    protected TupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
        this.children = filters;
        this.operator = op;
    }

    public void addChild(TupleFilter child) {
        children.add(child);
    }

    final public void addChildren(List<? extends TupleFilter> children) {
        for (TupleFilter c : children)
            addChild(c); // subclass overrides addChild()
    }

    final public void addChildren(TupleFilter... children) {
        for (TupleFilter c : children)
            addChild(c); // subclass overrides addChild()
    }

    public List<? extends TupleFilter> getChildren() {
        return children;
    }

    public boolean hasChildren() {
        return children != null && !children.isEmpty();
    }

    public FilterOperatorEnum getOperator() {
        return operator;
    }

    public TupleFilter copy() {
        throw new UnsupportedOperationException();
    }

    public TupleFilter reverse() {
        logger.warn("Cannot reverse " + this + ", loosen the filter to true");
        return ConstantTupleFilter.TRUE;
    }

    /**
     * The storage level dislike NOT logic
     */
    public TupleFilter removeNot() {
        return removeNotInternal(this);
    }

    private TupleFilter removeNotInternal(TupleFilter filter) {
        FilterOperatorEnum op = filter.getOperator();

        if (!(filter instanceof LogicalTupleFilter)) {
            return filter;
        }

        LogicalTupleFilter logicalFilter = (LogicalTupleFilter) filter;

        switch (logicalFilter.operator) {
        case NOT:
            assert (filter.children.size() == 1);
            TupleFilter reverse = filter.children.get(0).reverse();
            return removeNotInternal(reverse);
        case AND:
            LogicalTupleFilter andFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
            for (TupleFilter child : logicalFilter.children) {
                andFilter.addChild(removeNotInternal(child));
            }
            return andFilter;
        case OR:
            LogicalTupleFilter orFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
            for (TupleFilter child : logicalFilter.children) {
                orFilter.addChild(removeNotInternal(child));
            }
            return orFilter;
        default:
            throw new IllegalStateException("This filter is unexpected: " + filter);
        }
    }

    /**
     * flatten to OR-AND filter, (A AND B AND ..) OR (C AND D AND ..) OR ..
     * flatten filter will ONLY contain AND and OR , no NOT will exist.
     * This will help to decide scan ranges.
     * 
     * Notice that the flatten filter will ONLY be used for determining scan ranges,
     * The filter that is later pushed down into storage level is still the ORIGINAL
     * filter, since the flattened filter will be too "fat" to evaluate
     * 
     * @return
     */
    public TupleFilter flatFilter() {
        return flatFilter(KylinConfig.getInstanceFromEnv().getFlatFilterMaxChildrenSize());
    }

    /**
     * throws IllegalStateException when the flat children exceed the maxFlatChildrenSize
     * @param maxFlatChildrenSize
     * @return
     */
    public TupleFilter flatFilter(int maxFlatChildrenSize) {
        return flattenInternal(this, maxFlatChildrenSize);
    }

    private TupleFilter flattenInternal(TupleFilter filter, int maxFlatChildrenSize) {
        TupleFilter flatFilter = null;
        if (!(filter instanceof LogicalTupleFilter)) {
            flatFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
            flatFilter.addChild(filter);
            return flatFilter;
        }

        // post-order recursive travel
        FilterOperatorEnum op = filter.getOperator();
        List<TupleFilter> andChildren = new LinkedList<TupleFilter>();
        List<TupleFilter> orChildren = new LinkedList<TupleFilter>();
        for (TupleFilter child : filter.getChildren()) {
            TupleFilter flatChild = flattenInternal(child, maxFlatChildrenSize);
            FilterOperatorEnum childOp = flatChild.getOperator();
            if (childOp == FilterOperatorEnum.AND) {
                andChildren.add(flatChild);
            } else if (childOp == FilterOperatorEnum.OR) {
                orChildren.add(flatChild);
            } else {
                throw new IllegalStateException("Filter is " + filter + " and child is " + flatChild);
            }
        }

        // boolean algebra flatten
        if (op == FilterOperatorEnum.AND) {
            flatFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
            for (TupleFilter andChild : andChildren) {
                flatFilter.addChildren(andChild.getChildren());
            }
            if (!orChildren.isEmpty()) {
                List<TupleFilter> fullAndFilters = cartesianProduct(orChildren, flatFilter, maxFlatChildrenSize);
                flatFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
                flatFilter.addChildren(fullAndFilters);
            }
        } else if (op == FilterOperatorEnum.OR) {
            flatFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
            for (TupleFilter orChild : orChildren) {
                flatFilter.addChildren(orChild.getChildren());
            }
            flatFilter.addChildren(andChildren);
        } else if (op == FilterOperatorEnum.NOT) {
            assert (filter.children.size() == 1);
            TupleFilter reverse = filter.children.get(0).reverse();
            flatFilter = flattenInternal(reverse, maxFlatChildrenSize);
        } else {
            throw new IllegalStateException("Filter is " + filter);
        }
        if (flatFilter.getChildren() != null && flatFilter.getChildren().size() > maxFlatChildrenSize) {
            throw new IllegalStateException("the filter is too large after do the flat, size="
                    + flatFilter.getChildren().size());
        }
        return flatFilter;
    }

    private List<TupleFilter> cartesianProduct(List<TupleFilter> leftOrFilters, TupleFilter partialAndFilter, int maxFlatChildrenSize) {
        List<TupleFilter> oldProductFilters = new LinkedList<TupleFilter>();
        oldProductFilters.add(partialAndFilter);

        int flatChildrenSize = 1;
        for (TupleFilter orFilter : leftOrFilters) {
            flatChildrenSize *= orFilter.getChildren().size();
            if (flatChildrenSize > maxFlatChildrenSize) {
                throw new IllegalStateException("the filter is too large after do the flat, size="
                        + flatChildrenSize);
            }
        }
        for (TupleFilter orFilter : leftOrFilters) {
            List<TupleFilter> newProductFilters = new LinkedList<TupleFilter>();
            for (TupleFilter orChildFilter : orFilter.getChildren()) {
                for (TupleFilter productFilter : oldProductFilters) {
                    TupleFilter fullAndFilter = productFilter.copy();
                    fullAndFilter.addChildren(orChildFilter.getChildren());
                    newProductFilters.add(fullAndFilter);
                }
            }
            oldProductFilters = newProductFilters;
        }
        return oldProductFilters;
    }
    
    public HashMap<TblColRef, Object> findMustEqualColsAndValues(Collection<TblColRef> lookingForCols) {
        HashMap<TblColRef, Object> result = new HashMap<>();
        findMustEqualColsAndValues(this, lookingForCols, result);
        return result;
    }

    private void findMustEqualColsAndValues(TupleFilter filter, Collection<TblColRef> lookingForCols, HashMap<TblColRef, Object> result) {
        if (filter instanceof CompareTupleFilter) {
            CompareTupleFilter comp = (CompareTupleFilter) filter;
            TblColRef col = comp.getColumn();
            if (lookingForCols.contains(col)) {
                if (comp.getOperator() == FilterOperatorEnum.EQ)
                    result.put(col, comp.getFirstValue());
                else if (comp.getOperator() == FilterOperatorEnum.ISNULL)
                    result.put(col, null);
            }
            return;
        }

        if (filter instanceof LogicalTupleFilter) {
            LogicalTupleFilter logic = (LogicalTupleFilter) filter;
            if (logic.getOperator() == FilterOperatorEnum.AND) {
                for (TupleFilter child : logic.getChildren())
                    findMustEqualColsAndValues(child, lookingForCols, result);
            }
            return;
        }
    }

    //find must true compareTupleFilter
    public Set<CompareTupleFilter> findMustTrueCompareFilters() {
        Set<CompareTupleFilter> result = Sets.newHashSet();
        findMustTrueCompareFilters(this, result);
        return result;
    }

    private void findMustTrueCompareFilters(TupleFilter filter, Set<CompareTupleFilter> result) {
        if (filter instanceof CompareTupleFilter) {
            if (((CompareTupleFilter) filter).getColumn() != null) {
                result.add((CompareTupleFilter) filter);
            }
            return;
        }
        
        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == FilterOperatorEnum.AND) {
                for (TupleFilter child : filter.getChildren()) {
                    findMustTrueCompareFilters(child, result);
                }
            }
            return;
        }
    }
    
    public abstract boolean isEvaluable();

    public abstract boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs);

    public abstract Collection<?> getValues();

    public abstract void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public abstract void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer);

    public static boolean isEvaluableRecursively(TupleFilter filter) {
        if (filter == null)
            return true;

        if (!filter.isEvaluable())
            return false;

        for (TupleFilter child : filter.getChildren()) {
            if (!isEvaluableRecursively(child))
                return false;
        }
        return true;
    }

    public static void collectColumns(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null || collector == null)
            return;

        if (filter instanceof ColumnTupleFilter) {
            ColumnTupleFilter columnTupleFilter = (ColumnTupleFilter) filter;
            collector.add(columnTupleFilter.getColumn());
        }

        for (TupleFilter child : filter.getChildren()) {
            collectColumns(child, collector);
        }
    }

}
