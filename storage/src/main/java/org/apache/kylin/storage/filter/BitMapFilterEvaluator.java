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

package org.apache.kylin.storage.filter;

import java.util.List;

import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * @author yangli9
 *
 * Evaluate a group of records against a filter in batch.
 */
public class BitMapFilterEvaluator {

    /** Provides bitmaps for a record group ranging [0..N-1], where N is the size of the group */
    public static interface BitMapProvider {

        /** return records whose specified column having specified value */
        ConciseSet getBitMap(TblColRef col, Integer startId, Integer endId);

        /** return the size of the group */
        int getRecordCount();

        /** return the max value ID of a column according to dictionary */
        int getMaxValueId(TblColRef col);
    }

    BitMapProvider provider;

    public BitMapFilterEvaluator(BitMapProvider bitMapProvider) {
        this.provider = bitMapProvider;
    }

    /**
     * @param filter
     * @return a set of records that match the filter; or null if filter is null or unable to evaluate
     */
    public ConciseSet evaluate(TupleFilter filter) {
        if (filter == null)
            return null;

        if (filter instanceof LogicalTupleFilter)
            return evalLogical((LogicalTupleFilter) filter);

        if (filter instanceof CompareTupleFilter)
            return evalCompare((CompareTupleFilter) filter);

        if (filter instanceof ConstantTupleFilter) {
            if (!filter.evaluate(null, null)) {
                return new ConciseSet();
            }
        }

        return null; // unable to evaluate
    }

    private ConciseSet evalCompare(CompareTupleFilter filter) {
        switch (filter.getOperator()) {
        case ISNULL:
            return evalCompareIsNull(filter);
        case ISNOTNULL:
            return evalCompareIsNotNull(filter);
        case EQ:
            return evalCompareEqual(filter);
        case NEQ:
            return evalCompareNotEqual(filter);
        case IN:
            return evalCompareIn(filter);
        case NOTIN:
            return evalCompareNotIn(filter);
        case LT:
            return evalCompareLT(filter);
        case LTE:
            return evalCompareLTE(filter);
        case GT:
            return evalCompareGT(filter);
        case GTE:
            return evalCompareGTE(filter);
        default:
            throw new IllegalStateException("Unsupported operator " + filter.getOperator());
        }
    }

    private ConciseSet evalCompareLT(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId((String) filter.getFirstValue());
        return collectRange(filter.getColumn(), null, id - 1);
    }

    private ConciseSet evalCompareLTE(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId((String) filter.getFirstValue());
        return collectRange(filter.getColumn(), null, id);
    }

    private ConciseSet evalCompareGT(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId((String) filter.getFirstValue());
        return collectRange(filter.getColumn(), id + 1, null);
    }

    private ConciseSet evalCompareGTE(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId((String) filter.getFirstValue());
        return collectRange(filter.getColumn(), id, null);
    }

    private ConciseSet collectRange(TblColRef column, Integer startId, Integer endId) {
        return provider.getBitMap(column, startId, endId);
    }

    private ConciseSet evalCompareEqual(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId((String) filter.getFirstValue());
        ConciseSet bitMap = provider.getBitMap(filter.getColumn(), id, id);
        if (bitMap == null)
            return null;
        return bitMap.clone(); // NOTE the clone() to void messing provider's cache
    }

    private ConciseSet evalCompareNotEqual(CompareTupleFilter filter) {
        ConciseSet set = evalCompareEqual(filter);
        not(set);
        dropNull(set, filter);
        return set;
    }

    private ConciseSet evalCompareIn(CompareTupleFilter filter) {
        ConciseSet set = new ConciseSet();
        for (Object value : filter.getValues()) {
            int id = Dictionary.stringToDictId((String) value);
            ConciseSet bitMap = provider.getBitMap(filter.getColumn(), id, id);
            if (bitMap == null)
                return null;
            set.addAll(bitMap);
        }
        return set;
    }

    private ConciseSet evalCompareNotIn(CompareTupleFilter filter) {
        ConciseSet set = evalCompareIn(filter);
        not(set);
        dropNull(set, filter);
        return set;
    }

    private void dropNull(ConciseSet set, CompareTupleFilter filter) {
        if (set == null)
            return;

        ConciseSet nullSet = evalCompareIsNull(filter);
        set.removeAll(nullSet);
    }

    private ConciseSet evalCompareIsNull(CompareTupleFilter filter) {
        ConciseSet bitMap = provider.getBitMap(filter.getColumn(), null, null);
        if (bitMap == null)
            return null;
        return bitMap.clone(); // NOTE the clone() to void messing provider's cache
    }

    private ConciseSet evalCompareIsNotNull(CompareTupleFilter filter) {
        ConciseSet set = evalCompareIsNull(filter);
        not(set);
        return set;
    }

    private ConciseSet evalLogical(LogicalTupleFilter filter) {
        List<? extends TupleFilter> children = filter.getChildren();

        switch (filter.getOperator()) {
        case AND:
            return evalLogicalAnd(children);
        case OR:
            return evalLogicalOr(children);
        case NOT:
            return evalLogicalNot(children);
        default:
            throw new IllegalStateException("Unsupported operator " + filter.getOperator());
        }
    }

    private ConciseSet evalLogicalAnd(List<? extends TupleFilter> children) {
        ConciseSet set = new ConciseSet();
        not(set);

        for (TupleFilter c : children) {
            ConciseSet t = evaluate(c);
            if (t == null)
                continue; // because it's AND

            set.retainAll(t);
        }
        return set;
    }

    private ConciseSet evalLogicalOr(List<? extends TupleFilter> children) {
        ConciseSet set = new ConciseSet();

        for (TupleFilter c : children) {
            ConciseSet t = evaluate(c);
            if (t == null)
                return null; // because it's OR

            set.addAll(t);
        }
        return set;
    }

    private ConciseSet evalLogicalNot(List<? extends TupleFilter> children) {
        ConciseSet set = evaluate(children.get(0));
        not(set);
        return set;
    }

    private void not(ConciseSet set) {
        if (set == null)
            return;

        set.add(provider.getRecordCount());
        set.complement();
    }

    public static void main(String[] args) {
        ConciseSet s = new ConciseSet();
        s.add(5);
        s.complement();
        System.out.println(s);
    }
}
