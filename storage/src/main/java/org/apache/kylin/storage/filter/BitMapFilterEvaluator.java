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

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;

import org.roaringbitmap.RoaringBitmap;

/**
 * @author yangli9
 *
 * Evaluate a group of records against a filter in batch.
 */
public class BitMapFilterEvaluator {

    /** Provides bitmaps for a record group ranging [0..N-1], where N is the size of the group */
    public static interface BitMapProvider {

        /** return records whose specified column having specified value */
        RoaringBitmap getBitMap(TblColRef col, Integer startId, Integer endId);

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
    public RoaringBitmap evaluate(TupleFilter filter) {
        if (filter == null)
            return null;

        if (filter instanceof LogicalTupleFilter)
            return evalLogical((LogicalTupleFilter) filter);

        if (filter instanceof CompareTupleFilter)
            return evalCompare((CompareTupleFilter) filter);

        return null; // unable to evaluate
    }

    private RoaringBitmap evalCompare(CompareTupleFilter filter) {
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

    private RoaringBitmap evalCompareLT(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId(filter.getFirstValue());
        return collectRange(filter.getColumn(), null, id - 1);
    }

    private RoaringBitmap evalCompareLTE(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId(filter.getFirstValue());
        return collectRange(filter.getColumn(), null, id);
    }

    private RoaringBitmap evalCompareGT(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId(filter.getFirstValue());
        return collectRange(filter.getColumn(), id + 1, null);
    }

    private RoaringBitmap evalCompareGTE(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId(filter.getFirstValue());
        return collectRange(filter.getColumn(), id, null);
    }

    private RoaringBitmap collectRange(TblColRef column, Integer startId, Integer endId) {
        return provider.getBitMap(column, startId, endId);
    }

    private RoaringBitmap evalCompareEqual(CompareTupleFilter filter) {
        int id = Dictionary.stringToDictId(filter.getFirstValue());
        RoaringBitmap bitMap = provider.getBitMap(filter.getColumn(), id, id);
        if (bitMap == null)
            return null;
        return bitMap.clone(); // NOTE the clone() to void messing provider's cache // If the object is immutable, this is likely wasteful
    }

    private RoaringBitmap evalCompareNotEqual(CompareTupleFilter filter) {
        RoaringBitmap set = evalCompareEqual(filter);
        not(set);
        dropNull(set, filter);
        return set;
    }

    private RoaringBitmap evalCompareIn(CompareTupleFilter filter) {
        java.util.ArrayList<RoaringBitmap> buffer = new java.util.ArrayList<RoaringBitmap>();
        // an iterator would be better than an ArrayList, but there is
        // the convention that says that if one bitmap is null, we return null...
        for (String value : filter.getValues()) {
            int id = Dictionary.stringToDictId(value);
            RoaringBitmap bitMap = provider.getBitMap(filter.getColumn(), id, id);
            if (bitMap == null)
                return null;
            buffer.add(bitMap);
        }
        return RoaringBitmap.or(buffer.iterator());
    }

    private RoaringBitmap evalCompareNotIn(CompareTupleFilter filter) {
        RoaringBitmap set = evalCompareIn(filter);
        not(set);
        dropNull(set, filter);
        return set;
    }

    private void dropNull(RoaringBitmap set, CompareTupleFilter filter) {
        if (set == null)
            return;

        RoaringBitmap nullSet = evalCompareIsNull(filter);
        set.andNot(nullSet);
    }

    private RoaringBitmap evalCompareIsNull(CompareTupleFilter filter) {
        RoaringBitmap bitMap = provider.getBitMap(filter.getColumn(), null, null);
        if (bitMap == null)
            return null;
        return bitMap.clone(); // NOTE the clone() to void messing provider's cache // If the object is immutable, this is likely wasteful
    }

    private RoaringBitmap evalCompareIsNotNull(CompareTupleFilter filter) {
        RoaringBitmap set = evalCompareIsNull(filter);
        not(set);
        return set;
    }

    private RoaringBitmap evalLogical(LogicalTupleFilter filter) {
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

    private RoaringBitmap evalLogicalAnd(List<? extends TupleFilter> children) {
        int i = 0;
        RoaringBitmap answer = null;
        // we identify the first non-null
        for(; (i < children.size()) && (answer == null); ++i) {
            RoaringBitmap t = evaluate(children.get(i));
            if (t == null)
                continue; // because it's AND // following convention
            answer = t;
        }
        // then we compute the intersections
        for(; i < children.size(); ++i) {
            RoaringBitmap t = evaluate(children.get(i));
            if (t == null)
                continue; // because it's AND // following convention
            answer.and(t);
        }
        if(answer == null)
            answer = new RoaringBitmap();
        return answer;
    }

    private RoaringBitmap evalLogicalOr(List<? extends TupleFilter> children) {
        java.util.ArrayList<RoaringBitmap> buffer = new java.util.ArrayList<RoaringBitmap>();
        // could be done with iterator but there is the rule if that if there is a null, then we need to return null
        for (TupleFilter c : children) {
            RoaringBitmap t = evaluate(c);
            if (t == null)
                return null; // because it's OR // following convention
            buffer.add(t);
        }
        return RoaringBitmap.or(buffer.iterator());
    }

    private RoaringBitmap evalLogicalNot(List<? extends TupleFilter> children) {
        RoaringBitmap set = evaluate(children.get(0));
        not(set);
        return set;
    }

    private void not(RoaringBitmap set) {
        if (set == null)
            return;
        set.flip(0,provider.getRecordCount());
    }


}
