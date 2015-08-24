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

package org.apache.kylin.storage.hbase.ii.coprocessor.endpoint;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.BitMapFilterEvaluator.BitMapProvider;
import org.junit.Test;

import com.google.common.collect.Lists;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

public class BitMapFilterEvaluatorTest {

    static TblColRef colA;
    static TblColRef colB;

    static {
        TableDesc table = TableDesc.mockup("DEFAULT.TABLE");

        ColumnDesc col = ColumnDesc.mockup(table, 1, "colA", "string");
        colA = new TblColRef(col);

        col = ColumnDesc.mockup(table, 1, "colB", "string");
        colB = new TblColRef(col);
    }

    static class MockBitMapProivder implements BitMapProvider {

        private static final int MAX_ID = 8;
        private static final int REC_COUNT = 10;

        @Override
        public ConciseSet getBitMap(TblColRef col, Integer startId, Integer endId) {
            if (!col.equals(colA))
                return null;

            // i-th record has value ID i, and last record has value null
            if (startId == null && endId == null) {
                //entry for getting null value
                ConciseSet s = new ConciseSet();
                s.add(getRecordCount() - 1);
                return s;
            }

            int start = 0;
            int end = MAX_ID;
            if (startId != null) {
                start = startId;
            }
            if (endId != null) {
                end = endId;
            }

            ConciseSet ret = new ConciseSet();
            for (int i = start; i <= end; ++i) {
                ConciseSet temp = getBitMap(col, i);
                ret.addAll(temp);
            }
            return ret;
        }

        public ConciseSet getBitMap(TblColRef col, int valueId) {
            if (!col.equals(colA))
                return null;

            // i-th record has value ID i, and last record has value null
            ConciseSet bitMap = new ConciseSet();
            if (valueId < 0 || valueId > getMaxValueId(col)) // null
                bitMap.add(getRecordCount() - 1);
            else
                bitMap.add(valueId);

            return bitMap;
        }

        @Override
        public int getRecordCount() {
            return REC_COUNT;
        }

        @Override
        public int getMaxValueId(TblColRef col) {
            return MAX_ID;
        }
    }

    BitMapFilterEvaluator eval = new BitMapFilterEvaluator(new MockBitMapProivder());
    ArrayList<CompareTupleFilter> basicFilters = Lists.newArrayList();
    ArrayList<ConciseSet> basicResults = Lists.newArrayList();

    public BitMapFilterEvaluatorTest() {
        basicFilters.add(compare(colA, FilterOperatorEnum.ISNULL));
        basicResults.add(set(9));

        basicFilters.add(compare(colA, FilterOperatorEnum.ISNOTNULL));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8));

        basicFilters.add(compare(colA, FilterOperatorEnum.EQ, 0));
        basicResults.add(set(0));

        basicFilters.add(compare(colA, FilterOperatorEnum.NEQ, 0));
        basicResults.add(set(1, 2, 3, 4, 5, 6, 7, 8));

        basicFilters.add(compare(colA, FilterOperatorEnum.IN, 0, 5));
        basicResults.add(set(0, 5));

        basicFilters.add(compare(colA, FilterOperatorEnum.NOTIN, 0, 5));
        basicResults.add(set(1, 2, 3, 4, 6, 7, 8));

        basicFilters.add(compare(colA, FilterOperatorEnum.LT, 3));
        basicResults.add(set(0, 1, 2));

        basicFilters.add(compare(colA, FilterOperatorEnum.LTE, 3));
        basicResults.add(set(0, 1, 2, 3));

        basicFilters.add(compare(colA, FilterOperatorEnum.GT, 3));
        basicResults.add(set(4, 5, 6, 7, 8));

        basicFilters.add(compare(colA, FilterOperatorEnum.GTE, 3));
        basicResults.add(set(3, 4, 5, 6, 7, 8));
    }

    @Test
    public void testBasics() {
        for (int i = 0; i < basicFilters.size(); i++) {
            assertEquals(basicResults.get(i), eval.evaluate(basicFilters.get(i)));
        }
    }

    @Test
    public void testLogicalAnd() {
        for (int i = 0; i < basicFilters.size(); i++) {
            for (int j = 0; j < basicFilters.size(); j++) {
                LogicalTupleFilter f = logical(FilterOperatorEnum.AND, basicFilters.get(i), basicFilters.get(j));
                ConciseSet r = basicResults.get(i).clone();
                r.retainAll(basicResults.get(j));
                assertEquals(r, eval.evaluate(f));
            }
        }
    }

    @Test
    public void testLogicalOr() {
        for (int i = 0; i < basicFilters.size(); i++) {
            for (int j = 0; j < basicFilters.size(); j++) {
                LogicalTupleFilter f = logical(FilterOperatorEnum.OR, basicFilters.get(i), basicFilters.get(j));
                ConciseSet r = basicResults.get(i).clone();
                r.addAll(basicResults.get(j));
                assertEquals(r, eval.evaluate(f));
            }
        }
    }

    @Test
    public void testNotEvaluable() {
        CompareTupleFilter notEvaluable = compare(colB, FilterOperatorEnum.EQ, 0);
        assertEquals(null, eval.evaluate(notEvaluable));

        LogicalTupleFilter or = logical(FilterOperatorEnum.OR, basicFilters.get(1), notEvaluable);
        assertEquals(null, eval.evaluate(or));

        LogicalTupleFilter and = logical(FilterOperatorEnum.AND, basicFilters.get(1), notEvaluable);
        assertEquals(basicResults.get(1), eval.evaluate(and));
    }

    public static CompareTupleFilter compare(TblColRef col, TupleFilter.FilterOperatorEnum op, int... ids) {
        CompareTupleFilter filter = new CompareTupleFilter(op);
        filter.addChild(columnFilter(col));
        for (int i : ids) {
            filter.addChild(constFilter(i));
        }
        return filter;
    }

    public static LogicalTupleFilter logical(TupleFilter.FilterOperatorEnum op, TupleFilter... filters) {
        LogicalTupleFilter filter = new LogicalTupleFilter(op);
        for (TupleFilter f : filters)
            filter.addChild(f);
        return filter;
    }

    public static ColumnTupleFilter columnFilter(TblColRef col) {
        return new ColumnTupleFilter(col);
    }

    public static ConstantTupleFilter constFilter(int id) {
        return new ConstantTupleFilter(idToStr(id));
    }

    public static ConciseSet set(int... ints) {
        ConciseSet set = new ConciseSet();
        for (int i : ints)
            set.add(i);
        return set;
    }

    public static String idToStr(int id) {
        byte[] bytes = new byte[] { (byte) id };
        return Dictionary.dictIdToString(bytes, 0, bytes.length);
    }

}
