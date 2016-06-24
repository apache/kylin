/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.gridtable;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.apache.kylin.metadata.datatype.StringSerializer;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

public class SimpleInvertedIndexTest extends LocalFileMetadataTestCase {

    GTInfo info;
    GTInvertedIndex index;
    ArrayList<CompareTupleFilter> basicFilters = Lists.newArrayList();
    ArrayList<ConciseSet> basicResults = Lists.newArrayList();

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    public SimpleInvertedIndexTest() {

        info = UnitTestSupport.advancedInfo();
        TblColRef colA = info.colRef(0);

        // block i contains value "i", the last is NULL
        index = new GTInvertedIndex(info);
        GTRowBlock mockBlock = GTRowBlock.allocate(info);
        GTRowBlock.Writer writer = mockBlock.getWriter();
        GTRecord record = new GTRecord(info);
        for (int i = 0; i < 10; i++) {
            record.setValues(i < 9 ? "" + i : null, "", "", new LongMutable(0), new BigDecimal(0));
            for (int j = 0; j < info.getRowBlockSize(); j++) {
                writer.append(record);
            }
            writer.readyForFlush();
            index.add(mockBlock);

            writer.clearForNext();
        }

        basicFilters.add(compare(colA, FilterOperatorEnum.ISNULL));
        basicResults.add(set(9));

        basicFilters.add(compare(colA, FilterOperatorEnum.ISNOTNULL));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        basicFilters.add(compare(colA, FilterOperatorEnum.EQ, 0));
        basicResults.add(set(0));

        basicFilters.add(compare(colA, FilterOperatorEnum.NEQ, 0));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        basicFilters.add(compare(colA, FilterOperatorEnum.IN, 0, 5));
        basicResults.add(set(0, 5));

        basicFilters.add(compare(colA, FilterOperatorEnum.NOTIN, 0, 5));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

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
            assertEquals(basicResults.get(i), index.filter(basicFilters.get(i)));
        }
    }

    @Test
    public void testLogicalAnd() {
        for (int i = 0; i < basicFilters.size(); i++) {
            for (int j = 0; j < basicFilters.size(); j++) {
                LogicalTupleFilter f = logical(FilterOperatorEnum.AND, basicFilters.get(i), basicFilters.get(j));
                ConciseSet r = basicResults.get(i).clone();
                r.retainAll(basicResults.get(j));
                assertEquals(r, index.filter(f));
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
                assertEquals(r, index.filter(f));
            }
        }
    }

    @Test
    public void testNotEvaluable() {
        ConciseSet all = set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        CompareTupleFilter notEvaluable = compare(info.colRef(1), FilterOperatorEnum.EQ, 0);
        assertEquals(all, index.filter(notEvaluable));

        LogicalTupleFilter or = logical(FilterOperatorEnum.OR, basicFilters.get(0), notEvaluable);
        assertEquals(all, index.filter(or));

        LogicalTupleFilter and = logical(FilterOperatorEnum.AND, basicFilters.get(0), notEvaluable);
        assertEquals(basicResults.get(0), index.filter(and));
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
        byte[] space = new byte[10];
        ByteBuffer buf = ByteBuffer.wrap(space);
        StringSerializer stringSerializer = new StringSerializer(DataType.getType("string"));
        stringSerializer.serialize("" + id, buf);
        ByteArray data = new ByteArray(buf.array(), buf.arrayOffset(), buf.position());
        return new ConstantTupleFilter(data);
    }

    public static ConciseSet set(int... ints) {
        ConciseSet set = new ConciseSet();
        for (int i : ints)
            set.add(i);
        return set;
    }

}
