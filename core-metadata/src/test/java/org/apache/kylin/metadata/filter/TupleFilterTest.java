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

import static org.apache.kylin.metadata.filter.TupleFilter.and;
import static org.apache.kylin.metadata.filter.TupleFilter.compare;
import static org.apache.kylin.metadata.filter.TupleFilter.not;

import java.util.HashMap;
import java.util.Set;

import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TblColRef.InnerDataTypeEnum;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class TupleFilterTest {
    @Test
    // true ==> true
    public void removeNotTest0() {
        TupleFilter constFilter = ConstantTupleFilter.TRUE;
        Assert.assertEquals(constFilter, constFilter.removeNot());
    }

    @Test
    // Not(true) ==> false
    public void removeNotTest1() {
        TupleFilter notFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.NOT);
        notFilter.addChild(ConstantTupleFilter.TRUE);
        Assert.assertEquals(ConstantTupleFilter.FALSE, notFilter.removeNot());
    }

    @Test
    // Not(And(true, false)) ==> Or(false, true)
    public void removeNotTest2() {
        TupleFilter notFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.NOT);
        TupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        andFilter.addChildren(ConstantTupleFilter.TRUE, ConstantTupleFilter.FALSE);
        notFilter.addChild(andFilter);

        TupleFilter orFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        orFilter.addChildren(ConstantTupleFilter.FALSE, ConstantTupleFilter.TRUE);
        Assert.assertEquals(orFilter, notFilter.removeNot());
    }

    @Test
    // And(Not(true), false) ==> And(false, false)
    public void removeNotTest3() {
        TupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        TupleFilter notFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.NOT);
        notFilter.addChild(ConstantTupleFilter.TRUE);
        andFilter.addChildren(notFilter, ConstantTupleFilter.FALSE);

        TupleFilter andFilter2 = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        andFilter2.addChildren(ConstantTupleFilter.FALSE, ConstantTupleFilter.FALSE);
        Assert.assertEquals(andFilter2, andFilter.removeNot());
    }
    
    @Test
    public void testFindMustEqualColsAndValues() {
        TableDesc tbl = TableDesc.mockup("mockup_table");
        TblColRef colA = TblColRef.mockup(tbl, 0, "A", "bigint");
        TblColRef colB = TblColRef.mockup(tbl, 1, "B", "char(256)");
        Set<TblColRef> cols = Sets.newHashSet(colA, colB);
        
        {
            TupleFilter f = compare(colA, FilterOperatorEnum.EQ, "1234");
            Assert.assertEquals(map(colA, "1234"), f.findMustEqualColsAndValues(cols));
        }
        
        {
            TupleFilter f = compare(colA, FilterOperatorEnum.ISNULL);
            Assert.assertEquals(map(colA, null), f.findMustEqualColsAndValues(cols));
        }
        
        {
            TupleFilter f = and(compare(colA, FilterOperatorEnum.ISNULL), compare(colB, FilterOperatorEnum.EQ, "1234"));
            Assert.assertEquals(map(colA, null, colB, "1234"), f.findMustEqualColsAndValues(cols));
            Assert.assertTrue(not(f).findMustEqualColsAndValues(cols).isEmpty());
        }
        
        {
            TupleFilter f = compare(colA, FilterOperatorEnum.LT, "1234");
            Assert.assertTrue(f.findMustEqualColsAndValues(cols).isEmpty());
        }
    }

    private HashMap<TblColRef, Object> map(TblColRef col, String v) {
        HashMap<TblColRef, Object> r = new HashMap<>();
        r.put(col, v);
        return r;
    }
    
    private HashMap<TblColRef, Object> map(TblColRef col, String v, TblColRef col2, String v2) {
        HashMap<TblColRef, Object> r = new HashMap<>();
        r.put(col, v);
        r.put(col2, v2);
        return r;
    }

    @Test
    public void testMustTrueTupleFilter() {
        TupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        TupleFilter andFilter2  = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        TupleFilter orFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        andFilter.addChild(andFilter2);
        andFilter.addChild(orFilter);

        Set<CompareTupleFilter> trueTupleFilters = andFilter.findMustTrueCompareFilters();
        Assert.assertTrue(trueTupleFilters.isEmpty());

        TupleFilter compFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        compFilter.addChild(new ColumnTupleFilter(TblColRef.newInnerColumn("test1", TblColRef.InnerDataTypeEnum.LITERAL)));
        TupleFilter compFilter2 = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        compFilter2.addChild(new ColumnTupleFilter(TblColRef.newInnerColumn("test2", TblColRef.InnerDataTypeEnum.LITERAL)));
        andFilter2.addChild(compFilter);
        orFilter.addChild(compFilter2);
        Assert.assertEquals(Sets.newHashSet(compFilter), andFilter.findMustTrueCompareFilters());
        
        Assert.assertEquals(Sets.newHashSet(compFilter2), compFilter2.findMustTrueCompareFilters());
    }

    @Test
    public void flatFilterTest() {
        TupleFilter topAndFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        topAndFilter.addChild(createEQFilter("c1", "v1"));
        TupleFilter orFilter1 = new LogicalTupleFilter(FilterOperatorEnum.OR);
        TupleFilter andFilter11 = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter11.addChild(createEQFilter("c2", "v2"));
        andFilter11.addChild(createEQFilter("c3", "v3"));

        TupleFilter andFilter12 = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter12.addChild(createEQFilter("c2", "v21"));
        andFilter12.addChild(createEQFilter("c3", "v31"));

        orFilter1.addChild(andFilter11);
        orFilter1.addChild(andFilter12);

        TupleFilter orFilter2 = new LogicalTupleFilter(FilterOperatorEnum.OR);
        TupleFilter andFilter21 = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter21.addChild(createEQFilter("c4", "v4"));
        andFilter21.addChild(createEQFilter("c5", "v5"));

        TupleFilter andFilter22 = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter22.addChild(createEQFilter("c4", "v41"));
        andFilter22.addChild(createEQFilter("c5", "v51"));

        TupleFilter andFilter23 = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter23.addChild(createEQFilter("c4", "v42"));
        andFilter23.addChild(createEQFilter("c5", "v52"));

        orFilter2.addChild(andFilter21);
        orFilter2.addChild(andFilter22);
        orFilter2.addChild(andFilter23);

        topAndFilter.addChild(orFilter1);
        topAndFilter.addChild(orFilter2);

        TupleFilter flatFilter = topAndFilter.flatFilter(500000);
        Assert.assertEquals(6, flatFilter.children.size());
    }

    @Test(expected = IllegalStateException.class)
    public void flatFilterTooFatTest() {
        TupleFilter topAndFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        for (int i = 0; i < 3; i++) {
            TupleFilter orFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
            String col = "col-" + i;
            for (int j = 0; j < 100; j++) {
                orFilter.addChild(createEQFilter(col, String.valueOf(j)));
            }
            topAndFilter.addChild(orFilter);
        }
        TupleFilter flatFilter = topAndFilter.flatFilter(500000);
        System.out.println(flatFilter);
    }

    private TupleFilter createEQFilter(String colName, String colVal) {
        CompareTupleFilter compareTupleFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        compareTupleFilter
                .addChild(new ColumnTupleFilter(TblColRef.newInnerColumn(colName, InnerDataTypeEnum.LITERAL)));
        compareTupleFilter.addChild(new ConstantTupleFilter(colVal));
        return compareTupleFilter;
    }
    
}
