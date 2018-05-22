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
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

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
}
