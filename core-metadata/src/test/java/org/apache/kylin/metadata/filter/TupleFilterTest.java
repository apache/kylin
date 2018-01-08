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

import org.junit.Assert;
import org.junit.Test;

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
}
