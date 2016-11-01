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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Test;

/**
 * @author xjiang
 * 
 */
public class FilterSerializeTest extends FilterBaseTest {

    private void assertFilterSerDe(TupleFilter filter) {
        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        compareFilter(filter, newFilter);
    }

    @Test
    public void testSerialize01() {
        assertFilterSerDe(buildEQCompareFilter(buildGroups(), 0));
    }

    @Test
    public void testSerialize02() {
        assertFilterSerDe(buildEQCompareFilter(buildGroups(), 1));
    }

    @Test
    public void testSerialize03() {
        assertFilterSerDe(buildAndFilter(buildGroups()));
    }

    @Test
    public void testSerialize04() {
        assertFilterSerDe(buildOrFilter(buildGroups()));
    }

    @Test
    public void testSerialize05() {
        ColumnDesc column = new ColumnDesc();
        TblColRef colRef = column.getRef();
        List<TblColRef> groups = new ArrayList<TblColRef>();
        groups.add(colRef);

        assertFilterSerDe(buildEQCompareFilter(groups, 0));
    }

    @Test
    public void testSerialize06() {
        ColumnDesc column = new ColumnDesc();
        column.setName("META_CATEG_NAME");
        TblColRef colRef = column.getRef();
        List<TblColRef> groups = new ArrayList<TblColRef>();
        groups.add(colRef);

        assertFilterSerDe(buildEQCompareFilter(groups, 0));
    }

    @Test
    public void testSerialize07() {
        TableDesc table = new TableDesc();
        table.setName("TEST_KYLIN_FACT");
        table.setDatabase("DEFAULT");

        ColumnDesc column = new ColumnDesc();
        column.setTable(table);
        TblColRef colRef = column.getRef();
        List<TblColRef> groups = new ArrayList<TblColRef>();
        groups.add(colRef);

        assertFilterSerDe(buildEQCompareFilter(groups, 0));
    }

    @Test
    public void testSerialize08() {
        TableDesc table = new TableDesc();
        table.setDatabase("DEFAULT");

        ColumnDesc column = new ColumnDesc();
        column.setTable(table);
        TblColRef colRef = column.getRef();
        List<TblColRef> groups = new ArrayList<TblColRef>();
        groups.add(colRef);

        assertFilterSerDe(buildEQCompareFilter(groups, 0));
    }

    @Test
    public void testSerialize10() {
        List<TblColRef> groups = buildGroups();
        TupleFilter orFilter = buildOrFilter(groups);
        TupleFilter andFilter = buildAndFilter(groups);

        LogicalTupleFilter logicFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
        logicFilter.addChild(orFilter);
        logicFilter.addChild(andFilter);

        assertFilterSerDe(logicFilter);
    }

    @Test
    public void testSerialize11() {
        List<TblColRef> groups = buildGroups();
        TupleFilter orFilter = buildOrFilter(groups);
        TupleFilter andFilter = buildAndFilter(groups);

        LogicalTupleFilter logicFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
        logicFilter.addChild(orFilter);
        logicFilter.addChild(andFilter);

        assertFilterSerDe(logicFilter);
    }

    @Test
    public void testSerialize12() {
        assertFilterSerDe(buildCaseFilter(buildGroups()));
    }

    @Test
    public void testSerialize13() {
        assertFilterSerDe(buildCompareCaseFilter(buildGroups(), "0"));
    }

    @Test
    public void testSerialize14() throws ParseException {
        assertFilterSerDe(buildINCompareFilter(buildGroups().get(0)));
    }

    @Test
    public void testDynamic() {
        assertFilterSerDe(buildCompareDynamicFilter(buildGroups(), FilterOperatorEnum.EQ));
        assertFilterSerDe(buildCompareDynamicFilter(buildGroups(), FilterOperatorEnum.NEQ));
        assertFilterSerDe(buildCompareDynamicFilter(buildGroups(), FilterOperatorEnum.GT));
        assertFilterSerDe(buildCompareDynamicFilter(buildGroups(), FilterOperatorEnum.LT));
        assertFilterSerDe(buildCompareDynamicFilter(buildGroups(), FilterOperatorEnum.GTE));
        assertFilterSerDe(buildCompareDynamicFilter(buildGroups(), FilterOperatorEnum.LTE));
    }

}
