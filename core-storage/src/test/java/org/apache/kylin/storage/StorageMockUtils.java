/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package org.apache.kylin.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.TupleInfo;

import com.google.common.collect.ImmutableList;

/**
 */
public class StorageMockUtils {
    public static TupleInfo newTupleInfo(List<TblColRef> groups, List<FunctionDesc> aggregations) {
        TupleInfo info = new TupleInfo();
        int idx = 0;

        for (TblColRef col : groups) {
            info.setField(col.getName(), col, idx++);
        }

        TableDesc sourceTable = groups.get(0).getColumnDesc().getTable();
        for (FunctionDesc func : aggregations) {
            TblColRef col = func.newFakeRewriteColumn(sourceTable).getRef();
            info.setField(col.getName(), col, idx++);
        }

        return info;
    }

    public static List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc c1 = ColumnDesc.mockup(t1, 2, "CAL_DT", "date");
        TblColRef cf1 = c1.getRef();
        groups.add(cf1);

        TableDesc t2 = TableDesc.mockup("DEFAULT.TEST_CATEGORY_GROUPINGS");
        ColumnDesc c2 = ColumnDesc.mockup(t2, 14, "META_CATEG_NAME", "string");
        TblColRef cf2 = c2.getRef();
        groups.add(cf2);

        return groups;
    }

    public static List<FunctionDesc> buildAggregations1() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        TblColRef priceCol = ColumnDesc.mockup(t1, 7, "PRICE", "decimal(19,4)").getRef();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        p1.setColRefs(ImmutableList.of(priceCol));
        f1.setParameter(p1);
        f1.setReturnType("decimal(19,4)");
        functions.add(f1);

        return functions;
    }

    public static List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        TblColRef priceCol = ColumnDesc.mockup(t1, 7, "PRICE", "decimal(19,4)").getRef();
        TblColRef sellerCol = ColumnDesc.mockup(t1, 9, "SELLER_ID", "bigint").getRef();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        p1.setColRefs(ImmutableList.of(priceCol));
        f1.setParameter(p1);
        f1.setReturnType("decimal(19,4)");
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("COUNT_DISTINCT");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("SELLER_ID");
        p2.setColRefs(ImmutableList.of(sellerCol));
        f2.setParameter(p2);
        f2.setReturnType("hllc(10)");
        functions.add(f2);

        return functions;
    }

    public static CompareTupleFilter buildTs2010Filter(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2010-01-01");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static CompareTupleFilter buildTs2011Filter(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2011-01-01");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static CompareTupleFilter buildFilter1(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2012-05-23");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static CompareTupleFilter buildFilter2(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("ClothinShoes & Accessories");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    public static CompareTupleFilter buildFilter3(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2012-05-23");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    public static TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter logicFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(compareFilter2);
        return logicFilter;
    }
}
