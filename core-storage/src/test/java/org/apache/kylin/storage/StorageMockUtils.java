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
package org.apache.kylin.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.TupleInfo;

/**
 */
public class StorageMockUtils {
    
    final DataModelDesc model;
    
    public StorageMockUtils(DataModelDesc model) {
        this.model = model;
    }
    
    public TupleInfo newTupleInfo(List<TblColRef> groups, List<FunctionDesc> aggregations) {
        TupleInfo info = new TupleInfo();
        int idx = 0;

        for (TblColRef col : groups) {
            info.setField(col.getName(), col, idx++);
        }

        TableRef sourceTable = groups.get(0).getTableRef();
        for (FunctionDesc func : aggregations) {
            ColumnDesc colDesc = func.newFakeRewriteColumn(sourceTable.getTableDesc());
            TblColRef col = sourceTable.makeFakeColumn(colDesc);
            info.setField(col.getName(), col, idx++);
        }

        return info;
    }

    public List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TblColRef c1 = model.findColumn("DEFAULT.TEST_KYLIN_FACT.CAL_DT");
        groups.add(c1);

        TblColRef c2 = model.findColumn("DEFAULT.TEST_CATEGORY_GROUPINGS.META_CATEG_NAME");
        groups.add(c2);

        return groups;
    }

    public List<FunctionDesc> buildAggregations1() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TblColRef priceCol = model.findColumn("DEFAULT.TEST_KYLIN_FACTPRICE");

        FunctionDesc f1 = FunctionDesc.newInstance("SUM", //
                ParameterDesc.newInstance(priceCol), "decimal(19,4)");
        functions.add(f1);

        return functions;
    }

    public List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TblColRef priceCol = model.findColumn("DEFAULT.TEST_KYLIN_FACT.PRICE");
        TblColRef sellerCol = model.findColumn("DEFAULT.TEST_KYLIN_FACT.SELLER_ID");

        FunctionDesc f1 = FunctionDesc.newInstance("SUM", //
                ParameterDesc.newInstance(priceCol), "decimal(19,4)");
        functions.add(f1);

        FunctionDesc f2 = FunctionDesc.newInstance("COUNT_DISTINCT", //
                ParameterDesc.newInstance(sellerCol), "hllc(10)");
        functions.add(f2);

        return functions;
    }

    public CompareTupleFilter buildTs2010Filter(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2010-01-01");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public CompareTupleFilter buildTs2011Filter(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2011-01-01");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public CompareTupleFilter buildFilter1(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2012-05-23");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public CompareTupleFilter buildFilter2(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("ClothinShoes & Accessories");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    public CompareTupleFilter buildFilter3(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2012-05-23");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    public TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter logicFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(compareFilter2);
        return logicFilter;
    }
}
