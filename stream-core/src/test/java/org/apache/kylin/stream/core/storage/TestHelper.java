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

package org.apache.kylin.stream.core.storage;

import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class TestHelper {
    private CubeInstance cubeInstance;

    public TestHelper(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
    }

    public Set<TblColRef> simulateDimensions(String... dimName) {
        Set<TblColRef> dimensions = Sets.newHashSet();
        for (String dim : dimName) {
            TblColRef cf1 = getColumnRef(dim);
            dimensions.add(cf1);
        }

        return dimensions;
    }

    public Set<FunctionDesc> simulateMetrics() {
        List<FunctionDesc> functions = Lists.newArrayList();
        TblColRef gmvCol = getColumnRef("STREAMING_V2_TABLE.GMV");

//        FunctionDesc f1 = new FunctionDesc();
//        f1.setExpression("SUM");
//        ParameterDesc p1 = ParameterDesc.newInstance(gmvCol);
//        f1.setParameter(p1);
//        f1.setReturnType("decimal(19,6)");
//        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression(PercentileMeasureType.FUNC_PERCENTILE_APPROX);
        ParameterDesc p2 = ParameterDesc.newInstance(gmvCol);
        f2.setParameter(p2);
        f2.setReturnType("percentile(100)");
        functions.add(f2);

        return Sets.newHashSet(functions);
    }

    public FunctionDesc simulateMetric(String columnName, String funcName, String returnType) {
        TblColRef gmvCol = getColumnRef(columnName);

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression(funcName);
        ParameterDesc p1 = ParameterDesc.newInstance(gmvCol);
        f1.setParameter(p1);
        f1.setReturnType(returnType);
        return f1;
    }

    public FunctionDesc simulateCountMetric() {
        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("COUNT");
        ParameterDesc p1 = ParameterDesc.newInstance("1");
        f1.setParameter(p1);
        f1.setReturnType("bigint");
        return f1;
    }

    public TblColRef getColumnRef(String col) {
        return cubeInstance.getModel().findColumn(col);
    }

    public CompareTupleFilter buildEQFilter(String columnName, String value) {
        return buildCompareFilter(columnName, FilterOperatorEnum.EQ, value);
    }

    public TupleFilter buildLikeFilter(String columnName, String value) {
        BuiltInFunctionTupleFilter likeFilter = new BuiltInFunctionTupleFilter("like");
        likeFilter.addChild(buildColumnFilter(columnName));
        likeFilter.addChild(new ConstantTupleFilter(value));
        return likeFilter;
    }

    public TupleFilter buildLowerFilter(String columnName, FilterOperatorEnum op, String value) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(op);
        ColumnTupleFilter columnFilter1 = buildColumnFilter(columnName);
        BuiltInFunctionTupleFilter lowerFilter = new BuiltInFunctionTupleFilter("lower");
        lowerFilter.addChild(columnFilter1);
        compareFilter.addChild(lowerFilter);
        compareFilter.addChild(new ConstantTupleFilter(value));
        return compareFilter;
    }

    public CompareTupleFilter buildCompareFilter(String columnName, FilterOperatorEnum op, Object value) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(op);
        ColumnTupleFilter columnFilter1 = buildColumnFilter(columnName);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter(value);
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public ColumnTupleFilter buildColumnFilter(String columnName) {
        TblColRef column = getColumnRef(columnName);
        return new ColumnTupleFilter(column);
    }

    public TupleFilter buildAndFilter(TupleFilter filter1, TupleFilter filter2) {
        return buildLogicalFilter(filter1, filter2, FilterOperatorEnum.AND);
    }

    public TupleFilter buildOrFilter(TupleFilter filter1, TupleFilter filter2) {
        return buildLogicalFilter(filter1, filter2, FilterOperatorEnum.OR);
    }

    public TupleFilter buildLogicalFilter(TupleFilter filter1, TupleFilter filter2, FilterOperatorEnum op) {
        LogicalTupleFilter andFilter = new LogicalTupleFilter(op);
        andFilter.addChild(filter1);
        andFilter.addChild(filter2);
        return andFilter;
    }

    public TupleFilter buildTimeRangeFilter(String timeColumn, Object startTime, Object endTime) {
        CompareTupleFilter leftFilter = buildCompareFilter(timeColumn, FilterOperatorEnum.GTE, startTime);
        CompareTupleFilter rightFilter = buildCompareFilter(timeColumn, FilterOperatorEnum.LT, endTime);
        return buildAndFilter(leftFilter, rightFilter);
    }

}
