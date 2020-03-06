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

package org.apache.kylin.metadata.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.measure.stddev.StdDevSumMeasureType;
import org.apache.kylin.metadata.datatype.DataType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StdDevSumFunctionDesc extends TwoLayerFunctionDesc {

    protected TblColRef groupByDim;
    protected TblColRef mCol;
    protected Set<TblColRef> runtimeDims;
    protected Map<TblColRef, FunctionDesc> runtimeFuncMap;

    public StdDevSumFunctionDesc(ParameterDesc parameter, List<TblColRef> allCols) {
        super(parameter, StdDevSumMeasureType.FUNC_STDDEV_SUM, StdDevSumMeasureType.DATATYPE_STDDEV);

        assert allCols.size() == 2;
        groupByDim = allCols.get(0);
        runtimeDims = Sets.newHashSet(groupByDim);

        mCol = allCols.get(1);
        runtimeFuncMap = Maps.newHashMap();
        runtimeFuncMap.put(mCol, constructRuntimeFunction(mCol));
    }

    @Override
    public String getRewriteFieldName() {
        return "_KY_" + FUNC_SUM + "_" + getDigest(groupByDim, mCol);
    }

    @Override
    public DataType getRewriteFieldType() {
        return DataType.ANY;
    }

    @Override
    public Set<TblColRef> getRuntimeDimensions() {
        return runtimeDims;
    }

    @Override
    public Map<TblColRef, FunctionDesc> getRuntimeFuncMap() {
        return runtimeFuncMap;
    }

    @Override
    public void setRuntimeFuncMap(Map<TblColRef, FunctionDesc> funcMap) {
        assert funcMap.size() == 1;
        this.runtimeFuncMap = funcMap;
        this.mCol = funcMap.keySet().iterator().next();
    }

    @Override
    protected FunctionDesc constructRuntimeFunction(TblColRef column) {
        return FunctionDesc.newInstance(FUNC_SUM, ParameterDesc.newInstance(column), null);
    }

    public static String getDigest(TblColRef dim, TblColRef measure) {
        return "GROUP_BY_" + dim.getIdentity() + "_SUM_(" + measure.getIdentity() + ")";
    }
}