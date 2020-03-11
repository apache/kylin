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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.expression.ExpressionColCollector;
import org.apache.kylin.metadata.expression.TupleExpression;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public abstract class DynamicFunctionDesc extends FunctionDesc {

    protected final TupleExpression tupleExpression;
    protected final Set<TblColRef> filterColSet;
    protected Map<TblColRef, FunctionDesc> runtimeFuncMap;

    public DynamicFunctionDesc(ParameterDesc parameter, TupleExpression tupleExpression) {
        this.setParameter(parameter);
        this.tupleExpression = tupleExpression;

        Pair<Set<TblColRef>, Set<TblColRef>> colsPair = ExpressionColCollector.collectColumnsPair(tupleExpression);
        filterColSet = colsPair.getFirst();
        Set<TblColRef> measureColumns = colsPair.getSecond();
        this.runtimeFuncMap = Maps.newHashMapWithExpectedSize(measureColumns.size());
        for (TblColRef column : measureColumns) {
            runtimeFuncMap.put(column, constructRuntimeFunction(column));
        }
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    // TODO: this should be referred by the filters in tupleExpression
    public boolean ifFriendlyForDerivedFilter() {
        return false;
    }

    public TupleExpression getTupleExpression() {
        return tupleExpression;
    }

    public Set<TblColRef> getFilterColumnSet() {
        return filterColSet;
    }

    public Set<TblColRef> getMeasureColumnSet() {
        return runtimeFuncMap.keySet();
    }

    public Collection<FunctionDesc> getRuntimeFuncs() {
        return runtimeFuncMap.values();
    }

    public Map<TblColRef, FunctionDesc> getRuntimeFuncMap() {
        return runtimeFuncMap;
    }

    public void setRuntimeFuncMap(Map<TblColRef, FunctionDesc> funcMap) {
        this.runtimeFuncMap = funcMap;
    }

    protected abstract FunctionDesc constructRuntimeFunction(TblColRef column);
}
