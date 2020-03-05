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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.expression.ExpressionColCollector;
import org.apache.kylin.metadata.expression.TupleExpression;

import com.google.common.collect.Maps;

public abstract class ExpressionDynamicFunctionDesc extends DynamicFunctionDesc {

    protected TupleExpression tupleExpression;
    protected Set<TblColRef> filterColSet;
    protected Map<TblColRef, FunctionDesc> runtimeFuncMap;

    public ExpressionDynamicFunctionDesc(ParameterDesc parameter, String expression, String returnType,
            TupleExpression tupleExpression) {
        super(parameter, expression, returnType);
        this.tupleExpression = tupleExpression;

        Pair<Set<TblColRef>, Set<TblColRef>> colsPair = ExpressionColCollector.collectColumnsPair(tupleExpression);
        filterColSet = colsPair.getFirst();
        Set<TblColRef> measureColumns = colsPair.getSecond();
        this.runtimeFuncMap = Maps.newHashMapWithExpectedSize(measureColumns.size());
        for (TblColRef column : measureColumns) {
            runtimeFuncMap.put(column, constructRuntimeFunction(column));
        }
    }

    public TupleExpression getTupleExpression() {
        return tupleExpression;
    }

    @Override
    public Set<TblColRef> getRuntimeDimensions() {
        return filterColSet;
    }

    public Set<TblColRef> getRuntimeMeasures() {
        return Collections.unmodifiableSet(runtimeFuncMap.keySet());
    }

    @Override
    public Map<TblColRef, FunctionDesc> getRuntimeFuncMap() {
        return runtimeFuncMap;
    }

    @Override
    public void setRuntimeFuncMap(Map<TblColRef, FunctionDesc> funcMap) {
        this.runtimeFuncMap = funcMap;
        resetReturnType();
    }

    protected abstract void resetReturnType();
}