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

import java.util.Map;
import java.util.Set;

public abstract class DynamicFunctionDesc extends FunctionDesc {

    public DynamicFunctionDesc(ParameterDesc parameter, String expression, String returnType) {
        this.setParameter(parameter);
        this.setExpression(expression);
        this.setReturnType(returnType);
    }

    @Override
    public boolean needRewriteField() {
        return false;
    }

    // TODO: this should be referred by the filters in tupleExpression
    public boolean ifFriendlyForDerivedFilter() {
        return false;
    }

    public abstract Set<TblColRef> getRuntimeDimensions();

    public abstract Map<TblColRef, FunctionDesc> getRuntimeFuncMap();

    public abstract void setRuntimeFuncMap(Map<TblColRef, FunctionDesc> funcMap);

    protected abstract FunctionDesc constructRuntimeFunction(TblColRef column);
}