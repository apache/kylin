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

package org.apache.kylin.measure.corr;

import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_SUM;
import static org.apache.kylin.metadata.model.FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class CorrMeasureType extends MeasureType {

    public static final String FUNC_CORR = "CORR";

    public static final String DATATYPE_CORR = "corr";

    public static class Factory extends MeasureTypeFactory {

        @Override
        public MeasureType createMeasureType(String funcName, DataType dataType) {
            return new CorrMeasureType();
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_CORR;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_CORR;
        }

        @Override
        public Class<? extends DataTypeSerializer> getAggrDataTypeSerializer() {
            return null;
        }
    }

    @Override
    public MeasureIngester newIngester() {
        return null;
    }

    @Override
    public MeasureAggregator newAggregator() {
        return null;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    private static final Map<String, Class<?>> UDAF_MAP = ImmutableMap.of(FUNC_CORR, CorrAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public boolean expandable() {
        return true;
    }

    @Override
    public List<FunctionDesc> convertToInternalFunctionDesc(FunctionDesc functionDesc) {
        List<ParameterDesc> parameterDescList = Lists.newArrayList();
        for (ParameterDesc parameter : functionDesc.getParameters()) {
            parameter.getColRef();
            parameterDescList.add(parameter);
        }

        List<FunctionDesc> functionDescs = Lists.newArrayList();

        //convert CORR to SUM（X*X), SUM（X*Y), SUM（Y*Y)
        List<ParameterDesc> descs = Lists.newArrayList();
        for (int i = 0; i < parameterDescList.size(); i++) {
            for (int j = i; j < parameterDescList.size(); j++) {
                ParameterDesc newParam = new ParameterDesc();
                newParam.setType(PARAMETER_TYPE_MATH_EXPRESSION);
                newParam.setValue(String.format(Locale.ROOT, "%s * %s", parameterDescList.get(i).getValue(),
                        parameterDescList.get(j).getValue()));
                descs.add(newParam);
            }
        }
        parameterDescList.addAll(descs);

        for (ParameterDesc param : parameterDescList) {
            FunctionDesc function = FunctionDesc.newInstance(FUNC_SUM, Lists.newArrayList(param), null);
            functionDescs.add(function);
        }
        return functionDescs;
    }
}
