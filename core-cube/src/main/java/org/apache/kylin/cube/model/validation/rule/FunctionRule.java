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

package org.apache.kylin.cube.model.validation.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

import com.google.common.collect.Lists;

/**
 * Validate function parameter.
 * <p/>
 * if type is column, check values are valid fact table columns if type is
 * constant, the value only can be numberic
 * <p/>
 * the return type only can be int/bigint/long/double/decimal
 *
 */
public class FunctionRule implements IValidatorRule<CubeDesc> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.kylin.metadata.validation.IValidatorRule#validate(java.lang.Object
     * , org.apache.kylin.metadata.validation.ValidateContext)
     */
    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        List<MeasureDesc> measures = cube.getMeasures();

        if (validateMeasureNamesDuplicated(measures, context)) {
            return;
        }

        List<FunctionDesc> countFuncs = new ArrayList<FunctionDesc>();

        Iterator<MeasureDesc> it = measures.iterator();
        while (it.hasNext()) {
            MeasureDesc measure = it.next();
            FunctionDesc func = measure.getFunction();
            ParameterDesc parameter = func.getParameter();
            if (parameter == null) {
                context.addResult(ResultLevel.ERROR, "Must define parameter for function " + func.getExpression() + " in " + measure.getName());
                return;
            }

            String type = func.getParameter().getType();
            String value = func.getParameter().getValue();
            if (StringUtils.isEmpty(type)) {
                context.addResult(ResultLevel.ERROR, "Must define type for parameter type " + func.getExpression() + " in " + measure.getName());
                return;
            }
            if (StringUtils.isEmpty(value)) {
                context.addResult(ResultLevel.ERROR, "Must define type for parameter value " + func.getExpression() + " in " + measure.getName());
                return;
            }
            if (StringUtils.isEmpty(func.getReturnType())) {
                context.addResult(ResultLevel.ERROR, "Must define return type for function " + func.getExpression() + " in " + measure.getName());
                return;
            }

            if (StringUtils.equalsIgnoreCase(FunctionDesc.PARAMETER_TYPE_COLUMN, type)) {
                validateColumnParameter(context, cube, value);
            } else if (StringUtils.equals(FunctionDesc.PARAMETER_TYPE_CONSTANT, type)) {
                validateCostantParameter(context, cube, value);
            }

            try {
                func.getMeasureType().validate(func);
            } catch (IllegalArgumentException ex) {
                context.addResult(ResultLevel.ERROR, ex.getMessage());
            }

            if (func.isCount())
                countFuncs.add(func);

            if (TopNMeasureType.FUNC_TOP_N.equalsIgnoreCase(func.getExpression())) {
                if (parameter.getNextParameter() == null) {
                    context.addResult(ResultLevel.ERROR, "Must define at least 2 parameters for function " + func.getExpression() + " in " + measure.getName());
                    return;
                }

                ParameterDesc groupByCol = parameter.getNextParameter();
                List<String> duplicatedCol = Lists.newArrayList();
                while (groupByCol != null) {
                    String embeded_groupby = groupByCol.getValue();
                    for (DimensionDesc dimensionDesc : cube.getDimensions()) {
                        if (dimensionDesc.getColumn() != null && dimensionDesc.getColumn().equalsIgnoreCase(embeded_groupby)) {
                            duplicatedCol.add(embeded_groupby);
                        }
                    }
                    groupByCol = groupByCol.getNextParameter();
                }

            }
        }

        if (countFuncs.size() != 1) {
            context.addResult(ResultLevel.ERROR, "Must define one and only one count(1) function, but there are " + countFuncs.size() + " -- " + countFuncs);
        }
    }

    /**
     * @param context
     * @param cube
     * @param value
     */
    private void validateCostantParameter(ValidateContext context, CubeDesc cube, String value) {
        try {
            Integer.parseInt(value);
        } catch (Exception e) {
            context.addResult(ResultLevel.ERROR, "Parameter value must be number, but it is " + value);
        }
    }

    /**
     * @param context
     * @param cube
     * @param value
     */
    private void validateColumnParameter(ValidateContext context, CubeDesc cube, String value) {
        DataModelDesc model = cube.getModel();
        try {
            model.findColumn(value);
        } catch (IllegalArgumentException e) {
            context.addResult(ResultLevel.ERROR, e.getMessage());
        }
    }

    /**
     * @param measures
     */
    private boolean validateMeasureNamesDuplicated(List<MeasureDesc> measures, ValidateContext context) {
        Set<String> nameSet = new HashSet<>();
        for (MeasureDesc measure: measures){
            if (nameSet.contains(measure.getName())){
                context.addResult(ResultLevel.ERROR, "There is duplicated measure's name: " + measure.getName());
                return true;
            } else {
                nameSet.add(measure.getName());
            }
        }
        return false;
    }
}
