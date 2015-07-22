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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;

/**
 * Validate function parameter. Ticket:
 * https://github.scm.corp.ebay.com/Kylin/Kylin/issues/268
 * <p/>
 * if type is column, check values are valid fact table columns if type is
 * constant, the value only can be numberic
 * <p/>
 * the return type only can be int/bigint/long/double/decimal
 *
 * @author jianliu
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
            } else if (StringUtils.equals(FunctionDesc.PARAMTER_TYPE_CONSTANT, type)) {
                validateCostantParameter(context, cube, value);
            }
            validateReturnType(context, cube, func);

            if (func.isCount())
                countFuncs.add(func);
        }

        if (countFuncs.size() != 1) {
            context.addResult(ResultLevel.ERROR, "Must define one and only one count(1) function, but there are " + countFuncs.size() + " -- " + countFuncs);
        }
    }

    private void validateReturnType(ValidateContext context, CubeDesc cube, FunctionDesc funcDesc) {

        String func = funcDesc.getExpression();
        DataType rtype = funcDesc.getReturnDataType();

        if (funcDesc.isCount()) {
            if (rtype.isIntegerFamily() == false) {
                context.addResult(ResultLevel.ERROR, "Return type for function " + func + " must be one of " + DataType.INTEGER_FAMILY);
            }
        } else if (funcDesc.isCountDistinct()) {
            if (rtype.isHLLC() == false && funcDesc.isHolisticCountDistinct() == false) {
                context.addResult(ResultLevel.ERROR, "Return type for function " + func + " must be hllc(10), hllc(12) etc.");
            }
        } else if (funcDesc.isMax() || funcDesc.isMin() || funcDesc.isSum()) {
            if (rtype.isNumberFamily() == false) {
                context.addResult(ResultLevel.ERROR, "Return type for function " + func + " must be one of " + DataType.NUMBER_FAMILY);
            }
        } else {
            if (StringUtils.equalsIgnoreCase(KylinConfig.getInstanceFromEnv().getProperty(KEY_IGNORE_UNKNOWN_FUNC, "false"), "false")) {
                context.addResult(ResultLevel.ERROR, "Unrecognized function: [" + func + "]");
            }
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
        String factTable = cube.getFactTable();
        if (StringUtils.isEmpty(factTable)) {
            context.addResult(ResultLevel.ERROR, "Fact table can not be null.");
            return;
        }
        TableDesc table = MetadataManager.getInstance(cube.getConfig()).getTableDesc(factTable);
        if (table == null) {
            context.addResult(ResultLevel.ERROR, "Fact table can not be found: " + cube);
            return;
        }
        // Prepare column set
        Set<String> set = new HashSet<String>();
        ColumnDesc[] cdesc = table.getColumns();
        for (int i = 0; i < cdesc.length; i++) {
            ColumnDesc columnDesc = cdesc[i];
            set.add(columnDesc.getName());
        }

        String[] items = value.split(",");
        for (int i = 0; i < items.length; i++) {
            String item = items[i].trim();
            if (StringUtils.isEmpty(item)) {
                continue;
            }
            if (!set.contains(item)) {
                context.addResult(ResultLevel.ERROR, "Column [" + item + "] does not exist in factable table" + factTable);
            }
        }

    }
}
