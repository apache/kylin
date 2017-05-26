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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class FunctionDesc implements Serializable {

    public static FunctionDesc newInstance(String expression, ParameterDesc param, String returnType) {
        FunctionDesc r = new FunctionDesc();
        r.expression = (expression == null) ? null : expression.toUpperCase();
        r.parameter = param;
        r.returnType = returnType;
        r.returnDataType = DataType.getType(returnType);
        return r;
    }

    public static final String FUNC_SUM = "SUM";
    public static final String FUNC_MIN = "MIN";
    public static final String FUNC_MAX = "MAX";
    public static final String FUNC_COUNT = "COUNT";
    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";
    public static final Set<String> BUILT_IN_AGGREGATIONS = Sets.newHashSet();

    static {
        BUILT_IN_AGGREGATIONS.add(FUNC_COUNT);
        BUILT_IN_AGGREGATIONS.add(FUNC_MAX);
        BUILT_IN_AGGREGATIONS.add(FUNC_MIN);
        BUILT_IN_AGGREGATIONS.add(FUNC_SUM);
        BUILT_IN_AGGREGATIONS.add(FUNC_COUNT_DISTINCT);
    }

    public static final String PARAMETER_TYPE_CONSTANT = "constant";
    public static final String PARAMETER_TYPE_COLUMN = "column";

    @JsonProperty("expression")
    private String expression;
    @JsonProperty("parameter")
    private ParameterDesc parameter;
    @JsonProperty("returntype")
    private String returnType;

    @JsonProperty("configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> configuration = new LinkedHashMap<String, String>();

    private DataType returnDataType;
    private MeasureType<?> measureType;
    private boolean isDimensionAsMetric = false;

    public void init(DataModelDesc model) {
        expression = expression.toUpperCase();
        returnDataType = DataType.getType(returnType);

        for (ParameterDesc p = parameter; p != null; p = p.getNextParameter()) {
            if (p.isColumnType()) {
                TblColRef colRef = model.findColumn(p.getValue());
                p.setValue(colRef.getIdentity());
                p.setColRef(colRef);
            }
        }
    }

    private void reInitMeasureType() {
        if (isDimensionAsMetric && isCountDistinct()) {
            // create DimCountDis
            measureType = MeasureTypeFactory.createNoRewriteFieldsMeasureType(getExpression(), getReturnDataType());
        } else {
            measureType = MeasureTypeFactory.create(getExpression(), getReturnDataType());
        }
    }

    public MeasureType<?> getMeasureType() {
        //like max(cal_dt)
        if (isDimensionAsMetric && !isCountDistinct()) {
            return null;
        }

        if (measureType == null) {
            reInitMeasureType();
        }

        return measureType;
    }

    public boolean needRewrite() {
        if (getMeasureType() == null)
            return false;

        return getMeasureType().needRewrite();
    }

    public boolean needRewriteField() {
        if (!needRewrite())
            return false;

        return getMeasureType().needRewriteField();
    }

    public String getRewriteFieldName() {
        if (isSum()) {
            return getParameter().getValue();
        } else if (isCount()) {
            return "_KY_" + "COUNT__"; // ignores parameter, count(*), count(1), count(col) are all the same
        } else if (isCountDistinct()) {
            return "_KY_" + getFullExpressionInAlphabetOrder().replaceAll("[(),. ]", "_");
        } else {
            return "_KY_" + getFullExpression().replaceAll("[(),. ]", "_");
        }
    }

    public DataType getRewriteFieldType() {
        if (isSum() || isMax() || isMin())
            return parameter.getColRefs().get(0).getType();
        else if (getMeasureType() instanceof BasicMeasureType)
            return returnDataType;
        else
            return DataType.ANY;
    }

    public ColumnDesc newFakeRewriteColumn(TableDesc sourceTable) {
        ColumnDesc fakeCol = new ColumnDesc();
        fakeCol.setName(getRewriteFieldName());
        fakeCol.setDatatype(getRewriteFieldType().toString());
        if (isCount())
            fakeCol.setNullable(false);
        fakeCol.init(sourceTable);
        return fakeCol;
    }

    public boolean isMin() {
        return FUNC_MIN.equalsIgnoreCase(expression);
    }

    public boolean isMax() {
        return FUNC_MAX.equalsIgnoreCase(expression);
    }

    public boolean isSum() {
        return FUNC_SUM.equalsIgnoreCase(expression);
    }

    public boolean isCount() {
        return FUNC_COUNT.equalsIgnoreCase(expression);
    }

    public boolean isCountDistinct() {
        return FUNC_COUNT_DISTINCT.equalsIgnoreCase(expression);
    }

    /**
     * Get Full Expression such as sum(amount), count(1), count(*)...
     */
    public String getFullExpression() {
        StringBuilder sb = new StringBuilder(expression);
        sb.append("(");
        if (parameter != null) {
            sb.append(parameter.getValue());
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Parameters' name appears in alphabet order.
     * This method is used for funcs whose parameters appear in arbitrary order
     */
    public String getFullExpressionInAlphabetOrder() {
        StringBuilder sb = new StringBuilder(expression);
        sb.append("(");
        ParameterDesc localParam = parameter;
        List<String> flatParams = Lists.newArrayList();
        while (localParam != null) {
            flatParams.add(localParam.getValue());
            localParam = localParam.getNextParameter();
        }
        Collections.sort(flatParams);
        sb.append(Joiner.on(",").join(flatParams));
        sb.append(")");
        return sb.toString();
    }

    public boolean isDimensionAsMetric() {
        return isDimensionAsMetric;
    }

    public void setDimensionAsMetric(boolean isDimensionAsMetric) {
        this.isDimensionAsMetric = isDimensionAsMetric;
        if (measureType != null) {
            reInitMeasureType();
        }
    }

    public String getExpression() {
        return expression;
    }

    public ParameterDesc getParameter() {
        return parameter;
    }

    public int getParameterCount() {
        int count = 0;
        for (ParameterDesc p = parameter; p != null; p = p.getNextParameter()) {
            count++;
        }
        return count;
    }

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public DataType getReturnDataType() {
        return returnDataType;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        result = prime * result + ((isCount() || parameter == null) ? 0 : parameter.hashCode());
        // NOTE: don't compare returnType, FunctionDesc created at query engine does not have a returnType
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FunctionDesc other = (FunctionDesc) obj;
        if (expression == null) {
            if (other.expression != null)
                return false;
        } else if (!expression.equals(other.expression))
            return false;
        if (isCountDistinct()) {
            // for count distinct func, param's order doesn't matter
            if (parameter == null) {
                if (other.parameter != null)
                    return false;
            } else {
                return parameter.equalInArbitraryOrder(other.parameter);
            }
        } else if (!isCount()) { // NOTE: don't check the parameter of count()
            if (parameter == null) {
                if (other.parameter != null)
                    return false;
            } else {
                if (!parameter.equals(other.parameter))
                    return false;
            }
        }
        // NOTE: don't compare returnType, FunctionDesc created at query engine does not have a returnType
        return true;
    }

    @Override
    public String toString() {
        return "FunctionDesc [expression=" + expression + ", parameter=" + parameter + ", returnType=" + returnType + "]";
    }

}
