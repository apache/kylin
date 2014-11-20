/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.metadata.model.cube;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.metadata.model.schema.DataType;

/**
 * Created with IntelliJ IDEA. User: lukhan Date: 9/26/13 Time: 1:30 PM To
 * change this template use File | Settings | File Templates.
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class FunctionDesc {

    public static final String FUNC_SUM = "SUM";
    public static final String FUNC_MIN = "MIN";
    public static final String FUNC_MAX = "MAX";
    public static final String FUNC_COUNT = "COUNT";
    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";

    public static final String PARAMTER_TYPE_CONSTANT = "constant";
    public static final String PARAMETER_TYPE_COLUMN = "column";

    @JsonProperty("expression")
    private String expression;
    @JsonProperty("parameter")
    private ParameterDesc parameter;
    @JsonProperty("returntype")
    private String returnType;

    private DataType returnDataType;
    private boolean isAppliedOnDimension = false;

    public String getRewriteFieldName() {
        if (isSum()) {
            return getParameter().getValue();
        } else if (isCount()) {
            return "COUNT__"; // ignores parameter, count(*), count(1),
                              // count(col) are all the same
        } else {
            return getFullExpression().replaceAll("[(), ]", "_");
        }
    }

    public boolean needRewrite() {
        return !isSum() && !isHolisticCountDistinct() && !isAppliedOnDimension();
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

    public boolean isHolisticCountDistinct() {
        if (isCountDistinct() && returnDataType != null && returnDataType.isBigInt()) {
            return true;
        } else {
            return false;
        }
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

    public boolean isAppliedOnDimension() {
        return isAppliedOnDimension;
    }

    public void setAppliedOnDimension(boolean isAppliedOnDimension) {
        this.isAppliedOnDimension = isAppliedOnDimension;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public ParameterDesc getParameter() {
        return parameter;
    }

    public void setParameter(ParameterDesc parameter) {
        this.parameter = parameter;
    }

    public DataType getReturnDataType() {
        return returnDataType;
    }

    void setReturnDataType(DataType returnDataType) {
        this.returnDataType = returnDataType;
    }

    public String getSQLType() {
        if (isCountDistinct())
            return "any";
        else if (isSum() || isMax() || isMin())
            return parameter.getColRefs().get(0).getType().getName();
        else
            return returnType;
    }

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        result = prime * result + ((parameter == null) ? 0 : parameter.hashCode());
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
        // NOTE: don't check the parameter of count()
        if (isCount() == false) {
            if (parameter == null) {
                if (other.parameter != null)
                    return false;
            } else if (!parameter.equals(other.parameter))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "FunctionDesc [expression=" + expression + ", parameter=" + parameter + ", returnType=" + returnType + "]";
    }

}
