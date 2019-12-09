/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.engine.spark.metadata.cube.measure.MeasureType;
import org.apache.kylin.engine.spark.metadata.cube.measure.MeasureTypeFactory;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.engine.spark.metadata.cube.datatype.DataType;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class FunctionDesc implements Serializable {

    public static FunctionDesc newInstance(String expression, List<ParameterDesc> parameters, String returnType) {
        FunctionDesc r = new FunctionDesc();
        r.expression = (expression == null) ? null : expression.toUpperCase(Locale.ROOT);
        r.parameters = parameters;
        r.returnType = returnType;
        r.returnDataType = DataType.getType(returnType);
        return r;
    }

    public static FunctionDesc newCountOne() {
        return newInstance(FunctionDesc.FUNC_COUNT, Lists.newArrayList(ParameterDesc.newInstance("1")), "bigint");
    }

    public static String proposeReturnType(String expression, String colDataType) {
        return proposeReturnType(expression, colDataType, Maps.newHashMap());
    }

    public static String proposeReturnType(String expression, String colDataType, Map<String, String> override) {
        String returnType = override.getOrDefault(expression,
                EXPRESSION_DEFAULT_TYPE_MAP.getOrDefault(expression, colDataType));
        switch (expression) {
        case FunctionDesc.FUNC_SUM:
            if (colDataType != null) {
                DataType type = DataType.getType(returnType);
                if (type.isIntegerFamily()) {
                    returnType = TYPE_BIGINT;
                } else if (type.isDecimal()) {
                    returnType = String.format(Locale.ROOT, "decimal(19,%d)", type.getScale());
                }
            } else {
                returnType = "decimal(19,4)";
            }
            break;
        default:
            break;
        }
        return returnType;
    }

    public static final String TYPE_BIGINT = "bigint";

    public static final String FUNC_SUM = "SUM";
    public static final String FUNC_MIN = "MIN";
    public static final String FUNC_MAX = "MAX";
    public static final String FUNC_COUNT = "COUNT";
    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";
    public static final String FUNC_INTERSECT_COUNT = "INTERSECT_COUNT";
    public static final String FUNC_CORR = "CORR";
    public static final String FUNC_COUNT_DISTINCT_HLLC10 = "hllc(10)";
    public static final String FUNC_COUNT_DISTINCT_BIT_MAP = "bitmap";
    public static final String FUNC_PERCENTILE = "PERCENTILE_APPROX";
    public static final String FUNC_GROUPING = "GROUPING";
    public static final String FUNC_TOP_N = "TOP_N";
    public static final ImmutableSet<String> DIMENSION_AS_MEASURES = ImmutableSet.<String> builder()
            .add(FUNC_MAX, FUNC_MIN, FUNC_COUNT_DISTINCT).build();
    public static final ImmutableSet<String> BUILT_IN_AGGREGATIONS = ImmutableSet.<String> builder()
            .add(FUNC_MAX, FUNC_MIN, FUNC_COUNT_DISTINCT).add(FUNC_COUNT, FUNC_SUM, FUNC_PERCENTILE).build();
    public static final ImmutableSet<String> NOT_SUPPORTED_FUNCTION = ImmutableSet.<String> builder()
            .add(FUNC_CORR).build();

    private static final Map<String, String> EXPRESSION_DEFAULT_TYPE_MAP = Maps.newHashMap();

    static {
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_TOP_N, "topn(100, 4)");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COUNT_DISTINCT, FUNC_COUNT_DISTINCT_BIT_MAP);
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_PERCENTILE, "percentile(100)");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COUNT, TYPE_BIGINT);
    }

    public static final String PARAMETER_TYPE_CONSTANT = "constant";
    public static final String PARAMETER_TYPE_COLUMN = "column";
    public static final String PARAMETER_TYPE_MATH_EXPRESSION = "math_expression";

    @JsonProperty("expression")
    private String expression;
    @JsonProperty("parameters")
    private List<ParameterDesc> parameters;
    @JsonProperty("returntype")
    private String returnType;

    public List<ParameterDesc> getParameters() {
        return parameters;
    }

    public void setParameters(List<ParameterDesc> parameters) {
        this.parameters = parameters;
    }

    @JsonProperty("configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> configuration = Maps.newLinkedHashMap();

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    private DataType returnDataType;
    private MeasureType<?> measureType;
    private boolean isDimensionAsMetric = false;

    public void init(DataModel model) {
        expression = expression.toUpperCase(Locale.ROOT);
        if (expression.equals(PercentileMeasureType.FUNC_PERCENTILE)) {
            expression = PercentileMeasureType.FUNC_PERCENTILE_APPROX; // for backward compatibility
        }

        for (ParameterDesc p : getParameters()) {
            if (p.isColumnType()) {
                TblColRef colRef = model.findColumn(p.getValue());
                returnDataType = DataType.getType(proposeReturnType(expression, colRef.getDatatype()));
                p.setValue(colRef.getIdentity());
                p.setColRef(colRef);
            }
        }
        if (returnDataType == null) {
            returnDataType = DataType.getType(TYPE_BIGINT);
        }
        if (!StringUtils.isEmpty(returnType)) {
            returnDataType = DataType.getType(returnType);
        }
        returnType = returnDataType.toString();
    }

    private void reInitMeasureType() {
        if (isDimensionAsMetric && isCountDistinct()) {
            // create DimCountDis
            measureType = MeasureTypeFactory.createNoRewriteFieldsMeasureType(getExpression(), getReturnDataType());
            returnDataType = DataType.getType("dim_dc");
        } else {
            measureType = MeasureTypeFactory.create(getExpression(), getReturnDataType());
        }
    }

    public MeasureType getMeasureType() {
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
        if (isCountConstant()) {
            return "_KY_" + "COUNT__"; // ignores parameter, count(*) and count(1) are the same
        } else if (isCountDistinct()) {
            return "_KY_" + getFullExpressionInAlphabetOrder().replaceAll("[(),. ]", "_");
        } else {
            return "_KY_" + getFullExpression().replaceAll("[(),. ]", "_");
        }
    }

    public List<TblColRef> getColRefs() {
        if (CollectionUtils.isEmpty(parameters))
            return Lists.newArrayList();

        return parameters.stream().filter(ParameterDesc::isColumnType).map(ParameterDesc::getColRef).collect(Collectors.toList());
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

    public boolean isCountOnColumn() {
        return FUNC_COUNT.equalsIgnoreCase(expression) && CollectionUtils.isNotEmpty(parameters) && parameters.get(0).isColumnType();
    }

    public boolean isAggregateOnConstant() {
        return !this.isCount() && CollectionUtils.isNotEmpty(parameters) && parameters.get(0).isConstantParameterDesc();
    }

    public boolean isCountConstant() {//count(*) and count(1)
        return FUNC_COUNT.equalsIgnoreCase(expression) && (CollectionUtils.isEmpty(parameters) || parameters.get(0).isConstant());
    }

    public boolean isCountDistinct() {
        return FUNC_COUNT_DISTINCT.equalsIgnoreCase(expression);
    }

    public boolean isGrouping() {
        return FUNC_GROUPING.equalsIgnoreCase(expression);
    }

    /**
     * Get Full Expression such as sum(amount), count(1), count(*)...
     */
    public String getFullExpression() {
        StringBuilder sb = new StringBuilder(expression);
        sb.append("(");
        for (ParameterDesc desc : parameters) {
            sb.append(desc.getValue());
            sb.append(",");
        }
        if (sb.length() > 0)
            sb.setLength(sb.length() - 1);

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
        List<String> flatParams = parameters.stream().map(ParameterDesc::getValue).collect(Collectors.toList());
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

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public int getParameterCount() {
        return CollectionUtils.isEmpty(parameters) ? 0 : parameters.size();
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
        result = prime * result + ((isCount() || CollectionUtils.isEmpty(parameters)) ? 0 : parameters.hashCode());
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
            if (CollectionUtils.isEmpty(parameters)) {
                if (CollectionUtils.isNotEmpty(other.getParameters()))
                    return false;
            } else {
                return parametersEqualInArbitraryOrder(this.parameters, other.getParameters());
            }
        } else if (isCountConstant() && ((FunctionDesc) obj).isCountConstant()) { //count(*) and count(1) are equals
            return true;
        } else {
            if (CollectionUtils.isEmpty(parameters)) {
                if (CollectionUtils.isNotEmpty(other.getParameters()))
                    return false;
            } else {
                if (!Objects.equals(this.parameters, other.getParameters()))
                    return false;
            }
        }
        // NOTE: don't compare returnType, FunctionDesc created at query engine does not have a returnType
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        parameters.forEach(parameter -> {
            sb.append(parameter.toString());
            sb.append(",");
        });
        if (sb.length() > 0)
            sb.setLength(sb.length() - 1);
        return "FunctionDesc [expression=" + expression + ", parameter=" + sb.toString() + ", returnType=" + returnType
                + "]";
    }

    private boolean parametersEqualInArbitraryOrder(List<ParameterDesc> parameters, List<ParameterDesc> otherParameters) {
        return parameters.containsAll(otherParameters) && otherParameters.containsAll(parameters);
    }
}
