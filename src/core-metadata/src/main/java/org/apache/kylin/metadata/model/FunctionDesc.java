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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_MEASURE_DATA_TYPE;
import static org.apache.kylin.metadata.datatype.DataType.ANY;
import static org.apache.kylin.metadata.datatype.DataType.BIGINT;
import static org.apache.kylin.metadata.datatype.DataType.DECIMAL;
import static org.apache.kylin.metadata.datatype.DataType.DOUBLE;
import static org.apache.kylin.metadata.datatype.DataType.FLOAT;
import static org.apache.kylin.metadata.datatype.DataType.INTEGER;
import static org.apache.kylin.metadata.datatype.DataType.SMALL_INT;
import static org.apache.kylin.metadata.datatype.DataType.TINY_INT;
import static org.apache.kylin.metadata.model.TblColRef.UNKNOWN_ALIAS;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

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
        return newInstance(FunctionDesc.FUNC_COUNT, Lists.newArrayList(ParameterDesc.newInstance("1")), BIGINT);
    }

    public static String proposeReturnType(String expression, String colDataType) {
        return proposeReturnType(expression, colDataType, Maps.newHashMap(), false);
    }

    public static String proposeReturnType(String expression, String colDataType, Map<String, String> override) {
        return proposeReturnType(expression, colDataType, override, false);
    }

    public static String proposeReturnType(String expression, String colDataType, Map<String, String> override,
            boolean saveCheck) {
        if (saveCheck) {
            switch (expression) {
            case FunctionDesc.FUNC_SUM:
            case FunctionDesc.FUNC_PERCENTILE: {
                if (colDataType != null && DataType.getType(colDataType).isStringFamily()) {
                    throw new KylinException(INVALID_MEASURE_DATA_TYPE, String.format(Locale.ROOT,
                            "Invalid column type %s for measure %s", colDataType, expression));
                }
                break;
            }
            case FunctionDesc.FUNC_SUM_LC: {
                Preconditions.checkArgument(StringUtils.isNotEmpty(colDataType),
                        "SUM_LC Measure's input type shouldn't be null or empty");
                checkSumLCDataType(colDataType);
                break;
            }
            default:
                break;
            }
        }

        String returnType = override.getOrDefault(expression,
                EXPRESSION_DEFAULT_TYPE_MAP.getOrDefault(expression, colDataType));
        // widen return type for sum or sum_lc measure
        if (FunctionDesc.FUNC_SUM.equals(expression) || FunctionDesc.FUNC_SUM_LC.equals(expression)) {
            if (colDataType != null) {
                DataType type = DataType.getType(returnType);
                if (type.isIntegerFamily()) {
                    returnType = BIGINT;
                } else if (type.isDecimal()) {
                    //same with org.apache.spark.sql.catalyst.expressions.aggregate.Sum
                    returnType = String.format(Locale.ROOT, "decimal(%d,%d)",
                            DataType.decimalBoundedPrecision(type.getPrecision() + 10),
                            DataType.decimalBoundedScale(type.getScale()));
                }
            } else {
                returnType = "decimal(19,4)";
            }
        }
        return returnType;
    }

    private static void checkSumLCDataType(String dataTypeName) {
        DataType dataType = DataType.getType(dataTypeName);
        if (!dataType.isNumberFamily()) {
            throw new KylinException(INVALID_MEASURE_DATA_TYPE,
                    String.format(Locale.ROOT, "SUM_LC Measure's return type '%s' is illegal. It must be one of %s",
                            dataType, DataType.NUMBER_FAMILY));
        }
    }

    public static final String FUNC_SUM = "SUM";
    public static final String FUNC_MIN = "MIN";
    public static final String FUNC_MAX = "MAX";
    public static final String FUNC_COLLECT_SET = "COLLECT_SET";
    public static final String FUNC_COUNT = "COUNT";
    public static final String FUNC_COUNT_DISTINCT = "COUNT_DISTINCT";
    public static final String FUNC_BITMAP_UUID = "BITMAP_UUID";
    public static final String FUNC_BITMAP_COUNT = "BITMAP_COUNT";
    public static final String FUNC_INTERSECT_COUNT = "INTERSECT_COUNT";
    public static final String FUNC_INTERSECT_VALUE = "INTERSECT_VALUE";
    public static final String FUNC_INTERSECT_BITMAP_UUID = "INTERSECT_BITMAP_UUID";
    public static final String FUNC_INTERSECT_COUNT_V2 = "INTERSECT_COUNT_V2";
    public static final String FUNC_INTERSECT_VALUE_V2 = "INTERSECT_VALUE_V2";
    public static final String FUNC_INTERSECT_BITMAP_UUID_V2 = "INTERSECT_BITMAP_UUID_V2";
    public static final String FUNC_BITMAP_BUILD = "BITMAP_BUILD";
    public static final String FUNC_CORR = "CORR";
    public static final String FUNC_COUNT_DISTINCT_HLLC10 = "hllc(10)";
    public static final String FUNC_COUNT_DISTINCT_BIT_MAP = "bitmap";
    public static final String FUNC_PERCENTILE = "PERCENTILE_APPROX";
    public static final String FUNC_GROUPING = "GROUPING";
    public static final String FUNC_TOP_N = "TOP_N";
    public static final String FUNC_SUM_LC = "SUM_LC";
    public static final ImmutableSet<String> DIMENSION_AS_MEASURES = ImmutableSet.<String> builder()
            .add(FUNC_MAX, FUNC_MIN, FUNC_COUNT_DISTINCT).build();
    public static final ImmutableSet<String> NOT_SUPPORTED_FUNCTION = ImmutableSet.<String> builder().build();
    public static final ImmutableSet<String> NOT_SUPPORTED_FUNCTION_TABLE_INDEX = ImmutableSet.<String> builder()
            .add(FUNC_COLLECT_SET).build();

    private static final Map<String, String> EXPRESSION_DEFAULT_TYPE_MAP = Maps.newHashMap();

    static {
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_TOP_N, "topn(100, 4)");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COUNT_DISTINCT, FUNC_COUNT_DISTINCT_BIT_MAP);
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_PERCENTILE, "percentile(100)");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COUNT, BIGINT);
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_COLLECT_SET, "ARRAY");
        EXPRESSION_DEFAULT_TYPE_MAP.put(FUNC_CORR, "double");
    }

    public static final String PARAMETER_TYPE_CONSTANT = "constant";
    public static final String PARAMETER_TYPE_COLUMN = "column";
    public static final String PARAMETER_TYPE_MATH_EXPRESSION = "math_expression";

    @JsonProperty("expression")
    private String expression;
    @Getter
    @Setter
    @JsonProperty("parameters")
    private List<ParameterDesc> parameters;
    @JsonProperty("returntype")
    private String returnType;

    @Setter
    @JsonProperty("configuration")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> configuration = Maps.newLinkedHashMap();

    private DataType returnDataType;
    private MeasureType<?> measureType;
    private boolean isDimensionAsMetric = false;

    public void init(NDataModel model) {
        expression = expression.toUpperCase(Locale.ROOT);
        if (expression.equals(PercentileMeasureType.FUNC_PERCENTILE)) {
            expression = PercentileMeasureType.FUNC_PERCENTILE_APPROX; // for backward compatibility
        }

        for (ParameterDesc p : getParameters()) {
            if (p.isColumnType()) {
                TblColRef colRef = model.findColumn(p.getValue());
                p.setValue(colRef.getIdentity());
                p.setColRef(colRef);
                if (expression.equals(FUNC_SUM_LC)) {
                    if (Objects.isNull(returnDataType)) {
                        // use the first column to init returnType and returnDataType, ignore the second timestamp column
                        returnType = proposeReturnType(expression, colRef.getDatatype(), Maps.newHashMap(),
                                model.isSaveCheck());
                        returnDataType = DataType.getType(returnType);
                    }
                } else {
                    returnDataType = DataType.getType(proposeReturnType(expression, colRef.getDatatype(),
                            Maps.newHashMap(), model.isSaveCheck()));
                }
            }
        }
        if (!expression.equals(FUNC_SUM_LC)) {
            if (returnDataType == null) {
                returnDataType = DataType.getType(BIGINT);
            }
            if (!StringUtils.isEmpty(returnType)) {
                returnDataType = DataType.getType(returnType);
            }
            returnType = returnDataType.toString();
        }
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

    public DataType getRewriteFieldType() {
        if (getMeasureType() instanceof BasicMeasureType) {
            if (isMax() || isMin()) {
                return getColRefs().get(0).getType();
            } else if (isSum()) {
                if (parameters.get(0).isConstant())
                    return returnDataType;

                return getColRefs().get(0).getType();
            } else if (isCount()) {
                return DataType.getType(BIGINT);
            } else {
                throw new IllegalArgumentException("unknown measure type " + getMeasureType());
            }
        } else {
            return ANY;
        }
    }

    public List<TblColRef> getColRefs() {
        if (CollectionUtils.isEmpty(parameters))
            return Lists.newArrayList();

        return parameters.stream().filter(ParameterDesc::isColumnType).map(ParameterDesc::getColRef)
                .collect(Collectors.toList());
    }

    public Collection<TblColRef> getSourceColRefs() {
        Set<TblColRef> neededCols = new HashSet<>();
        for (TblColRef colRef : getColRefs()) {
            neededCols.addAll(colRef.getSourceColumns());
        }
        return neededCols;
    }

    public static boolean nonSupportFunTableIndex(List<FunctionDesc> aggregations) {
        for (FunctionDesc functionDesc : aggregations) {
            if (NOT_SUPPORTED_FUNCTION_TABLE_INDEX.contains(functionDesc.expression)) {
                return true;
            }
        }
        return false;
    }

    public ColumnDesc newFakeRewriteColumn(String rewriteFiledName, TableDesc sourceTable) {
        ColumnDesc fakeCol = new ColumnDesc();
        fakeCol.setName(rewriteFiledName);
        fakeCol.setDatatype(getRewriteFieldType().toString());
        if (isCountConstant())
            fakeCol.setNullable(false);
        fakeCol.init(sourceTable);
        return fakeCol;
    }

    public ColumnDesc newFakeRewriteColumn(TableDesc sourceTable) {
        return newFakeRewriteColumn(getRewriteFieldName(), sourceTable);
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
        return FUNC_COUNT.equalsIgnoreCase(expression) && CollectionUtils.isNotEmpty(parameters)
                && parameters.get(0).isColumnType();
    }

    public boolean isAggregateOnConstant() {
        return !this.isCount() && CollectionUtils.isNotEmpty(parameters) && parameters.get(0).isConstantParameterDesc();
    }

    public boolean isCountConstant() {//count(*) and count(1)
        return FUNC_COUNT.equalsIgnoreCase(expression)
                && (CollectionUtils.isEmpty(parameters) || parameters.get(0).isConstant());
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

    public boolean isDatatypeSuitable(DataType dataType) {
        switch (expression) {
        case FUNC_SUM:
        case FUNC_TOP_N:
            List<String> suitableTypes = Arrays.asList(TINY_INT, SMALL_INT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL);
            return suitableTypes.contains(dataType.getName());
        case FUNC_PERCENTILE:
            suitableTypes = Arrays.asList(TINY_INT, SMALL_INT, INTEGER, BIGINT);
            return suitableTypes.contains(dataType.getName());
        default:
            return true;
        }
    }

    // propose dimension as measure to answer count(distinct expr)、min(expr)、max(expr)
    public boolean canAnsweredByDimensionAsMeasure() {
        return DIMENSION_AS_MEASURES.contains(expression) && CollectionUtils.isNotEmpty(parameters)
                && UNKNOWN_ALIAS.equals(parameters.get(0).getColRef().getTableAlias());
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
                return !CollectionUtils.isNotEmpty(other.getParameters());
            } else {
                return parametersEqualInArbitraryOrder(this.parameters, other.getParameters());
            }
        } else if (isCountConstant() && ((FunctionDesc) obj).isCountConstant()) { //count(*) and count(1) are equals
            return true;
        } else {
            if (CollectionUtils.isEmpty(parameters)) {
                return !CollectionUtils.isNotEmpty(other.getParameters());
            } else {
                return Objects.equals(this.parameters, other.getParameters());
            }
        }
        // NOTE: don't compare returnType, FunctionDesc created at query engine does not have a returnType
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

    private boolean parametersEqualInArbitraryOrder(List<ParameterDesc> parameters,
            List<ParameterDesc> otherParameters) {
        return parameters.containsAll(otherParameters) && otherParameters.containsAll(parameters);
    }
}
