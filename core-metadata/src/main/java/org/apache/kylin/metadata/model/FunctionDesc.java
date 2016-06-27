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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class FunctionDesc {

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
    private HashMap<String, String> configuration = new LinkedHashMap<String, String>();

    private DataType returnDataType;
    private MeasureType<?> measureType;
    private boolean isDimensionAsMetric = false;

    public void init(TableDesc factTable, List<TableDesc> lookupTables) {
        expression = expression.toUpperCase();
        returnDataType = DataType.getType(returnType);

        for (ParameterDesc p = parameter; p != null; p = p.getNextParameter()) {
            p.setValue(p.getValue().toUpperCase());
        }

        ArrayList<TblColRef> colRefs = Lists.newArrayList();
        for (ParameterDesc p = parameter; p != null; p = p.getNextParameter()) {
            if (p.isColumnType()) {
                ColumnDesc sourceColumn = findColumn(factTable, lookupTables, p.getValue());
                TblColRef colRef = new TblColRef(sourceColumn);
                colRefs.add(colRef);
            }
        }

        parameter.setColRefs(colRefs);
    }

    private ColumnDesc findColumn(TableDesc factTable, List<TableDesc> lookups, String columnName) {
        ColumnDesc ret = factTable.findColumnByName(columnName);
        if (ret != null) {
            return ret;
        }

        for (TableDesc lookup : lookups) {
            ret = lookup.findColumnByName(columnName);
            if (ret != null) {
                return ret;
            }
        }
        throw new IllegalStateException("Column is not found in any table from the model: " + columnName);
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
            return "COUNT__"; // ignores parameter, count(*), count(1), count(col) are all the same
        } else {
            return getFullExpression().replaceAll("[(), ]", "_");
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

    public ParameterDesc getParameter() {
        return parameter;
    }

    public void setParameter(ParameterDesc parameter) {
        this.parameter = parameter;
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

    public DataType getReturnDataType() {
        return returnDataType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
        this.returnDataType = DataType.getType(returnType);
    }

    public TblColRef selectTblColRef(Collection<TblColRef> metricColumns, String factTableName) {
        if (this.isCount())
            return null; // count is not about any column but the whole row

        ParameterDesc parameter = this.getParameter();
        if (parameter == null)
            return null;

        String columnName = parameter.getValue();
        for (TblColRef col : metricColumns) {
            if (col.isSameAs(factTableName, columnName)) {
                return col;
            }
        }
        return null;
    }

    public HashMap<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(HashMap<String, String> configurations) {
        this.configuration = configurations;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        result = prime * result + ((isCount() || parameter == null) ? 0 : parameter.hashCode());
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
            } else {
                if (parameter == null) {
                    if (other.parameter != null)
                        return false;
                } else if (!parameter.equals(other.parameter))
                    return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "FunctionDesc [expression=" + expression + ", parameter=" + parameter + ", returnType=" + returnType + "]";
    }

}
