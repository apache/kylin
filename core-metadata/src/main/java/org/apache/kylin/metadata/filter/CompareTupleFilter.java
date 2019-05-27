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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * @author xjiang
 */
public class CompareTupleFilter extends TupleFilter implements IOptimizeableTupleFilter {
    
    public enum CompareResultType {
        AlwaysTrue, AlwaysFalse, Unknown
    }

    // if the two children are both CompareTupleFilter, isNormal will be false
    private boolean isNormal = true;

    // operand 1 is either a column or a function
    private TblColRef column;
    private FunctionTupleFilter function;
    private TblColRef secondColumn;

    // operand 2 is constants
    private Set<Object> conditionValues;
    private Object firstCondValue;
    private Map<String, Object> dynamicVariables;

    public CompareTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);
        this.conditionValues = new HashSet<Object>();
        this.dynamicVariables = new HashMap<String, Object>();
        boolean opGood = (op == FilterOperatorEnum.EQ || op == FilterOperatorEnum.NEQ //
                || op == FilterOperatorEnum.LT || op == FilterOperatorEnum.LTE //
                || op == FilterOperatorEnum.GT || op == FilterOperatorEnum.GTE //
                || op == FilterOperatorEnum.IN || op == FilterOperatorEnum.NOTIN //
                || op == FilterOperatorEnum.ISNULL || op == FilterOperatorEnum.ISNOTNULL);
        if (opGood == false)
            throw new IllegalArgumentException("Unsupported operator " + op);
    }

    private CompareTupleFilter(CompareTupleFilter another) {
        super(new ArrayList<TupleFilter>(another.children), another.operator);
        this.column = another.column;
        this.firstCondValue = another.getFirstValue();
        this.function = another.getFunction();
        this.conditionValues = new HashSet<Object>();
        this.conditionValues.addAll(another.conditionValues);
        this.dynamicVariables = new HashMap<String, Object>();
        this.dynamicVariables.putAll(another.dynamicVariables);
    }

    @Override
    public void addChild(TupleFilter child) {
        if (child instanceof CompareTupleFilter) {
            child = optimizeChildCompareTupleFilter((CompareTupleFilter) child);
        }
        super.addChild(child);
        if (child instanceof ColumnTupleFilter) {
            ColumnTupleFilter columnFilter = (ColumnTupleFilter) child;
            if (this.column != null) {
                this.secondColumn = columnFilter.getColumn();
            } else {
                this.column = columnFilter.getColumn();
                // if value is before column, we need to reverse the operator. e.g. "1 >= c1" => "c1 <= 1"
                // children.size() > 1 means already added one conditionValue or dynamicVariable
                if (this.children.size() > 1 && needSwapOperator()) {
                    this.operator = SWAP_OP_MAP.get(this.operator);
                    TupleFilter last = this.children.remove(this.children.size() - 1);
                    this.children.add(0, last);
                }
            }
        } else if (child instanceof ConstantTupleFilter) {
            this.conditionValues.addAll(child.getValues());
            if (!this.conditionValues.isEmpty()) {
                this.firstCondValue = this.conditionValues.iterator().next();
            }
        } else if (child instanceof DynamicTupleFilter) {
            DynamicTupleFilter dynamicFilter = (DynamicTupleFilter) child;
            if (!this.dynamicVariables.containsKey(dynamicFilter.getVariableName())) {
                this.dynamicVariables.put(dynamicFilter.getVariableName(), null);
            }
        } else if (child instanceof FunctionTupleFilter) {
            this.function = (FunctionTupleFilter) child;
        }
    }

    private boolean needSwapOperator() {
        return operator == FilterOperatorEnum.LT || operator == FilterOperatorEnum.GT || operator == FilterOperatorEnum.LTE || operator == FilterOperatorEnum.GTE;
    }

    @Override
    public Set<?> getValues() {
        return conditionValues;
    }

    public Object getFirstValue() {
        return firstCondValue;
    }

    public TblColRef getColumn() {
        return column;
    }

    public FunctionTupleFilter getFunction() {
        return function;
    }

    public Map<String, Object> getVariables() {
        return dynamicVariables;
    }

    public void bindVariable(String variable, Object value) {
        this.dynamicVariables.put(variable, value);
        this.conditionValues.add(value);
        this.firstCondValue = this.conditionValues.iterator().next();
    }

    public void clearPreviousVariableValues(String variable) {
        Object previousValue = dynamicVariables.get(variable);
        if (previousValue == null) {
            return;
        }
        if (this.firstCondValue == previousValue) {
            this.firstCondValue = null;
        }
        this.conditionValues.remove(previousValue);
    }

    @Override
    public TupleFilter copy() {
        return new CompareTupleFilter(this);
    }

    @Override
    public TupleFilter reverse() {
        TupleFilter reverse = copy();
        reverse.operator = REVERSE_OP_MAP.get(this.operator);
        return reverse;
    }

    @Override
    public String toString() {
        return (function == null ? column : function) + " " + operator + " " + conditionValues;
    }

    // TODO requires generalize, currently only evaluates COLUMN {op} CONST
    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem cs) {
        // extract tuple value
        Object tupleValue = null;
        for (TupleFilter filter : this.children) {
            if (!isConstant(filter)) {
                filter.evaluate(tuple, cs);
                tupleValue = filter.getValues().iterator().next();
            }
        }

        // consider null case
        if (cs.isNull(tupleValue)) {
            if (operator == FilterOperatorEnum.ISNULL)
                return true;
            else
                return false;
        } else {
            if (operator == FilterOperatorEnum.ISNOTNULL)
                return true;
            else if (operator == FilterOperatorEnum.ISNULL)
                return false;
        }
        
        if (cs.isNull(firstCondValue)) {
            return false;
        }

        // tricky here -- order is ensured by string compare (even for number columns)
        // because it's row key ID (not real value) being compared
        int comp = cs.compare(tupleValue, firstCondValue);

        boolean result;
        switch (operator) {
        case EQ:
            result = comp == 0;
            break;
        case NEQ:
            result = comp != 0;
            break;
        case LT:
            result = comp < 0;
            break;
        case LTE:
            result = comp <= 0;
            break;
        case GT:
            result = comp > 0;
            break;
        case GTE:
            result = comp >= 0;
            break;
        case IN:
            result = conditionValues.contains(tupleValue);
            break;
        case NOTIN:
            result = !conditionValues.contains(tupleValue);
            break;
        default:
            result = false;
        }
        return result;
    }

    private boolean isConstant(TupleFilter filter) {
        return (filter instanceof ConstantTupleFilter) || (filter instanceof DynamicTupleFilter);
    }

    @Override
    public boolean isEvaluable() {
        return isNormal && (column != null || (function != null && function.isEvaluable())) //
                && (!conditionValues.isEmpty() || operator == FilterOperatorEnum.ISNOTNULL || operator == FilterOperatorEnum.ISNULL) //
                && secondColumn == null;
    }

    public CompareResultType getCompareResultType() {
        // cases like 1 = 1, or 'a' <> 'b'
        if (this.operator == FilterOperatorEnum.EQ || this.operator == FilterOperatorEnum.NEQ) {
            if (this.children != null && this.children.size() == 2 && //
                    this.children.get(0) instanceof ConstantTupleFilter && //
                    this.children.get(1) instanceof ConstantTupleFilter) {
                if (((ConstantTupleFilter) this.children.get(0)).getValues().equals(((ConstantTupleFilter) this.children.get(1)).getValues())) {
                    return this.operator == FilterOperatorEnum.EQ ? CompareResultType.AlwaysTrue : CompareResultType.AlwaysFalse;
                } else {
                    return this.operator == FilterOperatorEnum.EQ ? CompareResultType.AlwaysFalse : CompareResultType.AlwaysTrue;
                }
            }
        }
        return CompareResultType.Unknown;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void serialize(IFilterCodeSystem cs, ByteBuffer buffer) {
        int size = this.dynamicVariables.size();
        BytesUtil.writeVInt(size, buffer);
        for (Map.Entry<String, Object> entry : this.dynamicVariables.entrySet()) {
            BytesUtil.writeUTFString(entry.getKey(), buffer);
            cs.serialize(entry.getValue(), buffer);
        }
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {

        this.dynamicVariables.clear();
        int size = BytesUtil.readVInt(buffer);
        for (int i = 0; i < size; i++) {
            String name = BytesUtil.readUTFString(buffer);
            Object value = cs.deserialize(buffer);
            bindVariable(name, value);
        }
    }

    @Override
    public TupleFilter acceptOptimizeTransformer(FilterOptimizeTransformer transformer) {
        return transformer.visit(this);
    }

    private TupleFilter optimizeChildCompareTupleFilter(CompareTupleFilter child) {
        FilterOptimizeTransformer transformer = new FilterOptimizeTransformer();
        TupleFilter result = child.acceptOptimizeTransformer(transformer);
        if (result == ConstantTupleFilter.TRUE) {
            // use string instead of boolean since it's encoded as string
            result = new ConstantTupleFilter("true");
        } else if (result == ConstantTupleFilter.FALSE) {
            result = new ConstantTupleFilter("false");
        } else {
            this.isNormal = false;
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CompareTupleFilter that = (CompareTupleFilter) o;

        if (operator != that.operator)
            return false;
        if (column != null ? !column.equals(that.column) : that.column != null)
            return false;
        if (function != null ? !function.equals(that.function) : that.function != null)
            return false;
        if (secondColumn != null ? !secondColumn.equals(that.secondColumn) : that.secondColumn != null)
            return false;
        if (conditionValues != null ? !conditionValues.equals(that.conditionValues) : that.conditionValues != null)
            return false;
        if (firstCondValue != null ? !firstCondValue.equals(that.firstCondValue) : that.firstCondValue != null)
            return false;
        return dynamicVariables != null ? dynamicVariables.equals(that.dynamicVariables)
                : that.dynamicVariables == null;

    }

    @Override
    public int hashCode() {
        int result = operator != null ? operator.hashCode() : 0;
        result = 31 * result + (column != null ? column.hashCode() : 0);
        result = 31 * result + (function != null ? function.hashCode() : 0);
        result = 31 * result + (secondColumn != null ? secondColumn.hashCode() : 0);
        result = 31 * result + (conditionValues != null ? conditionValues.hashCode() : 0);
        result = 31 * result + (firstCondValue != null ? firstCondValue.hashCode() : 0);
        result = 31 * result + (dynamicVariables != null ? dynamicVariables.hashCode() : 0);
        return result;
    }
}
