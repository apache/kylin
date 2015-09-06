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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;

/**
 * @author xjiang
 */
public class CompareTupleFilter extends TupleFilter {

    private TblColRef column;
    private Collection<String> conditionValues;
    private String firstCondValue;
    private Map<String, String> dynamicVariables;
    private String nullString;

    public CompareTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);
        this.conditionValues = new HashSet<String>();
        this.dynamicVariables = new HashMap<String, String>();
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
        this.conditionValues = new HashSet<String>();
        this.conditionValues.addAll(another.conditionValues);
        this.dynamicVariables = new HashMap<String, String>();
        this.dynamicVariables.putAll(another.dynamicVariables);
    }

    @Override
    public void addChild(TupleFilter child) {
        super.addChild(child);
        if (child instanceof ColumnTupleFilter) {
            ColumnTupleFilter columnFilter = (ColumnTupleFilter) child;
            if (this.column != null) {
                throw new IllegalStateException("Duplicate columns! old is " + column.getName() + " and new is " + columnFilter.getColumn().getName());
            }
            this.column = columnFilter.getColumn();
            // if value is before column, we need to reverse the operator. e.g. "1 >= c1" => "c1 <= 1"
            if (!this.conditionValues.isEmpty() && needSwapOperator()) {
                this.operator = SWAP_OP_MAP.get(this.operator);
            }
        } else if (child instanceof ConstantTupleFilter) {
            this.conditionValues.addAll(child.getValues());
            this.firstCondValue = this.conditionValues.iterator().next();
        } else if (child instanceof DynamicTupleFilter) {
            DynamicTupleFilter dynamicFilter = (DynamicTupleFilter) child;
            this.dynamicVariables.put(dynamicFilter.getVariableName(), null);
        }
        //TODO
        //        else if (child instanceof ExtractTupleFilter) {
        //        } else if (child instanceof CaseTupleFilter) {
        //        }
    }

    private boolean needSwapOperator() {
        return operator == FilterOperatorEnum.LT || operator == FilterOperatorEnum.GT || operator == FilterOperatorEnum.LTE || operator == FilterOperatorEnum.GTE;
    }

    @Override
    public Collection<String> getValues() {
        return conditionValues;
    }

    public String getFirstValue() {
        return firstCondValue;
    }

    public TblColRef getColumn() {
        return column;
    }

    public Map<String, String> getVariables() {
        return dynamicVariables;
    }

    public void bindVariable(String variable, String value) {
        this.dynamicVariables.put(variable, value);
        this.conditionValues.add(value);
        this.firstCondValue = this.conditionValues.iterator().next();
    }

    public String getNullString() {
        return nullString;
    }

    public void setNullString(String nullString) {
        this.nullString = nullString;
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
        return "CompareFilter [" + column + " " + operator + " " + conditionValues + ", children=" + children + "]";
    }

    // TODO requires generalize, currently only evaluates COLUMN {op} CONST
    @Override
    public boolean evaluate(ITuple tuple) {
        // extract tuple value
        String tupleValue = null;
        for (TupleFilter filter : this.children) {
            if (isConstant(filter) == false) {
                filter.evaluate(tuple);
                tupleValue = filter.getValues().iterator().next();
            }
        }

        // consider null string
        if (nullString != null && nullString.equals(tupleValue)) {
            tupleValue = null;
        }
        if (tupleValue == null) {
            if (operator == FilterOperatorEnum.ISNULL)
                return true;
            else
                return false;
        }

        // always false if compare to null
        if (firstCondValue.equals(nullString))
            return false;

        // tricky here -- order is ensured by string compare (even for number columns)
        // because it's row key ID (not real value) being compared
        int comp = tupleValue.compareTo(firstCondValue);

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
        return column != null && !conditionValues.isEmpty();
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int size = this.dynamicVariables.size();
        BytesUtil.writeVInt(size, buffer);
        for (Map.Entry<String, String> entry : this.dynamicVariables.entrySet()) {
            BytesUtil.writeUTFString(entry.getKey(), buffer);
            BytesUtil.writeUTFString(entry.getValue(), buffer);
        }
        BytesUtil.writeAsciiString(nullString, buffer);
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    @Override
    public void deserialize(byte[] bytes) {
        this.dynamicVariables.clear();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int size = BytesUtil.readVInt(buffer);
        for (int i = 0; i < size; i++) {
            String nameString = BytesUtil.readUTFString(buffer);
            String valueString = BytesUtil.readUTFString(buffer);
            bindVariable(nameString, valueString);
        }
        this.nullString = BytesUtil.readAsciiString(buffer);
    }

}
