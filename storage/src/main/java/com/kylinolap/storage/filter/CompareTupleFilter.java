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
package com.kylinolap.storage.filter;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.tuple.Tuple;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @author xjiang
 */
public class CompareTupleFilter extends TupleFilter {

    private TblColRef column;
    private Collection<String> conditionValues;
    private String firstCondValue;
    private Map<String, String> dynamicVariables;

    public CompareTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(2), op);
        this.conditionValues = new HashSet<String>();
        this.dynamicVariables = new HashMap<String, String>();
        Validate.isTrue(op == FilterOperatorEnum.EQ || op == FilterOperatorEnum.NEQ
                || op == FilterOperatorEnum.LT || op == FilterOperatorEnum.LTE || op == FilterOperatorEnum.GT
                || op == FilterOperatorEnum.GTE || op == FilterOperatorEnum.IN
                || op == FilterOperatorEnum.ISNULL || op == FilterOperatorEnum.ISNOTNULL);
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
                throw new IllegalStateException("Duplicate columns! old is " + column.getName()
                        + " and new is " + columnFilter.getColumn().getName());
            }
            this.column = columnFilter.getColumn();
            // if value is before column, we need to reverse the operator. e.g. "1 >= c1" => "c1 < 1"
            if (!this.conditionValues.isEmpty() && needSwapOperator()) {
                this.operator = REVERSE_OP_MAP.get(this.operator);
            }
        } else if (child instanceof ConstantTupleFilter) {
            this.conditionValues.addAll(child.getValues());
            this.firstCondValue = this.conditionValues.iterator().next();
        } else if (child instanceof DynamicTupleFilter) {
            DynamicTupleFilter dynamicFilter = (DynamicTupleFilter) child;
            this.dynamicVariables.put(dynamicFilter.getVariableName(), null);
        } else if (child instanceof ExtractTupleFilter) {
            // TODO
        } else if (child instanceof CaseTupleFilter) {

        }
    }

    private boolean needSwapOperator() {
        return operator == FilterOperatorEnum.LT || operator == FilterOperatorEnum.GT
                || operator == FilterOperatorEnum.LTE || operator == FilterOperatorEnum.GTE;
    }

    @Override
    public Collection<String> getValues() {
        return conditionValues;
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
        return "CompareFilter [operator=" + operator + ", column=" + column + ", values=" + conditionValues
                + ", children=" + children + "]";
    }

    @Override
    public boolean evaluate(Tuple tuple) {
        // extract tuple value 
        Object tupleValue = null;
        for (TupleFilter filter : this.children) {
            if (!(filter instanceof ConstantTupleFilter)) {
                filter.evaluate(tuple);
                tupleValue = filter.getValues().iterator().next();
            }
        }
        // compare tuple value with condition value
        boolean result = false;
        switch (this.operator) {
            case EQ:
                result = tupleValue.equals(firstCondValue);
                break;
            case NEQ:
                result = !tupleValue.equals(firstCondValue);
                break;
            case LT:
                Number tv = (Number) tupleValue;
                Number fv = Double.valueOf(firstCondValue);
                result = tv.doubleValue() < fv.doubleValue();
                break;
            case LTE:
                tv = (Number) tupleValue;
                fv = Double.valueOf(firstCondValue);
                result = tv.doubleValue() < fv.doubleValue();
                break;
            case GT:
                tv = (Number) tupleValue;
                fv = Double.valueOf(firstCondValue);
                result = tv.doubleValue() > fv.doubleValue();
                break;
            case GTE:
                tv = (Number) tupleValue;
                fv = Double.valueOf(firstCondValue);
                result = tv.doubleValue() >= fv.doubleValue();
                break;
            case ISNULL:
                result = tupleValue == null;
                break;
            case ISNOTNULL:
                result = tupleValue != null;
                break;
            case IN:
                conditionValues.contains(tupleValue);
                break;
            default:
                return false;
        }
        return result;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int size = this.dynamicVariables.size();
        BytesUtil.writeVInt(size, buffer);
        for (Map.Entry<String, String> entry : this.dynamicVariables.entrySet()) {
            BytesUtil.writeByteArray(entry.getKey().getBytes(Charset.forName("utf-8")), buffer);
            BytesUtil.writeByteArray(entry.getValue().getBytes(Charset.forName("utf-8")), buffer);
        }
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
            byte[] nameBytes = BytesUtil.readByteArray(buffer);
            String nameString = new String(nameBytes, Charset.forName("utf-8"));
            byte[] valueBytes = BytesUtil.readByteArray(buffer);
            String valueString = new String(valueBytes, Charset.forName("utf-8"));
            bindVariable(nameString, valueString);
        }
    }

}
