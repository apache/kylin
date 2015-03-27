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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * 
 * @author xjiang
 * 
 */
public class ConstantTupleFilter extends TupleFilter {

    public static final ConstantTupleFilter FALSE = new ConstantTupleFilter();
    public static final ConstantTupleFilter TRUE = new ConstantTupleFilter((Object) null); // not sure of underlying code system, null is the only value that applies to all types

    private Collection<Object> constantValues;

    public ConstantTupleFilter() {
        super(Collections.<TupleFilter> emptyList(), FilterOperatorEnum.CONSTANT);
        this.constantValues = new HashSet<Object>();
    }

    public ConstantTupleFilter(Object value) {
        this();
        this.constantValues.add(value);
    }

    public ConstantTupleFilter(Collection<?> values) {
        this();
        this.constantValues.addAll(values);
    }

    @Override
    public void addChild(TupleFilter child) {
        throw new UnsupportedOperationException("This is " + this + " and child is " + child);
    }

    @Override
    public String toString() {
        return "" + constantValues;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        return constantValues.size() > 0;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public Collection<?> getValues() {
        return this.constantValues;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public byte[] serialize(IFilterCodeSystem cs) {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int size = this.constantValues.size();
        BytesUtil.writeVInt(size, buffer);
        for (Object val : this.constantValues) {
            cs.serialize(val, buffer);
        }
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    @Override
    public void deserialize(byte[] bytes, IFilterCodeSystem<?> cs) {
        this.constantValues.clear();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int size = BytesUtil.readVInt(buffer);
        for (int i = 0; i < size; i++) {
            this.constantValues.add(cs.deserialize(buffer));
        }
    }

}
