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

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.StringSerializer;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

public class EvaluatableLikeFunction extends BuiltInFunctionTupleFilter {

    public EvaluatableLikeFunction(String name) {
        super(name, FilterOperatorEnum.LIKE);
    }

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
            return false;
        }

        ByteArray valueByteArray = (ByteArray) tupleValue;
        StringSerializer serializer = new StringSerializer(DataType.getType("string"));
        String value = serializer.deserialize(ByteBuffer.wrap(valueByteArray.array(), valueByteArray.offset(), valueByteArray.length()));
        try {
            return (Boolean) invokeFunction(value);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        if (!isValid()) {
            throw new IllegalStateException("must be valid");
        }
        if (methodParams.size() != 2 || methodParams.get(0) != null || methodParams.get(1) == null) {
            throw new IllegalArgumentException("bad methodParams: " + methodParams);
        }
        BytesUtil.writeUTFString(name, buffer);
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        this.name = BytesUtil.readUTFString(buffer);
        this.initMethod();
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    private boolean isConstant(TupleFilter filter) {
        return (filter instanceof ConstantTupleFilter) || (filter instanceof DynamicTupleFilter);
    }

    public String getLikePattern() {
        ByteArray byteArray = (ByteArray) methodParams.get(1);
        StringSerializer s = new StringSerializer(DataType.getType("string"));
        String pattern = s.deserialize(ByteBuffer.wrap(byteArray.array(), byteArray.offset(), byteArray.length()));
        return pattern;
    }

}
