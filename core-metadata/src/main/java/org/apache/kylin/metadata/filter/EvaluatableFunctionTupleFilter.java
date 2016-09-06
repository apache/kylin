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
import java.util.Collection;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.StringSerializer;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import com.google.common.collect.Lists;

public class EvaluatableFunctionTupleFilter extends BuiltInFunctionTupleFilter {

    private boolean constantsInitted = false;

    //about non-like
    private List<Object> values;
    private Object tupleValue;

    public EvaluatableFunctionTupleFilter(String name) {
        super(name, FilterOperatorEnum.EVAL_FUNC);
        values = Lists.newArrayListWithCapacity(1);
        values.add(null);
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

        TblColRef tblColRef = this.getColumn();
        DataType strDataType = DataType.getType("string");
        if (tblColRef.getType() != strDataType) {
            throw new IllegalStateException("Only String type is allow in BuiltInFunction");
        }
        ByteArray valueByteArray = (ByteArray) tupleValue;
        StringSerializer serializer = new StringSerializer(strDataType);
        String value = serializer.deserialize(ByteBuffer.wrap(valueByteArray.array(), valueByteArray.offset(), valueByteArray.length()));

        try {
            if (isLikeFunction()) {
                return (Boolean) invokeFunction(value);
            } else {
                this.tupleValue = invokeFunction(value);
                //convert back to ByteArray format because the outer EvaluatableFunctionTupleFilter assumes input as ByteArray
                ByteBuffer buffer = ByteBuffer.allocate(valueByteArray.length() * 2);
                serializer.serialize((String) this.tupleValue, buffer);
                this.tupleValue = new ByteArray(buffer.array(), 0, buffer.position());

                return true;
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<?> getValues() {
        this.values.set(0, tupleValue);
        return values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        if (!isValid()) {
            throw new IllegalStateException("must be valid");
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

    @Override
    public Object invokeFunction(Object input) throws InvocationTargetException, IllegalAccessException {
        if (isLikeFunction())
            initConstants();
        return super.invokeFunction(input);
    }

    private void initConstants() {
        if (constantsInitted) {
            return;
        }
        //will replace the ByteArray pattern to String type
        ByteArray byteArray = (ByteArray) methodParams.get(constantPosition);
        StringSerializer s = new StringSerializer(DataType.getType("string"));
        String pattern = s.deserialize(ByteBuffer.wrap(byteArray.array(), byteArray.offset(), byteArray.length()));
        //TODO 
        //pattern = pattern.toLowerCase();//to remove upper case
        methodParams.set(constantPosition, pattern);
        constantsInitted = true;
    }

    //even for "tolower(s)/toupper(s)/substring(like) like pattern", the like pattern can be used for index searching
    public String getLikePattern() {
        if (!isLikeFunction()) {
            return null;
        }

        initConstants();
        return (String) methodParams.get(1);
    }

    public boolean isLikeFunction() {
        return "like".equalsIgnoreCase(this.getName());
    }

}
