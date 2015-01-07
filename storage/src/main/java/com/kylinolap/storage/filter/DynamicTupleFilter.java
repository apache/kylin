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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.storage.tuple.ITuple;

/**
 * 
 * @author xjiang
 * 
 */
public class DynamicTupleFilter extends TupleFilter {

    private String variableName;

    public DynamicTupleFilter(String name) {
        super(Collections.<TupleFilter> emptyList(), FilterOperatorEnum.DYNAMIC);
        this.variableName = name;
    }

    public String getVariableName() {
        return variableName;
    }

    @Override
    public void addChild(TupleFilter child) {
        throw new UnsupportedOperationException("This is " + this + " and child is " + child);
    }

    @Override
    public String toString() {
        return "DynamicFilter [variableName=" + variableName + "]";
    }

    @Override
    public boolean evaluate(ITuple tuple) {
        return true;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public Collection<String> getValues() {
        return Collections.emptyList();
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        BytesUtil.writeUTFString(variableName, buffer);
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    @Override
    public void deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.variableName = BytesUtil.readUTFString(buffer);
    }

}
