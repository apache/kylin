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
import org.apache.kylin.metadata.tuple.ITuple;

/**
 * 
 * @author xjiang
 * 
 */
public class ConstantTupleFilter extends TupleFilter {

    public static final ConstantTupleFilter FALSE = new ConstantTupleFilter();
    public static final ConstantTupleFilter TRUE = new ConstantTupleFilter("TRUE");

    private Collection<String> constantValues;

    public ConstantTupleFilter() {
        super(Collections.<TupleFilter> emptyList(), FilterOperatorEnum.CONSTANT);
        this.constantValues = new HashSet<String>();
    }

    public ConstantTupleFilter(String value) {
        this();
        this.constantValues.add(value);
    }

    public ConstantTupleFilter(Collection<String> values) {
        this();
        this.constantValues.addAll(values);
    }

    @Override
    public void addChild(TupleFilter child) {
        throw new UnsupportedOperationException("This is " + this + " and child is " + child);
    }

    @Override
    public String toString() {
        return "ConstantFilter [constant=" + constantValues + "]";
    }

    @Override
    public boolean evaluate(ITuple tuple) {
        return constantValues.size() > 0;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public Collection<String> getValues() {
        return this.constantValues;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int size = this.constantValues.size();
        BytesUtil.writeVInt(size, buffer);
        for (String val : this.constantValues) {
            BytesUtil.writeUTFString(val, buffer);
        }
        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    @Override
    public void deserialize(byte[] bytes) {
        this.constantValues.clear();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int size = BytesUtil.readVInt(buffer);
        for (int i = 0; i < size; i++) {
            this.constantValues.add(BytesUtil.readUTFString(buffer));
        }
    }

}
