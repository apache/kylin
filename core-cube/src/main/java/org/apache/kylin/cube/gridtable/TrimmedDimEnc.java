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

package org.apache.kylin.cube.gridtable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class TrimmedDimEnc extends DimensionEncoding {
    private static final long serialVersionUID = 1L;

    int fixedLen;

    //no-arg constructor is required for Externalizable
    public TrimmedDimEnc() {
    }

    public TrimmedDimEnc(int fixedLen) {
        this.fixedLen = fixedLen;
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    @Override
    public void encode(byte[] value, int valueLen, byte[] output, int outputOffset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new TrimmedDimensionSerializer(fixedLen);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(fixedLen);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fixedLen = in.readShort();
    }
}