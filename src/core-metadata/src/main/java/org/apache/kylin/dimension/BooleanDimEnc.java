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

package org.apache.kylin.dimension;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

/**
 * Encoding Boolean values to bytes
 */
public class BooleanDimEnc extends DimensionEncoding implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String ENCODING_NAME = "boolean";

    //NOTE: when add new value, append to the array tail, DO NOT insert!
    public static String[] ALLOWED_VALUES = new String[] { "", "true", "false", "TRUE", "FALSE", "True", "False", "t",
            "f", "T", "F", "yes", "no", "YES", "NO", "Yes", "No", "y", "n", "Y", "N", "1", "0" };

    public static final Map<String, Integer> map = Maps.newHashMap();

    static {
        for (int i = 0; i < ALLOWED_VALUES.length; i++) {
            map.put(ALLOWED_VALUES[i], i);
        }
    }

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            return new BooleanDimEnc();
        }
    };

    // ============================================================================

    private int fixedLen = 1;

    //no-arg constructor is required for Externalizable
    public BooleanDimEnc() {
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    @Override
    public void encode(String value, byte[] output, int outputOffset) {
        if (value == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);
            return;
        }

        Integer encodeValue = map.get(value);
        if (encodeValue == null) {
            throw new IllegalArgumentException("Value '" + value + "' is not a recognized boolean value.");
        }

        BytesUtil.writeLong(encodeValue, output, outputOffset, fixedLen);
    }

    @Override
    public String decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }

        int x = (int) BytesUtil.readLong(bytes, offset, len);
        if (x >= ALLOWED_VALUES.length) {
            throw new IllegalStateException();
        }

        return ALLOWED_VALUES[x];
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new BooleanSerializer();
    }

    public class BooleanSerializer extends DataTypeSerializer<Object> {

        private byte[] currentBuf() {
            byte[] buf = (byte[]) current.get();
            if (buf == null) {
                buf = new byte[fixedLen];
                current.set(buf);
            }
            return buf;
        }

        @Override
        public void serialize(Object value, ByteBuffer out) {
            byte[] buf = currentBuf();
            String valueStr = value == null ? null : value.toString();
            encode(valueStr, buf, 0);
            out.put(buf);
        }

        @Override
        public Object deserialize(ByteBuffer in) {
            byte[] buf = currentBuf();
            in.get(buf);
            return decode(buf, 0, buf.length);
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return fixedLen;
        }

        @Override
        public int maxLength() {
            return fixedLen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return fixedLen;
        }

        @Override
        public Object valueOf(String str) {
            return str;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(fixedLen);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fixedLen = in.readShort();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return fixedLen;
    }
}
