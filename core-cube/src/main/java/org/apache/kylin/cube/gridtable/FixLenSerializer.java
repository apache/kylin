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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.measure.serializer.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixLenSerializer extends DataTypeSerializer {

    private static Logger logger = LoggerFactory.getLogger(FixLenSerializer.class);

    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<byte[]> current = new ThreadLocal<byte[]>();

    private int fixLen;
    transient int avoidVerbose = 0;

    FixLenSerializer(int fixLen) {
        this.fixLen = fixLen;
    }

    private byte[] currentBuf() {
        byte[] buf = current.get();
        if (buf == null) {
            buf = new byte[fixLen];
            current.set(buf);
        }
        return buf;
    }

    @Override
    public void serialize(Object value, ByteBuffer out) {
        byte[] buf = currentBuf();
        if (value == null) {
            Arrays.fill(buf, Dictionary.NULL);
            out.put(buf);
        } else {
            byte[] bytes = Bytes.toBytes(value.toString());
            if (bytes.length > fixLen) {
                if (avoidVerbose++ % 10000 == 0) {
                    logger.warn("Expect at most " + fixLen + " bytes, but got " + bytes.length + ", will truncate, value string: " + value.toString() + " times:" + avoidVerbose);
                }
            }
            out.put(bytes, 0, Math.min(bytes.length, fixLen));
            for (int i = bytes.length; i < fixLen; i++) {
                out.put(RowConstants.ROWKEY_PLACE_HOLDER_BYTE);
            }
        }
    }

    @Override
    public Object deserialize(ByteBuffer in) {
        byte[] buf = currentBuf();
        in.get(buf);

        int tail = fixLen;
        while (tail > 0 && (buf[tail - 1] == RowConstants.ROWKEY_PLACE_HOLDER_BYTE || buf[tail - 1] == Dictionary.NULL)) {
            tail--;
        }

        if (tail == 0) {
            return buf[0] == Dictionary.NULL ? null : "";
        }

        return Bytes.toString(buf, 0, tail);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return fixLen;
    }

    @Override
    public int maxLength() {
        return fixLen;
    }

    @Override
    public int getStorageBytesEstimate() {
        return fixLen;
    }

    @Override
    public Object valueOf(byte[] value) {
        try {
            return new String(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // does not happen
            throw new RuntimeException(e);
        }
    }

}
