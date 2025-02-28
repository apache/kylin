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

import java.io.Externalizable;

import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * Dimension encoding maps a dimension (String) to bytes of fixed length.
 *
 * It is similar to Dictionary in 1) the bytes is fixed length; 2) bi-way mapping;
 * 3) the mapping preserves order, but is also different to Dictionary as the target
 * bytes can be very long while dictionary ID is 4 bytes at most. This means it is
 * hard to enumerate all values of a encoding, thus TupleFilterDictionaryTranslater
 * cannot work on DimensionEncoding.
 */
public abstract class DimensionEncoding implements Externalizable {
    private static final long serialVersionUID = 1L;

    // it's convention that all 0xff means NULL
    public static final byte NULL = (byte) 0xff;

    public static boolean isNull(byte[] bytes, int offset, int length) {
        // all 0xFF is NULL
        if (length == 0) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (bytes[i + offset] != NULL) {
                return false;
            }
        }
        return true;
    }

    public static Object[] parseEncodingConf(String encoding) {
        String[] parts = encoding.split("\\s*[(),:]\\s*");
        if (parts == null || parts.length == 0 || parts[0].isEmpty())
            throw new IllegalArgumentException("Not supported row key col encoding: '" + encoding + "'");

        final String encodingName = parts[0];
        final String[] encodingArgs = parts[parts.length - 1].isEmpty() //
                ? StringHelper.subArray(parts, 1, parts.length - 1)
                : StringHelper.subArray(parts, 1, parts.length);

        return new Object[] { encodingName, encodingArgs };
    }

    /** return the fixed length of encoded bytes */
    public abstract int getLengthOfEncoding();

    /** encode given value to bytes, note the NULL convention */
    public abstract void encode(String value, byte[] output, int outputOffset);

    /** decode given bytes to value string, note the NULL convention */
    public abstract String decode(byte[] bytes, int offset, int len);

    /** return a DataTypeSerializer that does the same encoding/decoding on ByteBuffer */
    public abstract DataTypeSerializer<Object> asDataTypeSerializer();

}
