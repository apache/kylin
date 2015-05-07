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

package org.apache.kylin.cube.kv;

import java.util.Arrays;

import org.apache.kylin.dict.IDictionaryAware;
import org.apache.kylin.common.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Read/Write column values from/into bytes
 *
 * @author yangli9
 */
@SuppressWarnings("unchecked")
public class RowKeyColumnIO {

    private static final Logger logger = LoggerFactory.getLogger(RowKeyColumnIO.class);

    private IDictionaryAware IDictionaryAwareness;

    public RowKeyColumnIO(IDictionaryAware IDictionaryAwareness) {
        this.IDictionaryAwareness = IDictionaryAwareness;
    }

    public int getColumnLength(TblColRef col) {
        return IDictionaryAwareness.getColumnLength(col);
    }

    //TODO is type cast really necessary here?
    public Dictionary<String> getDictionary(TblColRef col) {
        return (Dictionary<String>) IDictionaryAwareness.getDictionary(col);
    }

    public void writeColumnWithoutDictionary(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength) {
        if (srcLength >= dstLength) {
            System.arraycopy(src, srcOffset, dst, dstOffset, dstLength);
        } else {
            System.arraycopy(src, srcOffset, dst, dstOffset, srcLength);
            Arrays.fill(dst, dstOffset + srcLength, dstOffset + dstLength, RowConstants.ROWKEY_PLACE_HOLDER_BYTE);
        }
    }

    public void writeColumnWithDictionary(Dictionary<String> dictionary, byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOffset, int dstLength, int roundingFlag, int defaultValue) {
        // dict value
        try {
            int id = dictionary.getIdFromValueBytes(src, srcOffset, srcLength, roundingFlag);
            BytesUtil.writeUnsigned(id, dst, dstOffset, dictionary.getSizeOfId());
        } catch (IllegalArgumentException ex) {
            Arrays.fill(dst, dstOffset, dstOffset + dstLength, (byte) defaultValue);
            logger.error("Can't translate value " + Bytes.toString(src, srcOffset, srcLength) + " to dictionary ID, roundingFlag " + roundingFlag + ". Using default value " + String.format("\\x%02X", defaultValue));
        }
    }



    public void writeColumn(TblColRef column, byte[] value, int valueLen, byte defaultValue, byte[] output, int outputOffset) {
        writeColumn(column, value, valueLen, 0, defaultValue, output, outputOffset);
    }

    public void writeColumn(TblColRef column, byte[] value, int valueLen, int roundingFlag, byte defaultValue, byte[] output, int outputOffset) {

        final Dictionary<String> dict = getDictionary(column);
        final int columnLen = getColumnLength(column);

        // non-dict value
        if (dict == null) {
            byte[] valueBytes = padFixLen(columnLen, value, valueLen);
            System.arraycopy(valueBytes, 0, output, outputOffset, columnLen);
            return;
        }

        // dict value
        try {
            int id = dict.getIdFromValueBytes(value, 0, valueLen, roundingFlag);
            BytesUtil.writeUnsigned(id, output, outputOffset, dict.getSizeOfId());
        } catch (IllegalArgumentException ex) {
            for (int i = outputOffset; i < outputOffset + columnLen; i++) {
                output[i] = defaultValue;
            }
            logger.error("Can't translate value " + Bytes.toString(value, 0, valueLen) + " to dictionary ID, roundingFlag " + roundingFlag + ". Using default value " + String.format("\\x%02X", defaultValue));
        }
    }

    private byte[] padFixLen(int length, byte[] valueBytes, int valLen) {
        if (valLen == length) {
            return valueBytes;
        } else if (valLen < length) {
            byte[] newValueBytes = new byte[length];
            System.arraycopy(valueBytes, 0, newValueBytes, 0, valLen);
            Arrays.fill(newValueBytes, valLen, length, RowConstants.ROWKEY_PLACE_HOLDER_BYTE);
            return newValueBytes;
        } else {
            return Arrays.copyOf(valueBytes, length);
        }
    }

    public String readColumnString(TblColRef col, byte[] bytes, int offset, int length) {
        Dictionary<String> dict = getDictionary(col);
        if (dict == null) {
            if (isNull(bytes, offset, length)) {
                return null;
            }
            bytes = removeFixLenPad(bytes, offset, length);
            return Bytes.toString(bytes);
        } else {
            int id = BytesUtil.readUnsigned(bytes, offset, length);
            try {
                String value = dict.getValueFromId(id);
                return value;
            } catch (IllegalArgumentException e) {
                logger.error("Can't get dictionary value for column " + col.getName() + " (id = " + id + ")");
                return "";
            }
        }
    }

    public String readColumnString(TblColRef col, byte[] bytes, int bytesLen) {
        return readColumnString(col, bytes, 0, bytesLen);
    }

    private boolean isNull(byte[] bytes, int offset, int length) {
        // all 0xFF is NULL
        if (length == 0) {
            return false;
        }
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i + offset] != AbstractRowKeyEncoder.DEFAULT_BLANK_BYTE) {
                return false;
            }
        }
        return true;
    }

    private byte[] removeFixLenPad(byte[] bytes, int offset, int length) {
        int padCount = 0;
        for (int i = 0; i < length; i++) {
            if (bytes[i + offset] == RowConstants.ROWKEY_PLACE_HOLDER_BYTE) {
                padCount++;
            }
        }

        int size = length - padCount;
        byte[] stripBytes = new byte[size];
        int index = 0;
        for (int i = 0; i < length; i++) {
            byte vb = bytes[i + offset];
            if (vb != RowConstants.ROWKEY_PLACE_HOLDER_BYTE) {
                stripBytes[index++] = vb;
            }
        }
        return stripBytes;
    }

}
