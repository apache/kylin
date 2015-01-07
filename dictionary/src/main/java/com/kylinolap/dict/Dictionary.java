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
package com.kylinolap.dict;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Writable;

import com.kylinolap.common.util.BytesUtil;

/**
 * A bi-way dictionary that maps from dimension/column values to IDs and vice
 * versa. By storing IDs instead of real values, the size of cube is
 * significantly reduced.
 * 
 * - IDs are smallest integers possible for the cardinality of a column, for the
 * purpose of minimal storage space - IDs preserve ordering of values, such that
 * range query can be applied to IDs directly
 * 
 * A dictionary once built, is immutable. This allows optimal memory footprint
 * by e.g. flatten the Trie structure into a byte array, replacing node pointers
 * with array offsets.
 * 
 * @author yangli9
 */
abstract public class Dictionary<T> implements Writable {

    public static final byte NULL = (byte) 0xff;

    // ID with all bit-1 (0xff e.g.) reserved for NULL value
    public static final int NULL_ID[] = new int[] { 0, 0xff, 0xffff, 0xffffff, 0xffffff };

    abstract public int getMinId();

    abstract public int getMaxId();

    /**
     * @return the size of an ID in bytes, determined by the cardinality of
     *         column
     */
    abstract public int getSizeOfId();

    /**
     * @return the (maximum) size of value in bytes, determined by the longest
     *         value of column
     */
    abstract public int getSizeOfValue();
    
    /**
     * Convenient form of <code>getIdFromValue(value, 0)</code>
     */
    final public int getIdFromValue(T value) {
        return getIdFromValue(value, 0);
    }

    /**
     * Returns the ID integer of given value. In case of not found - if
     * roundingFlag=0, throw IllegalArgumentException; - if roundingFlag<0, the
     * closest smaller ID integer if exist; - if roundingFlag>0, the closest
     * bigger ID integer if exist. The implementation often has cache, thus
     * faster than the byte[] version getIdFromValueBytes()
     * 
     * @throws IllegalArgumentException
     *             if value is not found in dictionary and rounding is off or
     *             failed
     */
    final public int getIdFromValue(T value, int roundingFlag) {
        if (isNullObjectForm(value))
            return nullId();
        else
            return getIdFromValueImpl(value, roundingFlag);
    }

    protected boolean isNullObjectForm(T value) {
        return value == null;
    }

    abstract protected int getIdFromValueImpl(T value, int roundingFlag);

    /**
     * @return the value corresponds to the given ID
     * @throws IllegalArgumentException
     *             if ID is not found in dictionary
     */
    final public T getValueFromId(int id) {
        if (isNullId(id))
            return null;
        else
            return getValueFromIdImpl(id);
    }

    abstract protected T getValueFromIdImpl(int id);

    /**
     * Convenient form of
     * <code>getIdFromValueBytes(value, offset, len, 0)</code>
     */
    final public int getIdFromValueBytes(byte[] value, int offset, int len) {
        return getIdFromValueBytes(value, offset, len, 0);
    }

    /**
     * A lower level API, return ID integer from raw value bytes. In case of not
     * found - if roundingFlag=0, throw IllegalArgumentException; - if
     * roundingFlag<0, the closest smaller ID integer if exist; - if
     * roundingFlag>0, the closest bigger ID integer if exist. Bypassing the
     * cache layer, this could be significantly slower than getIdFromValue(T
     * value).
     * 
     * @throws IllegalArgumentException
     *             if value is not found in dictionary and rounding is off or
     *             failed
     */
    final public int getIdFromValueBytes(byte[] value, int offset, int len, int roundingFlag) {
        if (isNullByteForm(value, offset, len))
            return nullId();
        else
            return getIdFromValueBytesImpl(value, offset, len, roundingFlag);
    }
    
    protected boolean isNullByteForm(byte[] value, int offset, int len) {
        return value == null;
    }

    abstract protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag);

    /**
     * A lower level API, get byte values from ID, return the number of bytes
     * written. Bypassing the cache layer, this could be significantly slower
     * than getIdFromValue(T value).
     * 
     * @throws IllegalArgumentException
     *             if ID is not found in dictionary
     */
    final public int getValueBytesFromId(int id, byte[] returnValue, int offset) {
        if (isNullId(id))
            return 0;
        else
            return getValueBytesFromIdImpl(id, returnValue, offset);
    }

    abstract protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset);

    abstract public void dump(PrintStream out);

    public int nullId() {
        return NULL_ID[getSizeOfId()];
    }

    public boolean isNullId(int id) {
        int nullId = NULL_ID[getSizeOfId()];
        return (nullId & id) == nullId;
    }

    /** utility that converts a dictionary ID to string, preserving order */
    public static String dictIdToString(byte[] idBytes, int offset, int length) {
        try {
            return new String(idBytes, offset, length, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            // never happen
            return null;
        }
    }

    /** the reverse of dictIdToString(), returns integer ID */
    public static int stringToDictId(String str) {
        try {
            byte[] bytes = str.getBytes("ISO-8859-1");
            return BytesUtil.readUnsigned(bytes, 0, bytes.length);
        } catch (UnsupportedEncodingException e) {
            // never happen
            return 0;
        }
    }
}
