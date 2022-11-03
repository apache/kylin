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

package org.apache.kylin.common.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
abstract public class Dictionary<T> implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Dictionary.class);

    private static final long serialVersionUID = 1L;

    // ID with all bit-1 (0xff e.g.) reserved for NULL value
    public static final int[] NULL_ID = new int[] { 0, 0xff, 0xffff, 0xffffff, 0xffffffff };

    abstract public int getMinId();

    abstract public int getMaxId();

    public int getSize() {
        return getMaxId() - getMinId() + 1;
    }

    /**
     * @return the size of an ID in bytes, determined by the cardinality of column
     */
    abstract public int getSizeOfId();

    /**
     * @return the (maximum) size of value in bytes, determined by the longest value
     */
    abstract public int getSizeOfValue();

    /**
     * @return true if each entry of this dict is contained by the dict in param
     */
    abstract public boolean contains(Dictionary<?> another);

    /**
     * Convenient form of <code>getIdFromValue(value, 0)</code>
     */
    final public int getIdFromValue(T value) throws IllegalArgumentException {
        return getIdFromValue(value, 0);
    }

    /**
     * Returns the ID integer of given value. In case of not found
     * <p>
     * - if roundingFlag=0, throw IllegalArgumentException; <br>
     * - if roundingFlag<0, the closest smaller ID integer if exist; <br>
     * - if roundingFlag>0, the closest bigger ID integer if exist. <br>
     * <p>
     * The implementation often has cache, thus faster than the byte[] version getIdFromValueBytes()
     * 
     * @throws IllegalArgumentException
     *             if value is not found in dictionary and rounding is off;
     *             or if rounding cannot find a smaller or bigger ID
     */
    public int getIdFromValue(T value, int roundingFlag) throws IllegalArgumentException {
        if (isNullObjectForm(value))
            return nullId();

        int id = getIdFromValueImpl(value, roundingFlag);
        if (id == -1) {
            throw new IllegalArgumentException("Value : " + value + " not exists");
        }
        return id;
    }

    final public boolean containsValue(T value) throws IllegalArgumentException {
        if (isNullObjectForm(value)) {
            return true;
        } else {
            try {
                //if no key found, it will throw exception
                getIdFromValueImpl(value, 0);
            } catch (IllegalArgumentException e) {
                return false;
            }
            return true;
        }
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
    public T getValueFromId(int id) throws IllegalArgumentException {
        if (isNullId(id))
            return null;
        else
            return getValueFromIdImpl(id);
    }

    /**
     * @return the value bytes corresponds to the given ID
     * @throws IllegalArgumentException
     *             if ID is not found in dictionary
     */
    final public byte[] getValueByteFromId(int id) throws IllegalArgumentException {
        if (isNullId(id))
            return null;
        else
            return getValueBytesFromIdImpl(id);
    }

    protected int cacheHitCount = 0;
    protected int cacheMissCount = 0;

    protected byte[] getValueBytesFromIdImpl(int id) {
        throw new UnsupportedOperationException();

    }

    public void printlnStatistics() {
        logger.info("cache hit count: " + cacheHitCount);
        logger.info("cache miss count: " + cacheMissCount);
        logger.info("cache hit percent: " + cacheHitCount * 1.0 / (cacheMissCount + cacheHitCount));
        cacheHitCount = 0;
        cacheMissCount = 0;
    }

    abstract protected T getValueFromIdImpl(int id);

    abstract public void dump(PrintStream out);

    public int nullId() {
        return NULL_ID[getSizeOfId()];
    }

    public boolean isNullId(int id) {
        int nullId = NULL_ID[getSizeOfId()];
        return (nullId & id) == nullId;
    }

    // Some dict need updated when copy from one metadata environment to another
    public Dictionary copyToAnotherMeta(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {
        return this;
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

    /** 
     * Serialize the fields of this object to <code>out</code>.
     * 
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    public abstract void write(DataOutput out) throws IOException;

    /** 
     * Deserialize the fields of this object from <code>in</code>.  
     * 
     * <p>For efficiency, implementations should attempt to re-use storage in the 
     * existing object where possible.</p>
     * 
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    public abstract void readFields(DataInput in) throws IOException;

}
