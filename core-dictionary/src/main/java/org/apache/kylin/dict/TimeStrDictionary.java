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

package org.apache.kylin.dict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Dictionary;

/**
 */
@SuppressWarnings("serial")
public class TimeStrDictionary extends Dictionary<String> {

    // Integer.MAX_VALUE - 1 to avoid cardinality (max_id - min_id + 1) overflow
    private static final int MAX_ID = Integer.MAX_VALUE - 1;
    private static final int MAX_LENGTH_OF_POSITIVE_LONG = 19;

    @Override
    public int getMinId() {
        return 0;
    }

    @Override
    public int getMaxId() {
        return MAX_ID;
    }

    @Override
    public int getSizeOfId() {
        return 4;
    }

    @Override
    public int getSizeOfValue() {
        return MAX_LENGTH_OF_POSITIVE_LONG;
    }

    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        long millis = DateFormat.stringToMillis(value);
        long seconds = millis / 1000;

        if (seconds > MAX_ID) {
            return nullId();
        } else if (seconds < 0) {
            throw new IllegalArgumentException("Illegal value: " + value + ", parsed seconds: " + seconds);
        }

        return (int) seconds;
    }

    /**
     *
     * @param id
     * @return return like "0000001430812800000"
     */
    @Override
    protected String getValueFromIdImpl(int id) {
        if (id == nullId())
            return null;

        long millis = 1000L * id;
        return DateFormat.formatToTimeWithoutMilliStr(millis);
    }

    @Override
    final protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        try {
            return getIdFromValue(new String(value, offset, len, "ISO-8859-1"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
    }

    @Override
    final protected byte[] getValueBytesFromIdImpl(int id) {
        String date = getValueFromId(id);
        byte[] bytes;
        try {
            bytes = date.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e); // never happen
        }
        return bytes;
    }

    @Override
    final protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        byte[] bytes = getValueBytesFromIdImpl(id);
        System.arraycopy(bytes, 0, returnValue, offset, bytes.length);
        return bytes.length;
    }

    @Override
    public void dump(PrintStream out) {
        out.println(this.toString());
    }

    @Override
    public String toString() {
        return "TimeStrDictionary supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07 (does not support millisecond)";
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;

        return o instanceof TimeStrDictionary;
    }

    @Override
    public boolean contains(Dictionary<?> other) {
        return this.equals(other);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
}
