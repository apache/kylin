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

package com.kylinolap.cube.invertedindex;

import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.dict.DateStrDictionary;
import com.kylinolap.dict.Dictionary;

/**
 * @author yangli9
 * 
 */
public class TableRecord implements Cloneable {

    TableRecordInfo info;
    byte[] buf; // consecutive column value IDs (encoded by dictionary)

    public TableRecord(TableRecordInfo info) {
        this.info = info;
        this.buf = new byte[info.byteFormLen];
        reset();
    }

    public TableRecord(TableRecord another) {
        this.info = another.info;
        this.buf = Bytes.copy(another.buf);
    }

    public byte[] getBytes() {
        return buf;
    }

    public void setBytes(byte[] bytes, int offset, int length) {
        assert buf.length == length;
        System.arraycopy(bytes, offset, buf, 0, length);
    }

    public void reset() {
        Arrays.fill(buf, Dictionary.NULL);
    }

    public long getTimestamp() {
        String str = getValueString(info.getTimestampColumn());
        return DateStrDictionary.stringToMillis(str);
    }

    public int length(int col) {
        return info.length(col);
    }

    public void setValueString(int col, String value) {
        if (info.isMetrics(col)) {
            LongWritable v = info.codec(col).valueOf(value);
            setValueMetrics(col, v);
        } else {
            int id = info.dict(col).getIdFromValue(value);
            setValueID(col, id);
        }
    }

    public String getValueString(int col) {
        if (info.isMetrics(col))
            return info.codec(col).toString(getValueMetrics(col));
        else
            return info.dict(col).getValueFromId(getValueID(col));
    }

    public void setValueBytes(int col, ImmutableBytesWritable bytes) {
        System.arraycopy(bytes.get(), bytes.getOffset(), buf, info.offset(col), info.length(col));
    }
    
    public void getValueBytes(int col, ImmutableBytesWritable bytes) {
        bytes.set(buf, info.offset(col), info.length(col));
    }
    
    private void setValueID(int col, int id) {
        BytesUtil.writeUnsigned(id, buf, info.offset(col), info.length(col));
    }
    
    private int getValueID(int col) {
        return BytesUtil.readUnsigned(buf, info.offset(col), info.length(col));
    }
    
    private void setValueMetrics(int col, LongWritable value) {
        info.codec(col).write(value, buf, info.offset(col));
    }

    private LongWritable getValueMetrics(int col) {
        return info.codec(col).read(buf, info.offset(col));
    }

    public short getShard() {
        int timestampID = getValueID(info.getTimestampColumn());
        return (short) (Math.abs(ShardingHash.hashInt(timestampID)) % info.getDescriptor().getSharding());
    }

    public TableRecordInfo info() {
        return info;
    }

    @Override
    public Object clone() {
        return new TableRecord(this);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("[");
        for (int col = 0; col < info.getColumnCount(); col++) {
            if (col > 0)
                buf.append(",");
            buf.append(getValueString(col));
        }
        buf.append("]");
        return buf.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(buf);
        result = prime * result + ((info == null) ? 0 : info.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableRecord other = (TableRecord) obj;
        if (!Arrays.equals(buf, other.buf))
            return false;
        if (info == null) {
            if (other.info != null)
                return false;
        } else if (!info.equals(other.info))
            return false;
        return true;
    }

}
