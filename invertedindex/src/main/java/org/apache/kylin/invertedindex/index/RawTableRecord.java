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

package org.apache.kylin.invertedindex.index;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;

/**
 * Created by honma on 11/10/14.
 */
public class RawTableRecord implements Cloneable {
    TableRecordInfoDigest digest;
    private byte[] buf; // consecutive column value IDs (encoded by dictionary)

    public RawTableRecord(TableRecordInfoDigest info) {
        this.digest = info;
        this.buf = new byte[info.getByteFormLen()];
        reset();
    }

    public RawTableRecord(RawTableRecord another) {
        this.digest = another.digest;
        this.buf = Bytes.copy(another.buf);
    }

    public void reset() {
        Arrays.fill(buf, Dictionary.NULL);
    }

    public boolean isMetric(int col) {
        return digest.isMetrics(col);
    }

    public FixedLenMeasureCodec<LongWritable> codec(int col) {
        return digest.codec(col);
    }

    public int length(int col) {
        return digest.length(col);
    }

    public int getColumnCount() {
        return digest.getColumnCount();
    }

    public void setValueID(int col, int id) {
        BytesUtil.writeUnsigned(id, buf, digest.offset(col), digest.length(col));
    }

    public int getValueID(int col) {
        return BytesUtil.readUnsigned(buf, digest.offset(col), digest.length(col));
    }

    public void setValueMetrics(int col, LongWritable value) {
        digest.codec(col).write(value, buf, digest.offset(col));
    }

    public String getValueMetric(int col) {
        digest.codec(col).read(buf, digest.offset(col));
        return (String) digest.codec(col).getValue();
    }

    public byte[] getBytes() {
        return buf;
    }

    //TODO is it possible to avoid copying?
    public void setBytes(byte[] bytes, int offset, int length) {
        assert buf.length == length;
        System.arraycopy(bytes, offset, buf, 0, length);
    }

    public void setValueBytes(int col, ImmutableBytesWritable bytes) {
        System.arraycopy(bytes.get(), bytes.getOffset(), buf, digest.offset(col), digest.length(col));
    }

    public void getValueBytes(int col, ImmutableBytesWritable bytes) {
        bytes.set(buf, digest.offset(col), digest.length(col));
    }


    @Override
    public Object clone() {
        return new RawTableRecord(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(buf);
        // result = prime * result + ((digest == null) ? 0 : digest.hashCode());
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
        RawTableRecord other = (RawTableRecord) obj;
        if (!Arrays.equals(buf, other.buf))
            return false;
        return true;
    }
}
