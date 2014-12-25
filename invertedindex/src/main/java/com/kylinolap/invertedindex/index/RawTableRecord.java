package com.kylinolap.invertedindex.index;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.metadata.measure.fixedlen.FixedLenMeasureCodec;
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


    protected boolean isMetric(int col) {
        return digest.isMetrics(col);
    }

    @SuppressWarnings("unchecked")
    protected FixedLenMeasureCodec<LongWritable> codec(int col) {
        return digest.codec(col);
    }

    protected int length(int col) {
        return digest.length(col);
    }

    protected int getColumnCount() {
        return digest.getColumnCount();
    }

    protected void setValueID(int col, int id) {
        BytesUtil.writeUnsigned(id, buf, digest.offset(col), digest.length(col));
    }

    protected int getValueID(int col) {
        return BytesUtil.readUnsigned(buf, digest.offset(col), digest.length(col));
    }

    protected void setValueMetrics(int col, LongWritable value) {
        digest.codec(col).write(value, buf, digest.offset(col));
    }

    protected LongWritable getValueMetrics(int col) {
        return digest.codec(col).read(buf, digest.offset(col));
    }

    public byte[] getBytes() {
        return buf;
    }

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
        //result = prime * result + ((digest == null) ? 0 : digest.hashCode());
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
