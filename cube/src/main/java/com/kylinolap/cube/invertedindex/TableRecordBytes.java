package com.kylinolap.cube.invertedindex;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.dict.Dictionary;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Created by honma on 11/10/14.
 */
public class TableRecordBytes implements Cloneable {
    TableRecordInfoDigest info;
    byte[] buf; // consecutive column value IDs (encoded by dictionary)

    public TableRecordBytes() {
    }

    public void setValueID(int col, int id) {
        BytesUtil.writeUnsigned(id, buf, info.offset(col), info.length(col));
    }

    public int getValueID(int col) {
        return BytesUtil.readUnsigned(buf, info.offset(col), info.length(col));
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

    public TableRecordBytes(TableRecordInfoDigest info) {
        this.info = info;
        this.buf = new byte[info.byteFormLen];
        reset();
    }

    public TableRecordBytes(TableRecordBytes another) {
        this.info = another.info;
        this.buf = Bytes.copy(another.buf);
    }

    @Override
    public Object clone() {
        return new TableRecordBytes(this);
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
