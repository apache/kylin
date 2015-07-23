package org.apache.kylin.storage.hbase.steps;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;

import com.google.common.collect.Lists;

public class InMemKeyValueCreator {
    byte[] cfBytes;
    byte[] qBytes;
    long timestamp;


    MeasureCodec codec;
    Object[] colValues;
    ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    int startPosition = 0;

    public InMemKeyValueCreator(HBaseColumnDesc colDesc, int startPosition) {

        cfBytes = Bytes.toBytes(colDesc.getColumnFamilyName());
        qBytes = Bytes.toBytes(colDesc.getQualifier());
        timestamp = System.currentTimeMillis();

        List<MeasureDesc> measures = Lists.newArrayList();
        for (MeasureDesc measure : colDesc.getMeasures()) {
            measures.add(measure);
        }
        codec = new MeasureCodec(measures);
        colValues = new Object[measures.size()];

        this.startPosition = startPosition;

    }

    public KeyValue create(Text key, Object[] measureValues) {
        return create(key.getBytes(), 0, key.getLength(), measureValues);
    }

    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, Object[] measureValues) {
        for (int i = 0; i < colValues.length; i++) {
            colValues[i] = measureValues[startPosition + i];
        }

        valueBuf.clear();
        codec.encode(colValues, valueBuf);

        return create(keyBytes, keyOffset, keyLength, valueBuf.array(), 0, valueBuf.position());
    }


    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, byte[] value, int voffset, int vlen) {
        return new KeyValue(keyBytes, keyOffset, keyLength, //
                cfBytes, 0, cfBytes.length, //
                qBytes, 0, qBytes.length, //
                timestamp, KeyValue.Type.Put, //
                value, voffset, vlen);
    }

    public KeyValue create(Text key, byte[] value, int voffset, int vlen) {
        return create(key.getBytes(), 0, key.getLength(), value, voffset, vlen);
    }

}
