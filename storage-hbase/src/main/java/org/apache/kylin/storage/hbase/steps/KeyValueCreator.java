package org.apache.kylin.storage.hbase.steps;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * @author George Song (ysong1)
 */
public class KeyValueCreator {
    byte[] cfBytes;
    byte[] qBytes;
    long timestamp;

    int[] refIndex;
    MeasureDesc[] refMeasures;

    MeasureCodec codec;
    Object[] colValues;
    ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    public boolean isFullCopy;

    public KeyValueCreator(CubeDesc cubeDesc, HBaseColumnDesc colDesc) {

        cfBytes = Bytes.toBytes(colDesc.getColumnFamilyName());
        qBytes = Bytes.toBytes(colDesc.getQualifier());
        timestamp = 0; // use 0 for timestamp

        refIndex = colDesc.getMeasureIndex();
        refMeasures = colDesc.getMeasures();

        codec = new MeasureCodec(refMeasures);
        colValues = new Object[refMeasures.length];

        isFullCopy = true;
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        for (int i = 0; i < measures.size(); i++) {
            if (refIndex.length <= i || refIndex[i] != i)
                isFullCopy = false;
        }
    }

    public KeyValue create(Text key, Object[] measureValues) {
        return create(key.getBytes(), 0, key.getLength(), measureValues);
    }

    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, Object[] measureValues) {
        for (int i = 0; i < colValues.length; i++) {
            colValues[i] = measureValues[refIndex[i]];
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
