package org.apache.kylin.job.hadoop.cube;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

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
        timestamp = System.currentTimeMillis();

        List<MeasureDesc> measures = cubeDesc.getMeasures();
        String[] measureNames = getMeasureNames(cubeDesc);
        String[] refs = colDesc.getMeasureRefs();

        refIndex = new int[refs.length];
        refMeasures = new MeasureDesc[refs.length];
        for (int i = 0; i < refs.length; i++) {
            refIndex[i] = indexOf(measureNames, refs[i]);
            refMeasures[i] = measures.get(refIndex[i]);
        }

        codec = new MeasureCodec(refMeasures);
        colValues = new Object[refs.length];

        isFullCopy = true;
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

    private int indexOf(String[] measureNames, String ref) {
        for (int i = 0; i < measureNames.length; i++)
            if (measureNames[i].equalsIgnoreCase(ref))
                return i;

        throw new IllegalArgumentException("Measure '" + ref + "' not found in " + Arrays.toString(measureNames));
    }

    private String[] getMeasureNames(CubeDesc cubeDesc) {
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        String[] result = new String[measures.size()];
        for (int i = 0; i < measures.size(); i++)
            result[i] = measures.get(i).getName();
        return result;
    }

}
