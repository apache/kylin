package org.apache.kylin.job.hadoop.cubev2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Created by shaoshi on 4/7/15.
 */
public class MapContextGTRecordWriter implements IGTRecordWriter {

    private static final Log logger = LogFactory.getLog(MapContextGTRecordWriter.class);
    protected MapContext mapContext;
    private Long lastCuboidId;
    protected CubeSegment cubeSegment;
    protected CubeDesc cubeDesc;

    private int bytesLength;
    private int dimensions;
    private byte[] keyBuf;
    private int[] measureColumnsIndex;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
    private Text outputValue = new Text();
    long cuboidRowCount = 0;

    public MapContextGTRecordWriter(MapContext mapContext, CubeDesc cubeDesc, CubeSegment cubeSegment) {
        this.mapContext = mapContext;
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;

    }

    @Override
    public void write(Long cuboidId, GTRecord record) throws IOException {

        if (lastCuboidId == null || !lastCuboidId.equals(cuboidId)) {
            // output another cuboid
            initVariables(cuboidId);
            if(lastCuboidId != null) {
                logger.info("Cuboid " + lastCuboidId + " has " + cuboidRowCount + " rows");
                cuboidRowCount = 0;
            }
        }

        cuboidRowCount++;
        int offSet = RowConstants.ROWKEY_CUBOIDID_LEN;
        for (int x = 0; x < dimensions; x++) {
            System.arraycopy(record.get(x).array(), record.get(x).offset(), keyBuf, offSet, record.get(x).length());
            offSet += record.get(x).length();
        }

        //output measures
        valueBuf.clear();
        record.exportColumns(measureColumnsIndex, valueBuf);

        outputKey.set(keyBuf, 0, offSet);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        try {
            mapContext.write(outputKey, outputValue);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initVariables(Long cuboidId) {
        bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        for (TblColRef column : cuboid.getColumns()) {
            bytesLength += cubeSegment.getColumnLength(column);
        }

        keyBuf = new byte[bytesLength];
        dimensions = BitSet.valueOf(new long[]{cuboidId}).cardinality();
        measureColumnsIndex = new int[cubeDesc.getMeasures().size()];
        for(int i =0; i< cubeDesc.getMeasures().size(); i++) {
            measureColumnsIndex[i] = dimensions + i;
        }

        System.arraycopy(Bytes.toBytes(cuboidId), 0, keyBuf, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
    }
}
