package org.apache.kylin.engine.mr.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.gridtable.GTRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public abstract class KVGTRecordWriter implements ICuboidWriter {

    private static final Log logger = LogFactory.getLog(KVGTRecordWriter.class);
    private Long lastCuboidId;
    protected CubeSegment cubeSegment;
    protected CubeDesc cubeDesc;

    private AbstractRowKeyEncoder rowKeyEncoder;
    private int dimensions;
    private int measureCount;
    private byte[] keyBuf;
    private int[] measureColumnsIndex;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private ByteArrayWritable outputKey = new ByteArrayWritable();
    private ByteArrayWritable outputValue = new ByteArrayWritable();
    private long cuboidRowCount = 0;

    //for shard

    public KVGTRecordWriter(CubeDesc cubeDesc, CubeSegment cubeSegment) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.measureCount = cubeDesc.getMeasures().size();
    }

    @Override
    public void write(long cuboidId, GTRecord record) throws IOException {

        if (lastCuboidId == null || !lastCuboidId.equals(cuboidId)) {
            if (lastCuboidId != null) {
                logger.info("Cuboid " + lastCuboidId + " has " + cuboidRowCount + " rows");
                cuboidRowCount = 0;
            }
            // output another cuboid
            initVariables(cuboidId);
            lastCuboidId = cuboidId;
        }

        cuboidRowCount++;
        rowKeyEncoder.encode(record, record.getInfo().getPrimaryKey(), keyBuf);

        //output measures
        valueBuf.clear();
        record.exportColumns(measureColumnsIndex, valueBuf);

        outputKey.set(keyBuf, 0, keyBuf.length);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        writeAsKeyValue(outputKey, outputValue);
    }

    protected abstract void writeAsKeyValue(ByteArrayWritable key, ByteArrayWritable value) throws IOException;

    private void initVariables(Long cuboidId) {
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, Cuboid.findById(cubeDesc, cuboidId));
        keyBuf = rowKeyEncoder.createBuf();

        dimensions = Long.bitCount(cuboidId);
        measureColumnsIndex = new int[measureCount];
        for (int i = 0; i < measureCount; i++) {
            measureColumnsIndex[i] = dimensions + i;
        }
    }
}
