package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.TblColRef;

/**
 */
public class MapContextGTRecordWriter implements ICuboidWriter {

    private static final Log logger = LogFactory.getLog(MapContextGTRecordWriter.class);
    protected MapContext<?, ?, ByteArrayWritable, ByteArrayWritable> mapContext;
    private Long lastCuboidId;
    protected CubeSegment cubeSegment;
    protected CubeDesc cubeDesc;

    private int bytesLength;
    private int dimensions;
    private int measureCount;
    private byte[] keyBuf;
    private int[] measureColumnsIndex;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private ByteArrayWritable outputKey = new ByteArrayWritable();
    private ByteArrayWritable outputValue = new ByteArrayWritable();
    private long cuboidRowCount = 0;

    //for shard

    public MapContextGTRecordWriter(MapContext<?, ?, ByteArrayWritable, ByteArrayWritable> mapContext, CubeDesc cubeDesc, CubeSegment cubeSegment) {
        this.mapContext = mapContext;
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
        int header = RowConstants.ROWKEY_HEADER_LEN;
        int offSet = header;
        for (int x = 0; x < dimensions; x++) {
            System.arraycopy(record.get(x).array(), record.get(x).offset(), keyBuf, offSet, record.get(x).length());
            offSet += record.get(x).length();
        }

        //fill shard
        short cuboidShardNum = cubeSegment.getCuboidShardNum(cuboidId);
        short shardOffset = ShardingHash.getShard(keyBuf, header, offSet - header, cuboidShardNum);
        short cuboidShardBase = cubeSegment.getCuboidBaseShard(cuboidId);
        short finalShard = ShardingHash.normalize(cuboidShardBase, shardOffset, cubeSegment.getTotalShards());
        BytesUtil.writeShort(finalShard, keyBuf, 0, RowConstants.ROWKEY_SHARDID_LEN);

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

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        
    }

    private void initVariables(Long cuboidId) {
        bytesLength = RowConstants.ROWKEY_HEADER_LEN;
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        for (TblColRef column : cuboid.getColumns()) {
            bytesLength += cubeSegment.getColumnLength(column);
        }

        keyBuf = new byte[bytesLength];
        dimensions = BitSet.valueOf(new long[] { cuboidId }).cardinality();
        measureColumnsIndex = new int[measureCount];
        for (int i = 0; i < measureCount; i++) {
            measureColumnsIndex[i] = dimensions + i;
        }

        //write cuboid id first
        BytesUtil.writeLong(cuboidId, keyBuf, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
    }
}
