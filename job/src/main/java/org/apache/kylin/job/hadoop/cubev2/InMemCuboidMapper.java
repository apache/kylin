package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.GridTable;
import org.apache.kylin.storage.gridtable.IGTScanner;
import org.apache.kylin.streaming.cube.CubeStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by shaoshi on 3/24/15.
 */
public class InMemCuboidMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, ImmutableBytesWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidMapper.class);
    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;

    private int mapperTaskId;
    private int counter;

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private Object[] measures;
    private MeasureCodec measureCodec;

    private List<List<String>> table;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        mapperTaskId = context.getTaskAttemptID().getTaskID().getId();

        measures = new Object[cubeDesc.getMeasures().size()];
        measureCodec = new MeasureCodec(cubeDesc.getMeasures());

        table = Lists.newArrayList();
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        List<String> row = HiveTableReader.getRowAsList(record);
        table.add(row);
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // end to trigger cubing calculation
        logger.info("Totally read " + counter + " rows in memory, trigger cube build now.");
        Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();
        Map<Long, GridTable> cuboidsMap = Maps.newHashMap();

        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (cubeDesc.getRowkey().isUseDictionary(col)) {
                    Dictionary<?> dict = cubeSegment.getDictionary(col);
                    if (dict == null) {
                        throw new IllegalArgumentException("Dictionary for " + col + " was not found.");
                    }

                    dictionaryMap.put(col, cubeSegment.getDictionary(col));
                }
            }
        }

        CubeStreamBuilder streamBuilder = new CubeStreamBuilder(cube, false, dictionaryMap, cuboidsMap);
        streamBuilder.build(table);
        logger.info("Cube build success");
        logger.info("Cube segment calculation in mapper " + mapperTaskId + " finished; cuboid number: " + cuboidsMap.size());
        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidsMap.keySet());
        Collections.sort(allCuboids);


        ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        Text outputValue = new Text();
        int offSet;
        RowKeyColumnIO colIO = new RowKeyColumnIO(this.cubeSegment);
        for (Long cuboidId : allCuboids) {
            int bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;
            Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
            for (TblColRef column : cuboid.getColumns()) {
                bytesLength += colIO.getColumnLength(column);
            }

            // output cuboids;
            int dimensions = BitSet.valueOf(new long[]{cuboidId}).cardinality();
            long cuboidRowCount = 0;
            byte[] keyBuf = new byte[bytesLength];
            System.arraycopy(Bytes.toBytes(cuboidId), 0, keyBuf, 0, RowConstants.ROWKEY_CUBOIDID_LEN);

            GridTable gt = cuboidsMap.get(cuboidId);
            GTScanRequest req = new GTScanRequest(gt.getInfo());
            IGTScanner scanner = gt.scan(req);
            for (GTRecord record : scanner) {
                cuboidRowCount++;
                offSet = RowConstants.ROWKEY_CUBOIDID_LEN;
                for (int x = 0; x < dimensions; x++) {
                    System.arraycopy(record.get(x).array(), record.get(x).offset(), keyBuf, offSet, record.get(x).length());
                    offSet += record.get(x).length();
                }

                //TODO use GTRecord.exportColumnBlock to gain better performance

                /*
                for (int i = 0; i < measures.length; i++) {
                    valueBuf.put(record.get(dimensions + i).array());
                }

                */
                Object[] values = record.getValues();
                System.arraycopy(values, dimensions, measures, 0, measures.length);

                valueBuf.clear();
                measureCodec.encode(measures, valueBuf);

                outputKey.set(keyBuf, 0, offSet); // key is cuboid-id + rowkey bytes
                outputValue.set(valueBuf.array(), 0, valueBuf.position());
                context.write(outputKey, outputValue);
            }
            logger.info("Cuboid " + cuboid + " has " + cuboidRowCount + " rows on mapper " + this.mapperTaskId);
        }

        table.clear();

    }


}
