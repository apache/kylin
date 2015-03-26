package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
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
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.cube.CubeStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by shaoshi on 3/24/15.
 */
public class InMemCuboidMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidMapper.class);
    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;

    private HCatSchema schema = null;

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private final LinkedBlockingDeque<Stream> queue = new LinkedBlockingDeque<Stream>();
    private CubeStreamBuilder streamBuilder = null;
    private Map<TblColRef, Dictionary> dictionaryMap = null;
    private Map<Long, GridTable> cuboidsMap = null; // key: cuboid id; value: grid table;
    private Future<?> future;
    private Cuboid baseCuboid;
    private int mapperTaskId;
    private int counter;

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private Object[] measures;
    private MeasureCodec measureCodec;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        dictionaryMap = Maps.newHashMap();
        cuboidsMap = Maps.newHashMap();

        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (cubeDesc.getRowkey().isUseDictionary(col)) {
                    Dictionary dict = cubeSegment.getDictionary(col);
                    if (dict == null) {
                        throw new IllegalArgumentException("Dictionary for " + col + " was not found.");
                    }

                    dictionaryMap.put(col, cubeSegment.getDictionary(col));
                }
            }
        }

        streamBuilder = new CubeStreamBuilder(queue, Integer.MAX_VALUE, cube, false, dictionaryMap, cuboidsMap);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        future = executorService.submit(streamBuilder);

        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        mapperTaskId = context.getTaskAttemptID().getTaskID().getId();

        measures = new Object[cubeDesc.getMeasures().size()];
        measureCodec = new MeasureCodec(cubeDesc.getMeasures());
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        queue.put(parse(record));

        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }

    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {

        // end to trigger cubing calculation
        logger.info("Totally read " + counter + " rows in memory, trigger cube build now.");
        queue.put(new Stream(-1, null));
        try {
            future.get();
        } catch (Exception e) {
            logger.error("cube build failed", e);
            throw new IOException(e);
        }
        logger.info("Cube build success");
        logger.info("Cube segment calculation in mapper " + mapperTaskId + " finished; cuboid number: " + cuboidsMap.size());
        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidsMap.keySet());
        Collections.sort(allCuboids);


        for (Long cuboidId : allCuboids) {
            int bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;
            Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
            RowKeyColumnIO colIO = new RowKeyColumnIO(this.cubeSegment);
            for (TblColRef column : cuboid.getColumns()) {
                bytesLength += colIO.getColumnLength(column);
            }

            logger.info("The keyBuf length is " + bytesLength);
            byte[] keyBuf = new byte[bytesLength];
            // output cuboids;
            int dimensions = BitSet.valueOf(new long[]{cuboidId}).cardinality();
            logger.info("Output cuboid " + cuboidId + " to reducer, dimension number is " + dimensions);
            long cuboidRowCount = 0;
            System.arraycopy(Bytes.toBytes(cuboidId), 0, keyBuf, 0, Bytes.toBytes(cuboidId).length);

            GridTable gt = cuboidsMap.get(cuboidId);
            GTScanRequest req = new GTScanRequest(gt.getInfo(), null, null, null, null);
            IGTScanner scanner = gt.scan(req);
            int offSet = 0;
            for (GTRecord record : scanner) {
                cuboidRowCount++;
                offSet = RowConstants.ROWKEY_CUBOIDID_LEN;
                for (int x = 0; x < dimensions; x++) {
                    logger.info("Copy key with offSet: " + offSet + ", length " + record.get(x).length());
                    System.arraycopy(record.get(x).array(), record.get(x).offset(), keyBuf, offSet, record.get(x).length());
                    offSet += record.get(x).length();
                }

                /*
                for (int i = 0; i < measures.length; i++) {
                    valueBuf.put(record.get(dimensions + i).array());
                }

                */
                Object[] values = record.getValues();
                for (int i = 0; i < measures.length; i++) {
                    measures[i] = values[dimensions + i];
                }

                valueBuf.clear();
                measureCodec.encode(measures, valueBuf);

                outputKey.set(keyBuf, 0, offSet); // key is cuboid-id + rowkey bytes
                outputValue.set(valueBuf.array(), 0, valueBuf.position());
                context.write(outputKey, outputValue);
            }
            logger.info("Cuboid " + cuboid + " has " + cuboidRowCount + " rows on mapper " + this.mapperTaskId);
        }


    }

    private Stream parse(HCatRecord record) {
        return new Stream(System.currentTimeMillis(), StringUtils.join(HiveTableReader.getRowAsStringArray(record), ",").getBytes());
    }

}
