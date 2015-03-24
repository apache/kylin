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
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GridTable;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.cube.CubeStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

    private HCatSchema schema = null;

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private final LinkedBlockingDeque<Stream> queue = new LinkedBlockingDeque<Stream>();
    private CubeStreamBuilder streamBuilder = null;
    private Map<TblColRef, DictionaryInfo> dictionaryMap = null;
    private Map<Long, GridTable> cuboidsMap = null;
    private byte[] keyBuf = new byte[4096];
    private static byte[] ZERO_BYTES = Bytes.toBytes(0l);
    private Future<?> future;
    private Cuboid baseCuboid;
    private int nReducders;
    private int mapperTaskId;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();

        dictionaryMap = Maps.newHashMap();
        cuboidsMap = Maps.newHashMap();

        streamBuilder = new CubeStreamBuilder(queue, Integer.MAX_VALUE, cube, dictionaryMap, cuboidsMap);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        future = executorService.submit(streamBuilder);

        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        mapperTaskId = context.getTaskAttemptID().getTaskID().getId();
        nReducders = context.getNumReduceTasks();
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        queue.put(parse(record));
    }

    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {

        // end to trigger cubing calculation
        queue.put(new Stream(-1, null));
        try {
            future.get();
        } catch (Exception e) {
            logger.error("cube build failed", e);
            throw new IOException(e);
        }

        assert dictionaryMap.size() > 0;
        assert cuboidsMap.size() > 0;

        // output dictionary to reducer
        DictionaryInfoSerializer dictionaryInfoSerializer = new DictionaryInfoSerializer();
        int keyLength = 0;
        for (TblColRef col : dictionaryMap.keySet()) {
            //serialize the dictionary to bytes;
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            dictionaryInfoSerializer.serialize(dictionaryMap.get(col), dout);
            dout.close();
            buf.close();
            byte[] dictionaryBytes = buf.toByteArray();
            outputValue.set(dictionaryBytes, 0, dictionaryBytes.length);
            // to all reducers
            for (int i = 0; i < nReducders; i++) {
                keyLength = buildDictionaryKey(col, i);
                outputKey.set(keyBuf, 0, keyLength);
                context.write(outputKey, outputValue);
            }
        }


        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidsMap.keySet());
        Collections.sort(allCuboids);

        for (Long cuboid : allCuboids) {
            // output cuboids;
        }

       // outputValue.set(bytes, 0, bytes.length);
        //context.write(outputKey, outputValue);


    }

    private int buildDictionaryKey(TblColRef col, int reducer) {
        // the key format is: [0l][col-index][mapper-number][reducer-number]
        int offset = 0;

        // cuboid id, use 0 cuboid id for dictionary
        System.arraycopy(ZERO_BYTES, 0, keyBuf, offset, ZERO_BYTES.length);
        offset += ZERO_BYTES.length;

        // column index
        int indexOfCol = baseCuboid.getColumns().indexOf(col);
        System.arraycopy(Bytes.toBytes(indexOfCol), 0, keyBuf, offset, Bytes.toBytes(indexOfCol).length);
        offset += Bytes.toBytes(indexOfCol).length;

        // mapper task Id
        System.arraycopy(Bytes.toBytes(mapperTaskId), 0, keyBuf, offset, Bytes.toBytes(mapperTaskId).length);
        offset += Bytes.toBytes(mapperTaskId).length;

        // to which reducer
        System.arraycopy(Bytes.toBytes(reducer), 0, keyBuf, offset, Bytes.toBytes(reducer).length);
        offset += Bytes.toBytes(reducer).length;

        return offset;
    }

    private Stream parse(HCatRecord record) {
        return new Stream(System.currentTimeMillis(), StringUtils.join(HiveTableReader.getRowAsStringArray(record), ",").getBytes());
    }

}
