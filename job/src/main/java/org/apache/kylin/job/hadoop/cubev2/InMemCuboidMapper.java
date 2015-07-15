package org.apache.kylin.job.hadoop.cubev2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRBatchCubingEngine;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;

/**
 */
public class InMemCuboidMapper<KEYIN> extends KylinMapper<KEYIN, Object, ImmutableBytesWritable, Text> {

    private static final Log logger = LogFactory.getLog(InMemCuboidMapper.class);
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private IMRTableInputFormat flatTableInputFormat;

    private int counter;
    private BlockingQueue<List<String>> queue = new ArrayBlockingQueue<List<String>>(10000);
    private Future<?> future;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        flatTableInputFormat = MRBatchCubingEngine.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();

        Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();

        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            // dictionary
            for (TblColRef col : dim.getColumnRefs()) {
                if (cubeDesc.getRowkey().isUseDictionary(col)) {
                    Dictionary<?> dict = cubeSegment.getDictionary(col);
                    if (dict == null) {
                        logger.warn("Dictionary for " + col + " was not found.");
                    }

                    dictionaryMap.put(col, cubeSegment.getDictionary(col));
                }
            }
        }

        DoggedCubeBuilder cubeBuilder = new DoggedCubeBuilder(cube.getDescriptor(), dictionaryMap);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        future = executorService.submit(cubeBuilder.buildAsRunnable(queue, new MapContextGTRecordWriter(context, cubeDesc, cubeSegment)));

    }

    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        String[] row = flatTableInputFormat.parseMapperInput(record);
        List<String> rowAsList = Arrays.asList(row);
        
        while (!future.isDone()) {
            if (queue.offer(rowAsList, 1, TimeUnit.SECONDS)) {
                counter++;
                if (counter % BatchConstants.COUNTER_MAX == 0) {
                    logger.info("Handled " + counter + " records!");
                }
                break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("Totally handled " + counter + " records!");

        while (!future.isDone()) {
            if (queue.offer(new ArrayList<String>(0), 1, TimeUnit.SECONDS)) {
                break;
            }
        }
        
        try {
            future.get();
        } catch (Exception e) {
            throw new IOException("Failed to build cube in mapper " + context.getTaskAttemptID().getTaskID().getId(), e);
        }
        queue.clear();
    }

}
