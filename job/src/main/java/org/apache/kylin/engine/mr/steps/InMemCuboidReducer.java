package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMROutput2.IMRStorageOutputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class InMemCuboidReducer extends KylinReducer<ByteArrayWritable, ByteArrayWritable, Object, Object> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidReducer.class);

    private IMRStorageOutputFormat storageOutputFormat;
    private MeasureCodec codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        
        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        boolean isMerge = Boolean.parseBoolean(context.getConfiguration().get(BatchConstants.CFG_IS_MERGE));

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeSegment cubeSeg = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        if (isMerge)
            storageOutputFormat = MRUtil.getBatchMergeOutputSide2(cubeSeg).getStorageOutputFormat();
        else
            storageOutputFormat = MRUtil.getBatchCubingOutputSide2(cubeSeg).getStorageOutputFormat();

        List<MeasureDesc> measuresDescs = Lists.newArrayList();
        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                for (MeasureDesc measure : colDesc.getMeasures()) {
                    measuresDescs.add(measure);
                }
            }
        }

        codec = new MeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);

        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];
    }

    @Override
    public void reduce(ByteArrayWritable key, Iterable<ByteArrayWritable> values, Context context) throws IOException, InterruptedException {

        aggs.reset();

        for (ByteArrayWritable value : values) {
            codec.decode(value.asBuffer(), input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);
        
        storageOutputFormat.doReducerOutput(key, result, context);
        
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }


}
