package org.apache.kylin.job.hadoop.cubev2;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by shaoshi on 3/25/15.
 */
public class InMemCuboidReducer extends KylinReducer<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidReducer.class);

    private String cubeName;
    private CubeDesc cubeDesc;
    private List<MeasureDesc> measuresDescs;

    private MeasureCodec codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeDesc = CubeManager.getInstance(config).getCube(cubeName).getDescriptor();
        measuresDescs = cubeDesc.getMeasures();

        codec = new MeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);

        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        aggs.reset();

        for (Text value : values) {
            codec.decode(value, input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);

        valueBuf.clear();
        codec.encode(result, valueBuf);

        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(key, outputValue);

        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }

}
