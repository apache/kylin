package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.hadoop.cube.KeyValueCreator;
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
public class InMemCuboidReducer extends KylinReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidReducer.class);

    private MeasureCodec codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    List<KeyValueCreator> keyValueCreators;
    private Text keyText = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());
        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeDesc cubeDesc = CubeManager.getInstance(config).getCube(cubeName).getDescriptor();
        List<MeasureDesc> measuresDescs = cubeDesc.getMeasures();

        codec = new MeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);

        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];

        keyValueCreators = Lists.newArrayList();

        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new KeyValueCreator(cubeDesc, colDesc));
            }
        }
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        keyText.set(key.get());
        aggs.reset();

        for (Text value : values) {
            codec.decode(value, input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);

        KeyValue outputValue;

        int n = keyValueCreators.size();
        if (n == 1 && keyValueCreators.get(0).isFullCopy) { // shortcut for
            // simple full copy

            valueBuf.clear();
            codec.encode(result, valueBuf);
            outputValue = keyValueCreators.get(0).create(keyText, valueBuf.array(), 0, valueBuf.position());
            context.write(key, outputValue);

        } else { // normal (complex) case that distributes measures to multiple
            // HBase columns

            for (int i = 0; i < n; i++) {
                outputValue = keyValueCreators.get(i).create(keyText, result);
                context.write(key, outputValue);
            }
        }
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }


}
