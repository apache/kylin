package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 */
public class InMemCuboidReducer extends KylinReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidReducer.class);

    private MeasureCodec codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;

    List<InMemKeyValueCreator> keyValueCreators;
    private int nColumns = 0;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeDesc cubeDesc = CubeManager.getInstance(config).getCube(cubeName).getDescriptor();

        List<MeasureDesc> measuresDescs = Lists.newArrayList();
        for (HBaseColumnFamilyDesc familyDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                for (MeasureDesc measure : hbaseColDesc.getMeasures()) {
                    measuresDescs.add(measure);
                }
            }
        }

        codec = new MeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);

        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];

        keyValueCreators = Lists.newArrayList();

        int startPosition = 0;
        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                keyValueCreators.add(new InMemKeyValueCreator(colDesc, startPosition));
                startPosition += colDesc.getMeasures().length;
            }
        }

        nColumns = keyValueCreators.size();
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        aggs.reset();

        for (Text value : values) {
            codec.decode(value, input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);

        KeyValue outputValue;

        for (int i = 0; i < nColumns; i++) {
            outputValue = keyValueCreators.get(i).create(key.get(), 0, key.getLength(), result);
            context.write(key, outputValue);
        }
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }


}
