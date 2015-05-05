package org.apache.kylin.job.dataGen;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by Hongbin Ma(Binmahone) on 5/5/15.
 */
public class StreamingDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(StreamingDataGenerator.class);
    private static Random random = new Random();
    private static String[] decimalFormat = new String[] { "%.4f", "%.5f", "%.6f" };

    public static Iterator<List<String>> generate(final long start, final long end, final int count) {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final IIInstance ii = IIManager.getInstance(config).getII("test_streaming_table");
        final IIDesc iiDesc = ii.getDescriptor();
        final MetadataManager metadataManager = MetadataManager.getInstance(config);
        final ColumnDesc[] columnDescs = metadataManager.getTableDesc(iiDesc.getFactTableName()).getColumns();

        return new Iterator<List<String>>() {
            private Map<String, String> values = Maps.newHashMap();

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public List<String> next() {
                values.clear();
                long ts = this.createTs(start, end);
                values.put("ts", Long.toString(ts));
                values.put("minute_start", Long.toString(TimeUtil.getMinuteStart(ts)));
                values.put("hour_start", Long.toString(TimeUtil.getHourStart(ts)));
                values.put("itm", Integer.toString(random.nextInt(20)));
                values.put("site", Integer.toString(random.nextInt(5)));

                values.put("gmv", String.format(decimalFormat[random.nextInt(3)], random.nextFloat() * 100));
                values.put("item_count", Integer.toString(random.nextInt(5)));

                if (values.size() != columnDescs.length) {
                    throw new RuntimeException("the structure of streaming table has changed, need to modify generator too");
                }

                List<String> ret = Lists.newArrayList();
                for (ColumnDesc columnDesc : columnDescs) {
                    String name = columnDesc.getName();
                    ret.add(values.get(name));
                }
                return ret;
            }

            @Override
            public void remove() {
            }

            private long createTs(final long start, final long end) {
                return start + (long) (random.nextDouble() * (end - start));
            }
        };
    }

}
