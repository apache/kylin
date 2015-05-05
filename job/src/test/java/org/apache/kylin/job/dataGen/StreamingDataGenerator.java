package org.apache.kylin.job.dataGen;

import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    public static Iterator<String> generate(final long start, final long end, final int count) {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final IIInstance ii = IIManager.getInstance(config).getII("test_streaming_table");
        final IIDesc iiDesc = ii.getDescriptor();
        final List<TblColRef> columns = iiDesc.listAllColumns();

        return new Iterator<String>() {
            private Map<String, String> values = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            private int index = 0;

            @Override
            public boolean hasNext() {
                return this.index < count;
            }

            @Override
            public String next() {
                values.clear();
                long ts = this.createTs(start, end);
                values.put("ts", Long.toString(ts));
                values.put("minute_start", Long.toString(TimeUtil.getMinuteStart(ts)));
                values.put("hour_start", Long.toString(TimeUtil.getHourStart(ts)));
                values.put("itm", Integer.toString(random.nextInt(20)));
                values.put("site", Integer.toString(random.nextInt(5)));

                values.put("gmv", String.format(decimalFormat[random.nextInt(3)], random.nextFloat() * 100));
                values.put("item_count", Integer.toString(random.nextInt(5)));

                if (values.size() != columns.size()) {
                    throw new RuntimeException("the structure of streaming table has changed, need to modify generator too");
                }

                ByteArrayOutputStream os = new ByteArrayOutputStream();
                try {
                    JsonUtil.writeValue(os, values);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                index++;
                return new String(os.toByteArray());
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
