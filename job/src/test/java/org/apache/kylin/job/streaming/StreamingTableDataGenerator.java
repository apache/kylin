package org.apache.kylin.job.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * this is for generating fact table data for test_streaming_table
 */
public class StreamingTableDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(StreamingTableDataGenerator.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static List<String> generate(int recordCount, long startTime, long endTime)  {
        Preconditions.checkArgument(startTime < endTime);
        Preconditions.checkArgument(recordCount > 0);

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        TableDesc tableDesc = MetadataManager.getInstance(kylinConfig).getTableDesc("streaming_table");

        SortedMultiset<Long> times = TreeMultiset.create();
        Random r = new Random();
        for (int i = 0; i < recordCount; i++) {
            long t = startTime + (long) ((endTime - startTime) * r.nextDouble());
            times.add(t);
        }

        List<String> ret = Lists.newArrayList();
        HashMap<String, String> kvs = Maps.newHashMap();
        for (long time : times) {
            kvs.clear();
            kvs.put("timestamp", String.valueOf(time));
            for (ColumnDesc columnDesc : tableDesc.getColumns()) {
                DataType dataType = columnDesc.getType();
                if (dataType.isDateTimeFamily()) {
                    continue;
                } else if (dataType.isStringFamily()) {
                    char c = (char) ('A' + (int) (26 * r.nextDouble()));
                    kvs.put(columnDesc.getName(), String.valueOf(c));
                } else if (dataType.isIntegerFamily()) {
                    int v = r.nextInt(10000);
                    kvs.put(columnDesc.getName(), String.valueOf(v));
                } else if (dataType.isNumberFamily()) {
                    String v = String.format("%.4f", r.nextDouble() * 100);
                    kvs.put(columnDesc.getName(), v);
                }
            }
            try {
                ret.add(mapper.writeValueAsString(kvs));
            } catch (JsonProcessingException e) {
                logger.error("error!",e);
            }
        }

        return ret;
    }
}
