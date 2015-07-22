package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.job.hadoop.cube.FactDistinctColumnsReducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 */
public class FactDistinctColumnsReducerTest {


    @Test
    public void testWriteCuboidStatistics() throws IOException {

        final Configuration conf = HadoopUtil.getCurrentConfiguration();
        final Path outputPath = new Path("file:///tmp/kylin/cuboidstatistics/" + UUID.randomUUID().toString());
        if (!FileSystem.getLocal(conf).exists(outputPath)) {
//            FileSystem.getLocal(conf).create(outputPath);
        }

        System.out.println(outputPath);
        Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = Maps.newHashMap();
        FactDistinctColumnsReducer.writeCuboidStatistics(conf, outputPath, cuboidHLLMap, 100);


    }
}
