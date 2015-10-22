package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.CuboidStatsUtil;
import org.junit.Test;

import com.google.common.collect.Maps;

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
        CuboidStatsUtil.writeCuboidStatistics(conf, outputPath, cuboidHLLMap, 100);
        FileSystem.getLocal(conf).delete(outputPath, true);

    }
}
