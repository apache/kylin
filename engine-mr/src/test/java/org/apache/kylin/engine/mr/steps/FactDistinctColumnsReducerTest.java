/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
