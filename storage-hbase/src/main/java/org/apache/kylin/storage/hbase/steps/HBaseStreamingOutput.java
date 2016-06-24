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
package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.inmemcubing.CompoundCuboidWriter;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidStatsUtil;
import org.apache.kylin.engine.streaming.IStreamingOutput;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class HBaseStreamingOutput implements IStreamingOutput {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStreamingOutput.class);

    @Override
    public ICuboidWriter getCuboidWriter(IBuildable buildable) {
        try {
            CubeSegment cubeSegment = (CubeSegment) buildable;

            final HTableInterface hTable;
            hTable = createHTable(cubeSegment);
            List<ICuboidWriter> cuboidWriters = Lists.newArrayList();
            cuboidWriters.add(new HBaseCuboidWriter(cubeSegment, hTable));
            cuboidWriters.add(new SequenceFileCuboidWriter(cubeSegment.getCubeDesc(), cubeSegment));
            return new CompoundCuboidWriter(cuboidWriters);
        } catch (IOException e) {
            throw new RuntimeException("failed to get ICuboidWriter", e);
        }
    }

    @Override
    public void output(IBuildable buildable, Map<Long, HyperLogLogPlusCounter> samplingResult) {
        try {
            CubeSegment cubeSegment = (CubeSegment) buildable;
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            final Configuration conf = HadoopUtil.getCurrentConfiguration();
            final Path outputPath = new Path("file://" + BatchConstants.CFG_STATISTICS_LOCAL_DIR + UUID.randomUUID().toString());
            CuboidStatsUtil.writeCuboidStatistics(conf, outputPath, samplingResult, 100);
            FSDataInputStream inputStream = null;
            try {
                inputStream = FileSystem.getLocal(conf).open(new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME));
                ResourceStore.getStore(kylinConfig).putResource(cubeSegment.getStatisticsResourcePath(), inputStream, System.currentTimeMillis());
            } finally {
                IOUtils.closeQuietly(inputStream);
                FileSystem.getLocal(conf).delete(outputPath, true);
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to write sampling result", e);
        }
    }

    private HTableInterface createHTable(final CubeSegment cubeSegment) throws IOException {
        final String hTableName = cubeSegment.getStorageLocationIdentifier();
        CubeHTableUtil.createHTable(cubeSegment, null);
        final HTableInterface hTable = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getTable(hTableName);
        logger.info("hTable:" + hTableName + " for segment:" + cubeSegment.getName() + " created!");
        return hTable;
    }
}
