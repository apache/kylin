/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */
package org.apache.kylin.storage.hbase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducer;
import org.apache.kylin.engine.streaming.IStreamingOutput;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.storage.hbase.steps.CubeHTableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

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
            return new HBaseCuboidWriter(cubeSegment.getCubeDesc(), hTable);
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
            final Path outputPath = new Path("file:///tmp/cuboidstatistics/" + UUID.randomUUID().toString());
            FileSystem.getLocal(conf).deleteOnExit(outputPath);
            FactDistinctColumnsReducer.writeCuboidStatistics(conf, outputPath, samplingResult, 100);
            FSDataInputStream inputStream = null;
            try {
                inputStream = FileSystem.getLocal(conf).open(new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION));
                ResourceStore.getStore(kylinConfig).putResource(cubeSegment.getStatisticsResourcePath(), inputStream, 0);
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to write sampling result", e);
        }
    }

    private HTableInterface createHTable(final CubeSegment cubeSegment) throws IOException {
        final String hTableName = cubeSegment.getStorageLocationIdentifier();
        CubeHTableUtil.createHTable(cubeSegment.getCubeDesc(), hTableName, null);
        final HTableInterface hTable = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getTable(hTableName);
        logger.info("hTable:" + hTableName + " for segment:" + cubeSegment.getName() + " created!");
        return hTable;
    }
}
