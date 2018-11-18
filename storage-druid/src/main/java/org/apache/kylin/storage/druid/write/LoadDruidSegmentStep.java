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

package org.apache.kylin.storage.druid.write;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kylin.storage.druid.common.DruidSerdeHelper.JSON_MAPPER;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.common.DruidCoordinatorClient;
import org.apache.kylin.storage.druid.common.DruidServerMetadata;
import org.apache.kylin.storage.druid.common.ImmutableSegmentLoadInfo;
import org.joda.time.Interval;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import io.druid.java.util.common.ISE;
import io.druid.timeline.DataSegment;

public class LoadDruidSegmentStep extends AbstractExecutable {
    private static final long MAX_LOAD_MILLIS = 2 * 3600 * 1000;

    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final Map<String, String> params = getParams();
        final String cubeName = CubingExecutableUtil.getCubeName(params);
        final String segmentID = CubingExecutableUtil.getSegmentId(params);
        stepLogger.log("cube: " + cubeName + ", segment: " + segmentID);

        try {
            CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = manager.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);

            final String dataSource = DruidSchema.getDataSource(segment.getCubeDesc());

            long start = System.currentTimeMillis();
            final Set<DataSegment> druidSegments = getDruidSegmentDescriptors(dataSource, segment);
            new AnnounceDruidSegment().announceHistoricalSegments(druidSegments);
            stepLogger.log("Read SegmentDescriptors and update druid mysql metadata elapsed time: "
                    + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            boolean success = false;
            final DruidCoordinatorClient coordinatorClient = DruidCoordinatorClient.getSingleton();

            while (System.currentTimeMillis() < (start + MAX_LOAD_MILLIS)) {
                int loaded = numLoadedDruidSegments(coordinatorClient, segment);
                if (loaded == druidSegments.size()) {
                    success = true;
                    break;
                }
                logger.info("Still waiting for druid to load segments ({}/{} loaded)", loaded, druidSegments.size());
                Thread.sleep(60000);
            }

            if (!success) {
                throw new ISE("Timeout after %,dms", MAX_LOAD_MILLIS);
            }

            stepLogger.log(StringUtils.format("Successfully loaded %d segments in %,dms", druidSegments.size(),
                    System.currentTimeMillis() - start));
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("LoadDruidSegmentStep failed: ", e);
            stepLogger.log("LoadDruidSegmentStep failed " + e.getMessage());
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }

    private int numLoadedDruidSegments(DruidCoordinatorClient coordinatorClient, CubeSegment segment) {
        final String dataSource = DruidSchema.getDataSource(segment.getCubeDesc());
        final Interval interval = DruidSchema.segmentInterval(segment);
        final String version = segment.getCreateTimeUTCStr();

        List<ImmutableSegmentLoadInfo> loaded = coordinatorClient.fetchServerView(dataSource, interval);

        int count = 0;
        for (ImmutableSegmentLoadInfo info : loaded) {
            if (info.getSegment().getVersion().compareTo(version) >= 0
                    && Iterables.any(info.getServers(), new Predicate<DruidServerMetadata>() {
                        @Override
                        public boolean apply(DruidServerMetadata input) {
                            return "historical".equals(input.getType());
                        }
                    })) {
                count++;
            }
        }
        return count;
    }

    private Set<DataSegment> getDruidSegmentDescriptors(String dataSource, CubeSegment cubeSegment) throws IOException {
        Set<DataSegment> segmentSet = new HashSet<>();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        long totalSize = 0;
        for (int i = 0; i < cubeSegment.getTotalShards(); i++) {
            Path descriptorPath = getSegmentDescriptor(dataSource, cubeSegment, i);
            try (FSDataInputStream in = fs.open(descriptorPath)) {
                DataSegment dataSegment = JSON_MAPPER.readValue((InputStream) in, DataSegment.class);
                segmentSet.add(dataSegment);
                totalSize += dataSegment.getSize();
            }
        }

        checkArgument(segmentSet.size() == cubeSegment.getTotalShards(), "expect %d value, got %d",
                cubeSegment.getTotalShards(), segmentSet.size());

        addExtraInfo(CubingJob.CUBE_SIZE_BYTES, totalSize + "");
        return segmentSet;
    }

    private Path getSegmentDescriptor(String dataSource, CubeSegment segment, int shardId) {
        return new Path(segment.getConfig().getDruidHdfsLocation() + "/" + dataSource + "/" + segment.getUuid(),
                shardId + "-" + "descriptor");
    }

    public void setCubeName(String cubeName) {
        CubingExecutableUtil.setCubeName(cubeName, getParams());
    }

    public void setSegmentID(String segmentID) {
        CubingExecutableUtil.setSegmentId(segmentID, getParams());
    }
}
