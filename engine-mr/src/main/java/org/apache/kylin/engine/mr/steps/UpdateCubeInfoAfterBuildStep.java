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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.DimensionRangeInfo;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.LookupMaterializeContext;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.datatype.DataTypeOrder;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 */
public class UpdateCubeInfoAfterBuildStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateCubeInfoAfterBuildStep.class);

    private long timeMaxValue = Long.MIN_VALUE;
    private long timeMinValue = Long.MAX_VALUE;

    public UpdateCubeInfoAfterBuildStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()))
                .latestCopyForWrite();
        final CubeSegment segment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        CubingJob cubingJob = (CubingJob) getManager().getJob(CubingExecutableUtil.getCubingJobId(this.getParams()));
        long sourceCount = cubingJob.findSourceRecordCount();
        long sourceSizeBytes = cubingJob.findSourceSizeBytes();
        long cubeSizeBytes = cubingJob.findCubeSizeBytes();

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Double> cuboidEstimateRatio = cubingJob.findEstimateRatio(segment, config);

        segment.setLastBuildJobID(CubingExecutableUtil.getCubingJobId(this.getParams()));
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setSizeKB(cubeSizeBytes / 1024);
        segment.setInputRecords(sourceCount);
        segment.setInputRecordsSize(sourceSizeBytes);
        segment.setEstimateRatio(cuboidEstimateRatio);

        try {
            saveExtSnapshotIfNeeded(cubeManager, cube, segment);
            updateSegment(segment);

            cubeManager.promoteNewlyBuiltSegments(cube, segment);
            return new ExecuteResult();
        } catch (IOException e) {
            logger.error("fail to update cube after build", e);
            return ExecuteResult.createError(e);
        }
    }

    private void saveExtSnapshotIfNeeded(CubeManager cubeManager, CubeInstance cube, CubeSegment segment)
            throws IOException {
        String extLookupSnapshotStr = this.getParam(BatchConstants.ARG_EXT_LOOKUP_SNAPSHOTS_INFO);
        if (extLookupSnapshotStr == null || extLookupSnapshotStr.isEmpty()) {
            return;
        }
        Map<String, String> extLookupSnapshotMap = LookupMaterializeContext.parseLookupSnapshots(extLookupSnapshotStr);
        logger.info("update ext lookup snapshots:{}", extLookupSnapshotMap);
        List<SnapshotTableDesc> snapshotTableDescList = cube.getDescriptor().getSnapshotTableDescList();
        for (SnapshotTableDesc snapshotTableDesc : snapshotTableDescList) {
            String tableName = snapshotTableDesc.getTableName();
            if (snapshotTableDesc.isExtSnapshotTable()) {
                String newSnapshotResPath = extLookupSnapshotMap.get(tableName);
                if (newSnapshotResPath == null || newSnapshotResPath.isEmpty()) {
                    continue;
                }

                if (snapshotTableDesc.isGlobal()) {
                    if (!newSnapshotResPath.equals(cube.getSnapshotResPath(tableName))) {
                        cubeManager.updateCubeLookupSnapshot(cube, tableName, newSnapshotResPath);
                    }
                } else {
                    segment.putSnapshotResPath(tableName, newSnapshotResPath);
                }
            }
        }
    }

    private void updateSegment(CubeSegment segment) throws IOException {
        final TblColRef partitionCol = segment.getCubeDesc().getModel().getPartitionDesc().getPartitionDateColumnRef();

        for (TblColRef dimColRef : segment.getCubeDesc().listDimensionColumnsExcludingDerived(true)) {
            if (!dimColRef.getType().needCompare())
                continue;

            final String factColumnsInputPath = this.getParams().get(BatchConstants.CFG_OUTPUT_PATH);
            Path colDir = new Path(factColumnsInputPath, dimColRef.getIdentity());
            FileSystem fs = HadoopUtil.getWorkingFileSystem();

            //handle multiple reducers
            Path[] outputFiles = HadoopUtil.getFilteredPath(fs, colDir,
                    dimColRef.getName() + FactDistinctColumnsReducer.DIMENSION_COL_INFO_FILE_POSTFIX);
            if (outputFiles == null || outputFiles.length == 0) {
                segment.getDimensionRangeInfoMap().put(dimColRef.getIdentity(), new DimensionRangeInfo(null, null));
                continue;
            }

            FSDataInputStream is = null;
            BufferedReader bufferedReader = null;
            InputStreamReader isr = null;
            Set<String> minValues = Sets.newHashSet(), maxValues = Sets.newHashSet();
            for (Path outputFile : outputFiles) {
                try {
                    is = fs.open(outputFile);
                    isr = new InputStreamReader(is, StandardCharsets.UTF_8);
                    bufferedReader = new BufferedReader(isr);
                    minValues.add(bufferedReader.readLine());
                    maxValues.add(bufferedReader.readLine());
                } finally {
                    IOUtils.closeQuietly(is);
                    IOUtils.closeQuietly(isr);
                    IOUtils.closeQuietly(bufferedReader);
                }
            }
            DataTypeOrder order = dimColRef.getType().getOrder();
            String minValue = order.min(minValues);
            String maxValue = order.max(maxValues);

            if (segment.isOffsetCube() && partitionCol != null
                    && partitionCol.getIdentity().equals(dimColRef.getIdentity())) {
                logger.debug("update partition. {} timeMinValue:" + minValue + " timeMaxValue:" + maxValue,
                        dimColRef.getName());
                if (DateFormat.stringToMillis(minValue) != timeMinValue
                        && DateFormat.stringToMillis(maxValue) != timeMaxValue) {
                    segment.setTSRange(
                            new TSRange(DateFormat.stringToMillis(minValue), DateFormat.stringToMillis(maxValue) + 1));
                }
            }
            segment.getDimensionRangeInfoMap().put(dimColRef.getIdentity(), new DimensionRangeInfo(minValue, maxValue));
        }
    }
}
