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

package org.apache.kylin.engine.spark.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateMetadataUtil {

    protected static final Logger logger = LoggerFactory.getLogger(UpdateMetadataUtil.class);

    public static void syncLocalMetadataToRemote(KylinConfig config, NSparkExecutable nsparkExecutable)
            throws IOException {
        String cubeId = nsparkExecutable.getParam(MetadataConstants.P_CUBE_ID);
        Set<String> segmentIds = Sets
                .newHashSet(StringUtils.split(nsparkExecutable.getParam(CubingExecutableUtil.SEGMENT_ID), " "));
        String segmentId = segmentIds.iterator().next();
        String remoteResourceStore = nsparkExecutable.getDistMetaUrl();
        String jobType = nsparkExecutable.getParam(MetadataConstants.P_JOB_TYPE);

        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance currentInstanceCopy = cubeManager.getCubeByUuid(cubeId).latestCopyForWrite();

        // load the meta from local meta path of this job
        KylinConfig kylinDistConfig = MetaDumpUtil.loadKylinConfigFromHdfs(remoteResourceStore);
        CubeInstance distCube = CubeManager.getInstance(kylinDistConfig).getCubeByUuid(cubeId);
        CubeSegment toUpdateSeg = distCube.getSegmentById(segmentId);

        List<CubeSegment> tobeSegments = currentInstanceCopy.calculateToBeSegments(toUpdateSeg);
        if (!tobeSegments.contains(toUpdateSeg))
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "For cube %s, segment %s is expected but not in the tobe %s",
                            currentInstanceCopy.toString(), toUpdateSeg.toString(), tobeSegments.toString()));

        String resKey = toUpdateSeg.getStatisticsResourcePath();
        String statisticsDir = config.getJobTmpDir(currentInstanceCopy.getProject()) + "/"
                + nsparkExecutable.getParam(MetadataConstants.P_JOB_ID) + "/" + ResourceStore.CUBE_STATISTICS_ROOT + "/"
                + cubeId + "/" + segmentId + "/";
        Path statisticsFile = new Path(statisticsDir, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(statisticsFile)) {
            FSDataInputStream is = fs.open(statisticsFile);
            ResourceStore.getStore(config).putBigResource(resKey, is, System.currentTimeMillis());
        }

        CubeUpdate update = new CubeUpdate(currentInstanceCopy);
        update.setCuboids(distCube.getCuboids());
        List<CubeSegment> toRemoveSegs = Lists.newArrayList();

        if (String.valueOf(CubeBuildTypeEnum.MERGE).equals(jobType)) {
            toUpdateSeg.getSnapshots().clear();
            // update the snapshot table path
            for (Map.Entry<String, String> entry : currentInstanceCopy.getLatestReadySegment().getSnapshots()
                    .entrySet()) {
                toUpdateSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
            }
        } else if (String.valueOf(CubeBuildTypeEnum.OPTIMIZE).equals(jobType)) {
            CubeSegment origSeg = currentInstanceCopy.getOriginalSegmentToOptimize(toUpdateSeg);
            toUpdateSeg.getDictionaries().putAll(origSeg.getDictionaries());
            toUpdateSeg.getSnapshots().putAll(origSeg.getSnapshots());
            toUpdateSeg.getRowkeyStats().addAll(origSeg.getRowkeyStats());

            CubeStatsReader optSegStatsReader = new CubeStatsReader(toUpdateSeg, config);
            CubeStatsReader origSegStatsReader = new CubeStatsReader(origSeg, config);
            Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();
            if (origSegStatsReader.getCuboidRowHLLCounters() == null) {
                logger.warn(
                        "Cuboid statistics of original segment do not exist. Please check the config of kylin.engine.segment-statistics-enabled.");
            } else {
                addFromCubeStatsReader(origSegStatsReader, cuboidHLLMap);
                addFromCubeStatsReader(optSegStatsReader, cuboidHLLMap);

                Set<Long> recommendCuboids = currentInstanceCopy.getCuboidsByMode(CuboidModeEnum.RECOMMEND);
                Map<Long, HLLCounter> resultCuboidHLLMap = Maps.newHashMapWithExpectedSize(recommendCuboids.size());
                for (long cuboid : recommendCuboids) {
                    HLLCounter hll = cuboidHLLMap.get(cuboid);
                    if (hll == null) {
                        logger.warn("Cannot get the row count stats for cuboid " + cuboid);
                    } else {
                        resultCuboidHLLMap.put(cuboid, hll);
                    }
                }
                if (fs.exists(statisticsFile)) {
                    fs.delete(statisticsFile, false);
                }
                CubeStatsWriter.writeCuboidStatistics(HadoopUtil.getCurrentConfiguration(), new Path(statisticsDir),
                        resultCuboidHLLMap, 1, origSegStatsReader.getSourceRowCount());
                FSDataInputStream is = fs.open(statisticsFile);
                ResourceStore.getStore(config).putBigResource(resKey, is, System.currentTimeMillis());
            }
            toUpdateSeg.setStatus(SegmentStatusEnum.READY_PENDING);
        } else {
            toUpdateSeg.setStatus(SegmentStatusEnum.READY);
            for (CubeSegment segment : currentInstanceCopy.getSegments()) {
                if (!tobeSegments.contains(segment))
                    toRemoveSegs.add(segment);
            }
            Collections.sort(toRemoveSegs);
            if (currentInstanceCopy.getConfig().isJobAutoReadyCubeEnabled()) {
                update.setStatus(RealizationStatusEnum.READY);
            }
        }

        logger.info("Promoting cube {}, new segment {}, to remove segments {}", currentInstanceCopy, toUpdateSeg,
                toRemoveSegs);

        toUpdateSeg.setLastBuildTime(System.currentTimeMillis());
        update.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[0])).setToUpdateSegs(toUpdateSeg);
        cubeManager.updateCube(update);
    }

    public static void updateMetadataAfterMerge(String cubeId, String segmentId, KylinConfig config)
            throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance currentInstanceCopy = cubeManager.getCubeByUuid(cubeId).latestCopyForWrite();

        CubeSegment toUpdateSegs = currentInstanceCopy.getSegmentById(segmentId);

        List<CubeSegment> tobeSegments = currentInstanceCopy.calculateToBeSegments(toUpdateSegs);
        if (!tobeSegments.contains(toUpdateSegs))
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "For cube %s, segment %s is expected but not in the tobe %s",
                            currentInstanceCopy.toString(), toUpdateSegs.toString(), tobeSegments.toString()));

        CubeUpdate update = new CubeUpdate(currentInstanceCopy);
        List<CubeSegment> toRemoveSegs = Lists.newArrayList();

        toUpdateSegs.setStatus(SegmentStatusEnum.READY);
        for (CubeSegment segment : currentInstanceCopy.getSegments()) {
            if (!tobeSegments.contains(segment))
                toRemoveSegs.add(segment);
        }
        Collections.sort(toRemoveSegs);
        if (currentInstanceCopy.getConfig().isJobAutoReadyCubeEnabled()) {
            update.setStatus(RealizationStatusEnum.READY);
        }

        logger.info("Promoting cube {}, new segment {}, to remove segments {}", currentInstanceCopy, toUpdateSegs,
                toRemoveSegs);

        toUpdateSegs.setLastBuildTime(System.currentTimeMillis());
        update.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[0])).setToUpdateSegs(toUpdateSegs);
        cubeManager.updateCube(update);
    }

    private static void addFromCubeStatsReader(CubeStatsReader cubeStatsReader, Map<Long, HLLCounter> cuboidHLLMap) {
        for (Map.Entry<Long, HLLCounter> entry : cubeStatsReader.getCuboidRowHLLCounters().entrySet()) {
            if (cuboidHLLMap.get(entry.getKey()) != null) {
                cuboidHLLMap.get(entry.getKey()).merge(entry.getValue());
            } else {
                cuboidHLLMap.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
