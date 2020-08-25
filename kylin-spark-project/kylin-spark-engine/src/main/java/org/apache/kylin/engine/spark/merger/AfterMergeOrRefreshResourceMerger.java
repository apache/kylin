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

package org.apache.kylin.engine.spark.merger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.collect.Lists;

public class AfterMergeOrRefreshResourceMerger extends MetadataMerger {

    public AfterMergeOrRefreshResourceMerger(KylinConfig config) {
        super(config);
    }

    @Override
    public void merge(String cubeId, String segmentId, ResourceStore remoteResourceStore, String jobType) {

        CubeManager cubeManager = CubeManager.getInstance(getConfig());
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(cubeId);
        CubeUpdate update = new CubeUpdate(cubeInstance.latestCopyForWrite());

        CubeManager distManager = CubeManager.getInstance(remoteResourceStore.getConfig());
        CubeInstance distCube = distManager.getCubeByUuid(cubeId).latestCopyForWrite();

        List<CubeSegment> toUpdateSegments = Lists.newArrayList();

        CubeSegment mergedSegment = distCube.getSegmentById(segmentId);
        mergedSegment.setStatus(SegmentStatusEnum.READY);
        Map<String, String> additionalInfo = mergedSegment.getAdditionalInfo();
        additionalInfo.put("storageType", "" + IStorageAware.ID_PARQUET);
        mergedSegment.setAdditionalInfo(additionalInfo);
        toUpdateSegments.add(mergedSegment);

        List<CubeSegment> toRemoveSegments = getToRemoveSegs(distCube, mergedSegment);
        Collections.sort(toRemoveSegments);
        makeSnapshotForNewSegment(mergedSegment, toRemoveSegments);

        if (String.valueOf(CubeBuildTypeEnum.MERGE).equals(jobType)) {
            Optional<Long> reduce = toRemoveSegments.stream().map(CubeSegment::getSizeKB).filter(size -> size != -1)
                    .reduce(Long::sum);
            Optional<Long> inputRecords = toRemoveSegments.stream().map(CubeSegment::getInputRecords).filter(records -> records != -1)
                    .reduce(Long::sum);
            if (reduce.isPresent()) {
                long totalSourceSize = reduce.get();
                mergedSegment.setSizeKB(totalSourceSize);
                mergedSegment.setInputRecords(inputRecords.get());
                mergedSegment.setLastBuildTime(System.currentTimeMillis());
            }
        }

        update.setToRemoveSegs(toRemoveSegments.toArray(new CubeSegment[0]));
        update.setToUpdateSegs(toUpdateSegments.toArray(new CubeSegment[0]));

        try {
            cubeManager.updateCube(update);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    List<CubeSegment> getToRemoveSegs(CubeInstance cube, CubeSegment mergedSegment) {
        Segments tobe = cube.calculateToBeSegments(mergedSegment);

        if (!tobe.contains(mergedSegment))
            throw new IllegalStateException(
                    "For Cube " + cube + ", segment " + mergedSegment + " is expected but not in the tobe " + tobe);

        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        List<CubeSegment> toRemoveSegs = Lists.newArrayList();
        for (CubeSegment s : cube.getSegments()) {
            if (!tobe.contains(s))
                toRemoveSegs.add(s);
        }

        return toRemoveSegs;
    }

    private void makeSnapshotForNewSegment(CubeSegment newSeg, List<CubeSegment> mergingSegments) {
        CubeSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        String buildStepUrl = abstractExecutable.getParam(MetadataConstants.P_OUTPUT_META_URL);
        KylinConfig buildConfig = KylinConfig.createKylinConfig(this.getConfig());
        buildConfig.setMetadataUrl(buildStepUrl);
        ResourceStore resourceStore = ResourceStore.getStore(buildConfig);
        String cubeId = abstractExecutable.getParam(MetadataConstants.P_CUBE_ID);
        String segmentId = abstractExecutable.getParam(CubingExecutableUtil.SEGMENT_ID);
        merge(cubeId, segmentId, resourceStore, abstractExecutable.getParam(MetadataConstants.P_JOB_TYPE));
    }

}
