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

package org.apache.kylin.engine.spark.job;

import com.google.common.collect.Sets;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 */

public class NSparkCubingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    // called by reflection
    public NSparkCubingStep() {}

    public NSparkCubingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCubeByUuid(cubeId);
        return MetaDumpUtil.collectCubeMetadata(cubeInstance);
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
    }

    @Override
    public boolean needMergeMetadata() {
        return true;
    }

    @Override
    protected void updateMetaAfterBuilding(KylinConfig config) throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance currentInstance = cubeManager.getCube(getCubeName());
        CubeUpdate update = new CubeUpdate(currentInstance.latestCopyForWrite());
        KylinConfig kylinDistConfig = MetaDumpUtil.loadKylinConfigFromHdfs(getDistMetaUrl());
        CubeInstance distCube = CubeManager.getInstance(kylinDistConfig).reloadCube(getCubeName());
        Set<String> segmentIds = Sets.newHashSet(org.apache.hadoop.util.StringUtils.split(getParam(MetadataConstants.P_SEGMENT_IDS)));
        update.setToUpdateSegs(distCube.getSegmentById(segmentIds.iterator().next()));
        update.setStatus(RealizationStatusEnum.READY);
        cubeManager.updateCube(update);
    }
}
