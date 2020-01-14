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

package io.kyligence.kap.engine.spark.job;

import io.kyligence.kap.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /*@Override
    public Set<String> getDependencies(KylinConfig config) {
        String dataflowId = getDataflowId();
        Set<String> segmentIds = getSegmentIds();

        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val dataflow = dfMgr.getDataflow(dataflowId);
        val indexPlan = dataflow.getIndexPlan();

        Set<String> result = Sets.newHashSet();
        for (String segId : segmentIds) {
            val seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            for (LayoutEntity layout : indexPlan.getAllLayouts()) {
                NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layout.getId());
                String path = "/" + NSparkCubingUtil.getStoragePathWithoutPrefix(dataCuboid.getSegDetails(),
                        dataCuboid.getLayoutId());
                result.add(path);
                result.add(path + DFBuildJob.TEMP_DIR_SUFFIX);
            }
        }

        val model = indexPlan.getModel();
        model.getJoinTables().forEach(lookupDesc -> {
            val tableDesc = lookupDesc.getTableRef().getTableDesc();
            val isLookupTable = model.isLookupTable(lookupDesc.getTableRef());
            if (isLookupTable) {
                val tablePath = "/" + tableDesc.getProject() + HadoopUtil.SNAPSHOT_STORAGE_ROOT + "/"
                        + tableDesc.getName();
                result.add(tablePath);
            }
        });
        return result;
    }*/
}
