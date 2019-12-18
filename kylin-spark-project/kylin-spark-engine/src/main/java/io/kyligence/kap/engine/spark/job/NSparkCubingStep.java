/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.LinkedHashSet;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.source.SourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;

/**
 */

public class NSparkCubingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    private CubeInstance cube;

    // called by reflection
    public NSparkCubingStep() {}

    public NSparkCubingStep(CubeInstance cube, String sparkSubmitClassName) {
        this.cube = cube;
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        return collectCubeMetadata(cube);
    }
    public static Set<String> collectCubeMetadata(CubeInstance cube) {
        // cube, model_desc, cube_desc, table
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getModel().getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());
        dumpList.add(cube.getProjectInstance().getResourcePath());

        for (TableRef tableRef : cube.getDescriptor().getModel().getAllTables()) {
            TableDesc table = tableRef.getTableDesc();
            dumpList.add(table.getResourcePath());
            dumpList.addAll(SourceManager.getMRDependentResources(table));
        }

        return dumpList;
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
