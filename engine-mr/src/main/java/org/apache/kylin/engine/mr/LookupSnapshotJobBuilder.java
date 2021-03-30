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

package org.apache.kylin.engine.mr;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.lookup.LookupExecutableUtil;
import org.apache.kylin.engine.mr.steps.lookup.LookupSnapshotToMetaStoreStep;
import org.apache.kylin.engine.mr.steps.lookup.UpdateCubeAfterSnapshotStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupSnapshotJobBuilder {
    private static final Logger logger = LoggerFactory.getLogger(LookupSnapshotJobBuilder.class);
    private CubeInstance cube;
    private String lookupTable;
    private List<String> segments;
    private String submitter;
    private KylinConfig kylinConfig;

    public LookupSnapshotJobBuilder(CubeInstance cube, String lookupTable, List<String> segments, String submitter) {
        this.cube = cube;
        this.lookupTable = lookupTable;
        this.segments = segments;
        this.submitter = submitter;
        this.kylinConfig = cube.getConfig();
    }

    public LookupSnapshotBuildJob build() {
        logger.info("new job to build lookup snapshot:{} for cube:{}", lookupTable, cube.getName());
        LookupSnapshotBuildJob result = LookupSnapshotBuildJob.createJob(cube, lookupTable, submitter, kylinConfig);
        CubeDesc cubeDesc = cube.getDescriptor();
        SnapshotTableDesc snapshotTableDesc = cubeDesc.getSnapshotTableDesc(lookupTable);
        if (snapshotTableDesc != null && snapshotTableDesc.isExtSnapshotTable()) {
            addExtMaterializeLookupTableSteps(result, snapshotTableDesc);
        } else {
            addInMetaStoreMaterializeLookupTableSteps(result);
        }
        return result;
    }

    private void addExtMaterializeLookupTableSteps(final LookupSnapshotBuildJob result,
            SnapshotTableDesc snapshotTableDesc) {
        LookupMaterializeContext lookupMaterializeContext = new LookupMaterializeContext(result);
        ILookupMaterializer materializer = MRUtil.getExtLookupMaterializer(snapshotTableDesc.getStorageType());
        materializer.materializeLookupTable(lookupMaterializeContext, cube, lookupTable);

        UpdateCubeAfterSnapshotStep afterSnapshotStep = new UpdateCubeAfterSnapshotStep();
        afterSnapshotStep.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_LOOKUP_TABLE_UPDATE_CUBE);

        afterSnapshotStep.getParams().put(BatchConstants.ARG_EXT_LOOKUP_SNAPSHOTS_INFO, lookupMaterializeContext.getAllLookupSnapshotsInString());
        LookupExecutableUtil.setCubeName(cube.getName(), afterSnapshotStep.getParams());
        LookupExecutableUtil.setLookupTableName(lookupTable, afterSnapshotStep.getParams());
        LookupExecutableUtil.setSegments(segments, afterSnapshotStep.getParams());
        LookupExecutableUtil.setJobID(result.getId(), afterSnapshotStep.getParams());
        result.addTask(afterSnapshotStep);
    }

    private void addInMetaStoreMaterializeLookupTableSteps(final LookupSnapshotBuildJob result) {
        LookupSnapshotToMetaStoreStep lookupSnapshotToMetaStoreStep = new LookupSnapshotToMetaStoreStep();
        lookupSnapshotToMetaStoreStep.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_LOOKUP_TABLE_META_STORE);
        LookupExecutableUtil.setCubeName(cube.getName(), lookupSnapshotToMetaStoreStep.getParams());
        LookupExecutableUtil.setLookupTableName(lookupTable, lookupSnapshotToMetaStoreStep.getParams());
        LookupExecutableUtil.setSegments(segments, lookupSnapshotToMetaStoreStep.getParams());
        result.addTask(lookupSnapshotToMetaStoreStep);
    }

}
