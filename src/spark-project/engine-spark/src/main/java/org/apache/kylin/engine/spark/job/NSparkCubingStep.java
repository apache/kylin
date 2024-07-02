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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.StageExecutable;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public class NSparkCubingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    public NSparkCubingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    public NSparkCubingStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        Set<String> dumpList = new LinkedHashSet<>();
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        dumpList.addAll(df.collectPrecalculationResource());
        dumpList.addAll(getLogicalViewMetaDumpList(config));
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

    @Override
    public Set<String> getDependencies(KylinConfig config) {
        String dataflowId = getDataflowId();
        Set<String> segmentIds = getSegmentIds();

        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val dataflow = dfMgr.getDataflow(dataflowId);
        val indexPlan = dataflow.getIndexPlan();

        Set<String> result = Sets.newHashSet();
        val storageStore = StorageStoreFactory.create(dataflow.getModel().getStorageType());
        for (String segId : segmentIds) {
            val seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            for (LayoutEntity layout : indexPlan.getAllLayouts()) {
                String path = "/"
                        + storageStore.getStoragePathWithoutPrefix(project, dataflowId, segId, layout.getId());
                result.add(new Path(path).getParent().toString());
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
    }

    @Override
    protected ExecutableState adjustState(ExecutableState originalState) {
        if (hasWarningStage()) {
            return ExecutableState.WARNING;
        }
        return super.adjustState(originalState);
    }

    protected boolean hasWarningStage() {
        ExecutableManager executableManager = getManager();
        Map<String, List<StageExecutable>> stagesMap = getStagesMap();
        for (Map.Entry<String, List<StageExecutable>> entry : stagesMap.entrySet()) {
            String segmentId = entry.getKey();
            List<StageExecutable> stages = entry.getValue();
            boolean hasWarning = stages.stream().anyMatch(stage -> executableManager.getOutput(stage.getId(), segmentId)
                    .getState() == ExecutableState.WARNING);
            if (hasWarning) {
                return true;
            }
        }
        return false;
    }
}
