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

package org.apache.kylin.rest.config.initialize;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.util.scd2.SCD2CondChecker;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.springframework.util.CollectionUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelBrokenListener {

    private boolean needHandleModelBroken(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        return model != null && model.isBroken() && !model.isHandledAfterBroken();
    }

    @Subscribe
    public void onModelBroken(NDataModel.ModelBrokenEvent event) {
        val project = event.getProject();
        val modelId = event.getSubject();

        if (!needHandleModelBroken(project, modelId)) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (!needHandleModelBroken(project, modelId)) {
                return null;
            }

            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);

            val model = getBrokenModel(project, MetadataType.mergeKeyWithType(modelId, MetadataType.MODEL));

            val dataflowManager = NDataflowManager.getInstance(config, project);
            val indexPlanManager = NIndexPlanManager.getInstance(config, project);

            if (config.getSmartModeBrokenModelDeleteEnabled()) {
                dataflowManager.dropDataflow(model.getId());
                indexPlanManager.dropIndexPlan(model.getId());
                modelManager.dropModel(model);
                return null;
            }
            val dataflow = dataflowManager.getDataflow(modelId);
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setStatus(RealizationStatusEnum.BROKEN);
            if (model.getBrokenReason() == NDataModel.BrokenReason.SCHEMA) {
                dfUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
            }
            dataflowManager.updateDataflow(dfUpdate);
            NDataModel copy = modelManager.copyForWrite(model);
            copy.setHandledAfterBroken(true);
            copy.setRecommendationsCount(0);
            modelManager.updateDataBrokenModelDesc(copy);

            OptRecManagerV2 optRecManagerV2 = OptRecManagerV2.getInstance(project);
            optRecManagerV2.discardAll(model.getId());
            return null;
        }, project);
    }

    private boolean needHandleModelRepair(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        return model != null && !model.isBroken() && model.isHandledAfterBroken();
    }

    @Subscribe
    public void onModelRepair(NDataModel.ModelRepairEvent event) {

        val project = event.getProject();
        val modelId = event.getSubject();

        if (!needHandleModelRepair(project, modelId)) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (!needHandleModelRepair(project, modelId)) {
                return null;
            }
            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);
            val modelOrigin = modelManager.getDataModelDesc(modelId);
            val model = modelManager.copyForWrite(modelOrigin);

            val dataflowManager = NDataflowManager.getInstance(config, project);
            val dataflow = dataflowManager.getDataflow(modelId);
            val dfUpdate = new NDataflowUpdate(dataflow.getId());

            dfUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            //if scd2 turn off , model should be offline
            if (dataflow.getLastStatus() != null && !checkSCD2Disabled(project, modelId)) {
                dfUpdate.setStatus(dataflow.getLastStatus());
            }
            dataflowManager.updateDataflow(dfUpdate);
            if (CollectionUtils.isEmpty(dataflow.getSegments())) {
                if (model.getManagementType() == ManagementType.MODEL_BASED && model.getPartitionDesc() == null) {
                    dataflowManager.fillDfManually(dataflow,
                            Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
                }
                final JobParam jobParam = new JobParam(model.getId(), "ADMIN");
                jobParam.setProject(project);
                val sourceUsageManager = SourceUsageManager.getInstance(config);
                sourceUsageManager.licenseCheckWrap(project,
                        () -> JobManager.getInstance(config, project).addIndexJob(jobParam));
            }
            model.setHandledAfterBroken(false);
            modelManager.updateDataBrokenModelDesc(model);

            return null;
        }, project);
    }

    private boolean checkSCD2Disabled(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        boolean isSCD2 = SCD2CondChecker.INSTANCE.isScd2Model(model);

        return !NProjectManager.getInstance(config).getProject(project).getConfig().isQueryNonEquiJoinModelEnabled()
                && isSCD2;
    }

    private NDataModel getBrokenModel(String project, String resourcePath) {
        try {
            val resource = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource(resourcePath);
            val modelDesc = JsonUtil.readValue(resource.getByteSource().read(), NDataModel.class);
            modelDesc.setBroken(true);
            modelDesc.setProject(project);
            modelDesc.setMvcc(resource.getMvcc());
            return modelDesc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
