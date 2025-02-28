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

package org.apache.kylin.rec.util;

import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.AbstractSemiContext;
import org.apache.kylin.rec.ModelCreateContext;
import org.apache.kylin.rec.ModelReuseContext;
import org.apache.kylin.rec.ProposerJob;
import org.apache.kylin.rec.SmartContext;
import org.apache.kylin.rec.SmartMaster;

import lombok.val;

/**
 * This class is used to execute acceleration. Only used in testing.
 */
public class AccelerationUtil {

    private AccelerationUtil() {
    }

    public static AbstractContext runWithSmartContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        return runWithSmartContext(kylinConfig, project, sqlArray, false);
    }

    public static AbstractContext runWithSmartContext(KylinConfig kylinConfig, String project, String[] sqlArray,
            boolean online) {
        KylinConfig config = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig();
        SmartContext context = new SmartContext(config, project, sqlArray);
        SmartMaster smartMaster = new SmartMaster(context);
        Consumer<AbstractContext> hook = online ? onlineHook : null;
        smartMaster.runUtWithContext(hook);
        return smartMaster.getContext();
    }

    public static AbstractSemiContext runModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray,
            boolean canCreateNewModel) {
        KylinConfig config = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig();
        ModelReuseContext context = new ModelReuseContext(config, project, sqlArray, canCreateNewModel);
        context.getExtraMeta().setOnlineModelIds(getOnlineModelIds(project));
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.executePropose();
        return context;
    }

    public static AbstractSemiContext runModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        KylinConfig config = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig();
        ModelReuseContext context = new ModelReuseContext(config, project, sqlArray);
        context.getExtraMeta().setOnlineModelIds(getOnlineModelIds(project));
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        AccelerationUtil.onlineModel(context);
        return context;
    }

    private static Set<String> getOnlineModelIds(String project) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        return NDataflowManager.getInstance(projectConfig, project).listOnlineDataModels().stream()
                .map(RootPersistentEntity::getUuid).collect(Collectors.toSet());
    }

    public static AbstractSemiContext runModelCreateContext(KylinConfig kylinConfig, String project,
            String[] sqlArray) {
        KylinConfig projectConfig = NProjectManager.getInstance(kylinConfig).getProject(project).getConfig();
        ModelCreateContext context = new ModelCreateContext(projectConfig, project, sqlArray) {
            @Override
            public void saveMetadata() {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    KylinConfig config = KylinConfig.getInstanceFromEnv();
                    NIndexPlanManager indexPlanMgr = NIndexPlanManager.getInstance(config, getProject());
                    NDataflowManager dfMgr = NDataflowManager.getInstance(config, getProject());
                    NDataModelManager modelMgr = NDataModelManager.getInstance(config, getProject());
                    for (ModelContext modelCtx : getModelContexts()) {
                        if (modelCtx.skipSavingMetadata()) {
                            continue;
                        }
                        NDataModel model = modelCtx.getTargetModel();
                        if (modelMgr.getDataModelDesc(model.getUuid()) != null) {
                            modelMgr.updateDataModelDesc(model);
                        } else {
                            modelMgr.createDataModelDesc(model, model.getOwner());
                        }
                        IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
                        if (indexPlanMgr.getIndexPlan(indexPlan.getUuid()) == null) {
                            indexPlanMgr.createIndexPlan(indexPlan);
                            dfMgr.createDataflow(indexPlan, indexPlan.getModel().getOwner());
                        } else {
                            indexPlanMgr.updateIndexPlan(indexPlan);
                        }
                    }
                    return true;
                }, getProject());
            }
        };
        context.getProposers().execute();
        context.saveMetadata();
        AccelerationUtil.onlineModel(context);
        return context;
    }

    public static void onlineModel(AbstractContext context) {
        if (context == null || context.getModelContexts() == null) {
            return;
        }
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig kylinConfig = context.getKapConfig().getKylinConfig();
            context.getModelContexts().forEach(ctx -> {
                NDataflowManager dfManager = NDataflowManager.getInstance(kylinConfig, context.getProject());
                NDataModel model = ctx.getTargetModel();
                if (model == null || dfManager.getDataflow(model.getId()) == null) {
                    return;
                }
                dfManager.updateDataflowStatus(model.getId(), RealizationStatusEnum.ONLINE);
            });
            return true;
        }, context.getProject());
    }

    public static AbstractContext genOptRec(KylinConfig config, String project, String[] sqls) {
        val context = new ModelReuseContext(config, project, sqls);
        return ProposerJob.propose(context);
    }

    public static Consumer<AbstractContext> onlineHook = AccelerationUtil::onlineModel;
}
