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

package org.apache.kylin.tool.garbage;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.optimization.GarbageLayoutType;
import org.apache.kylin.metadata.cube.optimization.IndexOptimizerFactory;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class IndexCleaner extends MetadataCleaner {

    List<String> needUpdateModels = Lists.newArrayList();

    public IndexCleaner(String project) {
        super(project);
    }

    @Override
    public void prepare() {
        log.info("Start to clean index in project {}", project);
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val projectInstance = NProjectManager.getInstance(config).getProject(project);

        if (projectInstance.isExpertMode()) {
            log.info("not semiautomode, can't run index clean");
            return;
        }
        OptRecManagerV2 recManagerV2 = OptRecManagerV2.getInstance(project);
        for (val model : dataflowManager.listUnderliningDataModels()) {
            val dataflow = dataflowManager.getDataflow(model.getId()).copy();
            Map<Long, GarbageLayoutType> garbageLayouts = IndexOptimizerFactory.getOptimizer(dataflow, true)
                    .getGarbageLayoutMap(dataflow);

            if (MapUtils.isEmpty(garbageLayouts)) {
                continue;
            }
            boolean hasNewRecItem = recManagerV2.genRecItemsFromIndexOptimizer(project, model.getUuid(),
                    garbageLayouts);
            if (hasNewRecItem) {
                needUpdateModels.add(model.getId());
            }
        }

        log.info("Clean index in project {} finished", project);
    }

    @Override
    public void cleanup() {
        if (needUpdateModels.isEmpty()) {
            return;
        }
        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        needUpdateModels.forEach(modelId -> {
            NDataModel dataModel = mgr.getDataModelDesc(modelId);
            if (dataModel != null && !dataModel.isBroken()) {
                OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
                int newSize = optRecV2.getAdditionalLayoutRefs().size() + optRecV2.getRemovalLayoutRefs().size();
                if (dataModel.getRecommendationsCount() != newSize) {
                    mgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(newSize));
                }
            }
        });
    }

}
