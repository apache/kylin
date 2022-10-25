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

package org.apache.kylin.metadata.model;

import java.util.Objects;
import java.util.Optional;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class FusionModelManager {
    private static final Logger logger = LoggerFactory.getLogger(FusionModelManager.class);

    public static FusionModelManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, FusionModelManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static FusionModelManager newInstance(KylinConfig config, String project) {
        return new FusionModelManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<FusionModel> crud;

    private FusionModelManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.FUSION_MODEL_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FusionModel>(getStore(), resourceRootPath, FusionModel.class) {
            @Override
            protected FusionModel initEntityAfterReload(FusionModel t, String resourceName) {
                t.init(config, project);
                return t;
            }
        };
        crud.reloadAll();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public FusionModel getFusionModel(String modelId) {
        if (org.apache.commons.lang.StringUtils.isEmpty(modelId)) {
            return null;
        }
        return crud.get(modelId);
    }

    public FusionModel dropModel(String id) {
        val model = getFusionModel(id);
        if (model == null) {
            return null;
        }
        crud.delete(model);
        return model;
    }

    public FusionModel createModel(FusionModel desc) {
        if (desc == null) {
            throw new IllegalArgumentException();
        }
        if (crud.contains(desc.resourceName()))
            throw new IllegalArgumentException("Fusion Model  '" + desc.getAlias() + "' already exists");

        return crud.save(desc);
    }

    public String getModelId(NativeQueryRealization realization) {
        String modelId = realization.getModelId();
        FusionModel fusionModel = getFusionModel(modelId);
        if (!realization.isStreamingLayout() && !Objects.isNull(fusionModel)) {
            NDataModel dataModel = fusionModel.getBatchModel();
            modelId = Optional.ofNullable(dataModel).map(RootPersistentEntity::getId).orElse("");
        }
        return modelId;
    }
}
