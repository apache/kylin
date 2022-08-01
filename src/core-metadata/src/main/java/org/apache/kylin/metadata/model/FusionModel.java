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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class FusionModel extends RootPersistentEntity implements Serializable {

    private String project;

    @EqualsAndHashCode.Include
    @JsonProperty("alias")
    private String alias;

    @JsonProperty("model_mapping")
    private Map<String, Map<String, String>> modelMapping = Maps.newHashMap();

    private KylinConfig config;

    public FusionModel() {
        super();
    }

    public FusionModel(FusionModel other) {
        this.uuid = other.uuid;
        this.alias = other.alias;
        this.modelMapping = other.modelMapping;
        this.project = other.project;
        this.createTime = other.createTime;
        this.config = other.config;
        this.lastModified = other.lastModified;
        this.version = other.version;
    }

    public FusionModel(NDataModel model, NDataModel batchModel) {
        this.uuid = model.getUuid();
        this.project = model.getProject();
        this.config = model.getConfig();
        this.modelMapping.put(model.getUuid(), null);
        Map<String, String> tableMapping = Maps.newHashMap();
        tableMapping.put(model.getRootFactTableName(), batchModel.getRootFactTableName());
        this.modelMapping.put(batchModel.getUuid(), tableMapping);
    }

    public static String getBatchName(String streamingAlias, String modelId) {
        return streamingAlias + "_" + modelId.substring(0, 8);
    }

    public void init(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
        this.setDependencies(calcDependencies());
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        return this.modelMapping.keySet().stream().filter(Objects::nonNull).map(id -> {
            NDataModel model = modelManager.getDataModelDesc(id);
            return model != null ? model : new MissingRootPersistentEntity(TableDesc.concatResourcePath(id, project));
        }).collect(Collectors.toList());
    }

    @Override
    public String resourceName() {
        return uuid;
    }

    public Set<String> getModelsId() {
        return modelMapping.keySet();
    }

    public NDataModel getBatchModel() {
        return getModelByType(NDataModel.ModelType.BATCH);
    }

    public NDataModel getStreamingModel() {
        return getModelByType(NDataModel.ModelType.HYBRID);
    }

    public String getAlias() {
        return getStreamingModel().getAlias();
    }

    public NDataModel getModelByType(NDataModel.ModelType modelType) {
        NDataModelManager modelManager = NDataModelManager.getInstance(config, project);
        for (String modelId : getModelsId()) {
            NDataModel model = modelManager.getDataModelDesc(modelId);
            if (model.getModelType() == modelType) {
                return model;
            }
        }
        return null;
    }
}
