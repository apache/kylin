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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.NDataModelManager;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class MockSparkTestExecutable extends NSparkExecutable {

    private String metaUrl;

    public MockSparkTestExecutable() {
        super();
    }

    public MockSparkTestExecutable(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {

        NDataModelManager modelManager = NDataModelManager.getInstance(config, "default");
        Set<String> dumpSet = modelManager.listAllModelIds().stream().map(this::getResourcePath)
                .collect(Collectors.toSet());
        return dumpSet;
    }

    private String getResourcePath(String modelId) {
        return "/default" + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT + "/" + modelId + MetadataConstants.FILE_SURFIX;
    }

    @Override
    public String getDistMetaUrl() {
        return getMetaUrl();
    }
}
