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

package org.apache.kylin.rest.service;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.draft.DraftManager;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.streaming.StreamingManager;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.storage.hybrid.HybridManager;

public abstract class BasicService {

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public TableMetadataManager getTableManager() {
        return TableMetadataManager.getInstance(getConfig());
    }
    
    public DataModelManager getDataModelManager() {
        return DataModelManager.getInstance(getConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public StreamingManager getStreamingManager() {
        return StreamingManager.getInstance(getConfig());
    }

    public KafkaConfigManager getKafkaManager() throws IOException {
        return KafkaConfigManager.getInstance(getConfig());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getConfig());
    }

    public ProjectManager getProjectManager() {
        return ProjectManager.getInstance(getConfig());
    }

    public HybridManager getHybridManager() {
        return HybridManager.getInstance(getConfig());
    }

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public BadQueryHistoryManager getBadQueryHistoryManager() {
        return BadQueryHistoryManager.getInstance(getConfig());
    }
    
    public DraftManager getDraftManager() {
        return DraftManager.getInstance(getConfig());
    }

    public TableACLManager getTableACLManager() {
        return TableACLManager.getInstance(getConfig());
    }

    public MetricsManager getMetricsManager() {
        return MetricsManager.getInstance();
    }
}
