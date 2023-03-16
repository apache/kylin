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

package org.apache.kylin.metadata.cube.model;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import lombok.val;

public class NSegmentConfigHelper {

    final static ObjectMapper mapper = new ObjectMapper();

    public static SegmentConfig getModelSegmentConfig(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel dataModel = NDataModelManager.getInstance(kylinConfig, project).getDataModelDesc(model);
        val segmentConfigInModel = dataModel.getSegmentConfig();
        val segmentConfigInProject = getFromProject(project, kylinConfig);
        val managementType = dataModel.getManagementType();
        switch (managementType) {
        case MODEL_BASED:
            return mergeConfig(segmentConfigInModel, segmentConfigInProject);
        case TABLE_ORIENTED:
            val segmentConfigInDataLoadingRange = getTableSegmentConfig(project, dataModel.getRootFactTableName());
            return mergeConfig(segmentConfigInModel, segmentConfigInDataLoadingRange);
        default:
            return null;
        }
    }

    public static SegmentConfig getTableSegmentConfig(String project, String table) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val segmentConfigInProject = getFromProject(project, kylinConfig);
        val segmentConfigInDataLoadingRange = getFromDataLoadingRange(project, kylinConfig, table);
        return mergeConfig(segmentConfigInDataLoadingRange, segmentConfigInProject);
    }

    private static SegmentConfig mergeConfig(SegmentConfig firstSegmentConfig, SegmentConfig secondSegmentConfig) {
        if (firstSegmentConfig == null && secondSegmentConfig == null) {
            return null;
        }
        if (firstSegmentConfig == null) {
            return secondSegmentConfig;
        }
        if (secondSegmentConfig == null) {
            return firstSegmentConfig;
        }
        Map<String, Object> firstSegmentConfigMap = mapper.convertValue(firstSegmentConfig, Map.class);
        Map<String, Object> secondSegmentConfigMap = mapper.convertValue(secondSegmentConfig, Map.class);
        secondSegmentConfigMap.entrySet().forEach(entry -> {
            val key = entry.getKey();
            val value = entry.getValue();
            if (firstSegmentConfigMap.get(key) == null) {
                firstSegmentConfigMap.put(key, value);
            }
        });
        return mapper.convertValue(firstSegmentConfigMap, SegmentConfig.class);
    }

    private static SegmentConfig getFromDataLoadingRange(String project, KylinConfig kylinConfig, String table) {
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(kylinConfig, project);
        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        if (dataLoadingRange == null) {
            return null;
        }
        return dataLoadingRange.getSegmentConfig();
    }

    private static SegmentConfig getFromProject(String project, KylinConfig kylinConfig) {
        val projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        Preconditions.checkState(projectInstance != null);
        return projectInstance.getSegmentConfig();
    }

}
