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

package org.apache.kylin.rest.signature;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class FactTableRealizationSetCalculator extends RealizationSetCalculator {

    public static final Logger logger = LoggerFactory.getLogger(FactTableRealizationSetCalculator.class);

    /**
     * In case that cube selection result changes after a new cube's data is ready,
     * the cache result should be invalidated, which requires the related signature should be changed.
     * To achieve this, we need to consider all of those cubes who shares the same fact table
     */
    @Override
    protected Set<String> getRealizations(KylinConfig config, String cubes, ProjectInstance project) {
        Set<String> realizations = super.getRealizations(config, cubes, project);
        if (realizations == null) {
            return null;
        }
        Set<String> factTables = Sets.newHashSet();
        for (String realName : realizations) {
            IRealization realInstance = getRealization(config, realName);
            String factTable = getRootFactTableForRealization(realInstance);
            if (factTable != null) {
                factTables.add(factTable);
            }
        }

        Set<String> ret = Sets.newHashSet(realizations);
        for (RealizationEntry entry : project.getRealizationEntries()) {
            String realName = entry.getRealization();
            IRealization realInstance = getRealization(config, realName, entry.getType());
            String factTableForEntry = getRootFactTableForRealization(realInstance);
            if (factTableForEntry != null) {
                if (factTables.contains(factTableForEntry)) {
                    ret.add(realName);
                }
            }
        }
        return ret;
    }

    private String getRootFactTableForRealization(IRealization realization) {
        if (realization == null) {
            logger.warn("Cannot find realization %s", realization);
            return null;
        }
        DataModelDesc model = realization.getModel();
        if (model == null) {
            logger.warn("The model for realization %s is null", realization.getName());
            return null;
        }
        TableRef rootFactTable = model.getRootFactTable();
        if (rootFactTable == null) {
            logger.warn("The root table for model %s is null", model.getName());
            return null;
        }
        return rootFactTable.getTableIdentity();
    }

    private IRealization getRealization(KylinConfig config, String name, RealizationType type) {
        switch (type) {
        case CUBE:
            return CubeManager.getInstance(config).getCube(name);
        case HYBRID:
            return HybridManager.getInstance(config).getHybridInstance(name);
        default:
            return getRealization(config, name);
        }
    }

    private IRealization getRealization(KylinConfig config, String name) {
        HybridInstance hybridInstance = HybridManager.getInstance(config).getHybridInstance(name);
        if (hybridInstance != null) {
            return hybridInstance;
        }
        return CubeManager.getInstance(config).getCube(name);
    }
}
