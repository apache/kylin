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
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;

import com.google.common.collect.Sets;

public class FactTableRealizationSetCalculator extends RealizationSetCalculator {

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
        for (String realization : realizations) {
            String factTable = getFactTablesForRealization(config, realization);
            if (factTable != null) {
                factTables.add(factTable);
            }
        }
        Set<String> ret = Sets.newHashSet(realizations);
        for (RealizationEntry entry : project.getRealizationEntries()) {
            String realization = entry.getRealization();
            switch (entry.getType()) {
            case CUBE:
                CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(realization);
                if (cubeInstance != null) {
                    if (factTables.contains(cubeInstance.getRootFactTable())) {
                        ret.add(realization);
                    }
                }
                break;
            case HYBRID:
                HybridInstance hybridInstance = HybridManager.getInstance(config).getHybridInstance(realization);
                if (hybridInstance != null && hybridInstance.getModel() != null) {
                    if (factTables.contains(hybridInstance.getModel().getRootFactTable().getTableIdentity())) {
                        ret.add(realization);
                    }
                }
                break;
            default:
            }
        }
        return ret;
    }

    private String getFactTablesForRealization(KylinConfig config, String name) {
        HybridInstance hybridInstance = HybridManager.getInstance(config).getHybridInstance(name);
        if (hybridInstance != null) {
            return hybridInstance.getModel().getRootFactTable().getTableIdentity();
        }
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(name);
        if (cubeInstance != null) {
            return cubeInstance.getRootFactTable();
        }
        return null;
    }
}
