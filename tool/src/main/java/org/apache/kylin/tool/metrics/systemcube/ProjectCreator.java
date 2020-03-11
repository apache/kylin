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

package org.apache.kylin.tool.metrics.systemcube;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectStatusEnum;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metrics.MetricsManager;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ProjectCreator {

    private static final String SYSTEM_PROJECT_DESC = "This project is kylin's system project for kylin metrics";

    public static ProjectInstance generateKylinProjectInstance(String owner, List<TableDesc> kylinTables,
            List<DataModelDesc> kylinModels, List<CubeDesc> kylinCubeDescs) {
        ProjectInstance projectInstance = new ProjectInstance();

        projectInstance.setName(MetricsManager.SYSTEM_PROJECT);
        projectInstance.setOwner(owner);
        projectInstance.setDescription(SYSTEM_PROJECT_DESC);
        projectInstance.setStatus(ProjectStatusEnum.ENABLED);
        projectInstance.setCreateTimeUTC(0L);
        projectInstance.updateRandomUuid();

        if (kylinTables != null)
            projectInstance.setTables(Sets.newHashSet(Lists.transform(kylinTables, new Function<TableDesc, String>() {
                @Nullable
                @Override
                public String apply(@Nullable TableDesc tableDesc) {
                    if (tableDesc != null) {
                        return tableDesc.getIdentity();
                    }
                    return null;
                }
            })));
        else
            projectInstance.setTables(Sets.<String> newHashSet());

        if (kylinModels != null)
            projectInstance.setModels(Lists.transform(kylinModels, new Function<DataModelDesc, String>() {
                @Nullable
                @Override
                public String apply(@Nullable DataModelDesc modelDesc) {
                    if (modelDesc != null) {
                        return modelDesc.getName();
                    }
                    return null;
                }
            }));
        else
            projectInstance.setModels(Lists.<String> newArrayList());

        if (kylinCubeDescs != null)
            projectInstance
                    .setRealizationEntries(Lists.transform(kylinCubeDescs, new Function<CubeDesc, RealizationEntry>() {
                        @Nullable
                        @Override
                        public RealizationEntry apply(@Nullable CubeDesc cubeDesc) {
                            if (cubeDesc != null) {
                                RealizationEntry entry = new RealizationEntry();
                                entry.setRealization(cubeDesc.getName());
                                entry.setType(RealizationType.CUBE);
                                return entry;
                            }
                            return null;
                        }
                    }));
        else
            projectInstance.setRealizationEntries(Lists.<RealizationEntry> newArrayList());

        return projectInstance;
    }
}
