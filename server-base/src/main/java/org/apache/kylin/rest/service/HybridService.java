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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.tool.HybridCubeCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

@Component("hybridService")
public class HybridService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(HybridService.class);

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or " + Constant.ACCESS_HAS_ROLE_MODELER)
    public HybridInstance createHybridCube(String hybridName, String projectName, String modelName, String[] cubeNames) {
        List<String> args = new ArrayList<String>();
        args.add("-name");
        args.add(hybridName);
        args.add("-project");
        args.add(projectName);
        args.add("-model");
        args.add(modelName);
        args.add("-cubes");
        args.add(StringUtils.join(cubeNames, ","));
        args.add("-action");
        args.add("create");
        try {
            HybridCubeCLI.main(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            logger.warn("Create Hybrid Failed", e);
            throw e;
        }
        return getHybridInstance(hybridName);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public HybridInstance updateHybridCube(String hybridName, String projectName, String modelName, String[] cubeNames) {
        List<String> args = new ArrayList<String>();
        args.add("-name");
        args.add(hybridName);
        args.add("-project");
        args.add(projectName);
        args.add("-model");
        args.add(modelName);
        args.add("-cubes");
        args.add(StringUtils.join(cubeNames, ","));
        args.add("-action");
        args.add("update");
        try {
            HybridCubeCLI.main(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            logger.warn("Update Hybrid Failed", e);
            throw e;
        }
        return getHybridInstance(hybridName);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteHybridCube(String hybridName, String projectName, String modelName) {
        List<String> args = new ArrayList<String>();
        args.add("-name");
        args.add(hybridName);
        args.add("-project");
        args.add(projectName);
        args.add("-model");
        args.add(modelName);
        args.add("-action");
        args.add("delete");
        try {
            HybridCubeCLI.main(args.toArray(new String[args.size()]));
        } catch (Exception e) {
            logger.warn("Delete Hybrid Failed", e);
            throw e;
        }
    }

    public HybridInstance getHybridInstance(String hybridName) {
        HybridInstance hybridInstance = getHybridManager().getHybridInstance(hybridName);
        return hybridInstance;
    }

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<HybridInstance> listHybrids(final String projectName, final String modelName) {
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;
        List<HybridInstance> hybridsInProject = new ArrayList<HybridInstance>();

        if (StringUtils.isEmpty(projectName)) {
            hybridsInProject = new ArrayList(getHybridManager().listHybridInstances());
        } else if (project == null) {
            return new ArrayList<>();
        } else {
            List<RealizationEntry> realizationEntries = project.getRealizationEntries(RealizationType.HYBRID);
            if (realizationEntries != null) {
                for (RealizationEntry entry : realizationEntries) {
                    HybridInstance instance = getHybridManager().getHybridInstance(entry.getRealization());
                    hybridsInProject.add(instance);
                }
            }
        }

        DataModelDesc model = (null != modelName) ? getMetadataManager().getDataModelDesc(modelName) : null;
        if (StringUtils.isEmpty(modelName)) {
            return hybridsInProject;
        } else if (model == null) {
            return new ArrayList<>();
        } else {
            List<HybridInstance> hybridsInModel = new ArrayList<HybridInstance>();
            for (HybridInstance hybridInstance : hybridsInProject) {
                boolean hybridInModel = false;
                for (RealizationEntry entry : hybridInstance.getRealizationEntries()) {
                    CubeDesc cubeDesc = getCubeDescManager().getCubeDesc(entry.getRealization());
                    if (cubeDesc != null && model.getName().equalsIgnoreCase(cubeDesc.getModel().getName())) {
                        hybridInModel = true;
                        break;
                    }
                }
                if (hybridInModel) {
                    hybridsInModel.add(hybridInstance);
                }
            }
            return hybridsInModel;
        }
    }

}
