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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.AddressUtil;
import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("epochService")
public class EpochService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(EpochService.class);

    @Autowired(required = false)
    public AclEvaluate aclEvaluate;

    public void updateEpoch(List<String> projects, boolean force, boolean client) {
        if (!client)
            aclEvaluate.checkIsGlobalAdmin();

        EpochManager epochMgr = EpochManager.getInstance();

        NProjectManager projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (projects.isEmpty()) {
            projects.add(EpochManager.GLOBAL);
            projects.addAll(
                    projectMgr.listAllProjects().stream().map(ProjectInstance::getName).collect(Collectors.toList()));
        }
        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (String project : projects) {
            if (!rgManager.instanceHasPermissionToOwnEpochTarget(project, AddressUtil.getLocalInstance())) {
                continue;
            }
            logger.info("update epoch {}", project);
            epochMgr.updateEpochWithNotifier(project, force);
        }
    }

    public void updateAllEpochs(boolean force, boolean client) {
        if (!client)
            aclEvaluate.checkIsGlobalAdmin();
        NProjectManager projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<String> prjs = projectMgr.listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toList());
        prjs.add(UnitOfWork.GLOBAL_UNIT);
        updateEpoch(prjs, force, client);
    }

    public boolean isMaintenanceMode() {
        EpochManager epochMgr = EpochManager.getInstance();
        return epochMgr.isMaintenanceMode();
    }
}
