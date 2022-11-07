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

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.MAINTENANCE_MODE_ENTER_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.MAINTENANCE_MODE_LEAVE_FAILED;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.rest.response.MaintenanceModeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("maintenanceModeService")
public class MaintenanceModeService extends BasicService implements MaintenanceModeSupporter {

    @Autowired
    private AclEvaluate aclEvaluate;

    private static final Logger logger = LoggerFactory.getLogger(MaintenanceModeService.class);

    public void setMaintenanceMode(String reason) {
        aclEvaluate.checkIsGlobalAdmin();
        EpochManager epochMgr = EpochManager.getInstance();
        if (Boolean.FALSE.equals(epochMgr.setMaintenanceMode(reason))) {
            throw new KylinException(MAINTENANCE_MODE_ENTER_FAILED);
        }
        logger.info("System enter maintenance mode.");
    }

    public void unsetMaintenanceMode(String reason) {
        aclEvaluate.checkIsGlobalAdmin();
        EpochManager epochMgr = EpochManager.getInstance();
        if (Boolean.FALSE.equals(epochMgr.unsetMaintenanceMode(reason))) {
            throw new KylinException(MAINTENANCE_MODE_LEAVE_FAILED);
        }
        logger.info("System leave maintenance mode.");
    }

    public MaintenanceModeResponse getMaintenanceMode() {
        EpochManager epochMgr = EpochManager.getInstance();
        Pair<Boolean, String> maintenanceModeDetail = epochMgr.getMaintenanceModeDetail();
        return new MaintenanceModeResponse(maintenanceModeDetail.getFirst(), maintenanceModeDetail.getSecond());
    }

    @Override
    public boolean isMaintenanceMode() {
        return getMaintenanceMode().isMaintenanceMode();
    }
}
