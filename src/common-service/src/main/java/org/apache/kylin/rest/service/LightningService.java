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

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.SYSTEM_PROFILE_ABNORMAL_DATA;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.SystemProfileResponse;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("lightningService")
public class LightningService extends BasicService {

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public SystemProfileResponse systemProfile() {

        if (!KapConfig.getInstanceFromEnv().isCloud()) {
            SystemProfileResponse systemProfileResponse = new SystemProfileResponse();
            systemProfileResponse.setIsCloud(false);
            return systemProfileResponse;
        }

        try {
            String data = SystemProfileExtractorFactory.create(KylinConfig.getInstanceFromEnv()).getSystemProfile();
            return JsonUtil.readValue(data, SystemProfileResponse.class);
        } catch (Exception e) {
            throw new KylinException(SYSTEM_PROFILE_ABNORMAL_DATA, e);
        }
    }

}
