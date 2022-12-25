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

package org.apache.kylin.rest.security;

import java.util.List;
import java.util.Objects;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.upgrade.GlobalAclVersionManager;
import org.apache.kylin.rest.service.UserAclService;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class AdminUserAspect {
    private List<String> adminUserList;

    @Autowired
    @Qualifier("userAclService")
    private UserAclService userAclService;

    private boolean superAdminInitialized = false;

    private boolean isUpgraded() {
        val versionManager = GlobalAclVersionManager.getInstance(KylinConfig.getInstanceFromEnv());
        return versionManager.exists();
    }

    private boolean isAdminUserUpgraded() {
        val userAclManager = UserAclManager.getInstance(KylinConfig.getInstanceFromEnv());
        return userAclManager.listAclUsernames().size() > 0;
    }

    @AfterReturning(value = "execution(* org.apache.kylin.rest.service.OpenUserService.listAdminUsers(..))", returning = "adminUserList")
    public void doAfterListAdminUsers(List<String> adminUserList) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        if (CollectionUtils.isEmpty(adminUserList)) {
            return;
        }
        // upgrade admin user acl from job node
        if (kylinConfig.isJobNode() && isUpgraded() && !isAdminUserUpgraded()) {
            userAclService.syncAdminUserAcl(adminUserList, false);
        }

        // add the acl for super admin.
        if (kylinConfig.isJobNode() && !superAdminInitialized) {
            userAclService.syncSuperAdminUserAcl();
            superAdminInitialized = true;
            return;
        }

        if (Objects.isNull(this.adminUserList)
                || !CollectionUtils.isEqualCollection(adminUserList, this.adminUserList)) {
            EventBusFactory.getInstance().postSync(new AdminUserSyncEventNotifier(adminUserList, true));
            this.adminUserList = adminUserList;
        }
    }

}
