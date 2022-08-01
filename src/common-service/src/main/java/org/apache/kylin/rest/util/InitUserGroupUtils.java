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

package org.apache.kylin.rest.util;

import static org.apache.kylin.rest.util.CreateAdminUserUtils.PROFILE_DEFAULT;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.usergroup.NUserGroupManager;
import org.springframework.core.env.Environment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitUserGroupUtils {

    public static void initUserGroups(Environment env) {
        String[] groupNames = new String[] { Constant.GROUP_ALL_USERS, Constant.ROLE_ADMIN, Constant.ROLE_MODELER,
                Constant.ROLE_ANALYST };
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NUserGroupManager userGroupManager = NUserGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
            if (userGroupManager.getAllGroups().isEmpty() && env.acceptsProfiles(PROFILE_DEFAULT)) {
                for (String groupName : groupNames) {
                    userGroupManager.add(groupName);
                    log.info("Init user group {}.", groupName);
                }
            }
            return null;
        }, UnitOfWork.GLOBAL_UNIT);

    }
}
