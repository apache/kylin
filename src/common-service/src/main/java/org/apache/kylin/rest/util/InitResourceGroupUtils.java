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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InitResourceGroupUtils {
    private InitResourceGroupUtils() {
        // nothing
    }

    public static void initResourceGroup() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            ResourceGroupManager resourceGroupManager = ResourceGroupManager
                    .getInstance(KylinConfig.getInstanceFromEnv());
            if (!resourceGroupManager.resourceGroupInitialized()) {
                resourceGroupManager.initResourceGroup();
                log.info("Init resource group.");
            }
            return null;
        }, UnitOfWork.GLOBAL_UNIT);
    }
}
