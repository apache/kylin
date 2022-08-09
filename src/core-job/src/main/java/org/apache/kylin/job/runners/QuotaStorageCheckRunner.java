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
package org.apache.kylin.job.runners;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.storage.ProjectStorageInfoCollector;
import org.apache.kylin.metadata.cube.storage.StorageInfoEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.var;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class QuotaStorageCheckRunner extends AbstractDefaultSchedulerRunner {
    private static final Logger logger = LoggerFactory.getLogger(QuotaStorageCheckRunner.class);

    private final ProjectStorageInfoCollector collector;

    public QuotaStorageCheckRunner(NDefaultScheduler nDefaultScheduler) {
        super(nDefaultScheduler);

        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.STORAGE_QUOTA, StorageInfoEnum.TOTAL_STORAGE);
        collector = new ProjectStorageInfoCollector(storageInfoEnumList);
    }

    @Override
    protected void doRun() {
        logger.info("start check project {} storage quota.", nDefaultScheduler.getProject());
        context.setReachQuotaLimit(reachStorageQuota());
    }

    private boolean reachStorageQuota() {
        var storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(),
                nDefaultScheduler.getProject());
        var totalSize = storageVolumeInfo.getTotalStorageSize();
        int retryCount = 3;
        while (retryCount-- > 0 && totalSize < 0) {
            storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(),
                    nDefaultScheduler.getProject());
            totalSize = storageVolumeInfo.getTotalStorageSize();
        }
        val storageQuotaSize = storageVolumeInfo.getStorageQuotaSize();
        if (totalSize < 0) {
            logger.error(
                    "Project '{}' : an exception occurs when getting storage volume info, no job will be scheduled!!! The error info : {}",
                    nDefaultScheduler.getProject(),
                    storageVolumeInfo.getThrowableMap().get(StorageInfoEnum.TOTAL_STORAGE));
            return true;
        }
        if (totalSize >= storageQuotaSize) {
            logger.info("Project '{}' reach storage quota, no job will be scheduled!!!",
                    nDefaultScheduler.getProject());
            return true;
        }
        return false;
    }
}
