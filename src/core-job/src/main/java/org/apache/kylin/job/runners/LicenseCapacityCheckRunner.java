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

import static org.apache.kylin.common.exception.CommonErrorCode.LICENSE_OVER_CAPACITY;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class LicenseCapacityCheckRunner extends AbstractDefaultSchedulerRunner {
    private static final Logger logger = LoggerFactory.getLogger(LicenseCapacityCheckRunner.class);

    public LicenseCapacityCheckRunner(NDefaultScheduler nDefaultScheduler) {
        super(nDefaultScheduler);
    }

    @Override
    protected void doRun() {
        logger.info("start check license capacity for project {}", project);
        context.setLicenseOverCapacity(isLicenseOverCapacity());
    }

    private boolean isLicenseOverCapacity() {
        val sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());

        try {
            sourceUsageManager.checkIsOverCapacity(project);
        } catch (KylinException e) {
            if (LICENSE_OVER_CAPACITY.toErrorCode() == e.getErrorCode()) {
                logger.warn("Source usage over capacity, no job will be scheduled.", e);
                return true;
            }
        } catch (Throwable e) {
            logger.warn("Check source usage over capacity failed.", e);
        }

        // not over capacity
        return false;
    }
}
