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
package org.apache.kylin.tool.daemon.checker;

import java.util.Locale;

import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.apache.kylin.tool.daemon.HealthChecker;
import org.apache.kylin.tool.daemon.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHealthChecker extends Worker implements HealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHealthChecker.class);

    private int priority = Integer.MAX_VALUE;

    @Override
    public int getPriority() {
        return this.priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    protected void preCheck() {
        // nothing to do
    }

    protected void postCheck(CheckResult result) {
        // nothing to do
    }

    abstract CheckResult doCheck();

    @Override
    public CheckResult check() {
        logger.info("Checker:[{}] start to do check ...", this.getClass().getName());

        CheckResult result = null;
        try {
            preCheck();

            result = doCheck();

            postCheck(result);
            logger.info("Checker: [{}], do check finished! ", this.getClass().getName());
        } catch (Exception e) {
            logger.error("Checker: [{}], do check failed! ", this.getClass().getName(), e);
        }

        if (null == result) {
            String message = String.format(Locale.ROOT, "Checker: [%s] check result is null!",
                    this.getClass().getName());
            logger.warn(message);

            result = new CheckResult(CheckStateEnum.OTHER, message);
        }

        if (null == result.getCheckState()) {
            String message = String.format(Locale.ROOT, "Checker: [%s] check result state is null!",
                    this.getClass().getName());
            logger.warn(message);

            result.setCheckState(CheckStateEnum.OTHER);
            if (null == result.getReason()) {
                result.setReason(message);
            }
        }

        if (null == result.getReason()) {
            result.setReason("");
        }

        if (null == result.getCheckerName()) {
            result.setCheckerName(this.getClass().getName());
        }

        return result;
    }

}
