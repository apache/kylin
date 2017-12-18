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

package org.apache.kylin.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class RetryableTestExecutable extends BaseTestExecutable {
    private static final Logger logger = LoggerFactory.getLogger(RetryableTestExecutable.class);

    public RetryableTestExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) {
        logger.debug("run retryable exception test. ");
        String[] exceptions = KylinConfig.getInstanceFromEnv().getJobRetryExceptions();
        Throwable ex = null;
        if (exceptions != null && exceptions[0] != null) {
            try {
                ex = (Throwable) Class.forName(exceptions[0]).newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new ExecuteResult(ExecuteResult.State.ERROR, null, ex);
    }
}
