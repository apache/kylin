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

package org.apache.kylin.job.common;

import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.LoggerFactory;

import lombok.val;

/**
 */
public class ShellExecutable extends AbstractExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ShellExecutable.class);

    private static final String CMD = "cmd";

    public ShellExecutable() {
        super();
    }

    public ShellExecutable(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected ExecuteResult doWork(JobContext context) {
        try {
            logger.info("executing:{}", getCmd());
            val patternedLogger = new BufferedLogger(logger);
            val result = context.getKylinConfig().getCliCommandExecutor().execute(getCmd(), patternedLogger);

            Preconditions.checkState(result.getCode() == 0);
            return ExecuteResult.createSucceed(result.getCmd());
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    public void setCmd(String cmd) {
        setParam(CMD, cmd);
    }

    public String getCmd() {
        return getParam(CMD);
    }

}
