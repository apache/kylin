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

import java.io.IOException;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.ShellException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.LoggerFactory;

/**
 */
public class ShellExecutable extends AbstractExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ShellExecutable.class);

    private static final String CMD = "cmd";

    public ShellExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            logger.info("executing:" + getCmd());
            final PatternedLogger patternedLogger = new PatternedLogger(logger);
            final Pair<Integer, String> result = context.getConfig().getCliCommandExecutor().execute(getCmd(), patternedLogger, null);
            getManager().addJobInfo(getId(), patternedLogger.getInfo());
            return result.getFirst() == 0 ? new ExecuteResult(ExecuteResult.State.SUCCEED, result.getSecond())
                    : ExecuteResult.createFailed(new ShellException(result.getSecond()));
        } catch (IOException e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
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
