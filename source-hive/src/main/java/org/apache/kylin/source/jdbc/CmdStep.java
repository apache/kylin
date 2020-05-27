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
package org.apache.kylin.source.jdbc;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class CmdStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CmdStep.class);
    private final PatternedLogger stepLogger = new PatternedLogger(logger);

    public void setCmd(String cmd) {
        setParam("cmd", cmd);
    }

    public CmdStep() {
    }

    protected void sqoopFlatHiveTable(KylinConfig config) throws IOException {
        String cmd = getParam("cmd");
        stepLogger.log(String.format(Locale.ROOT, "exe cmd:%s", cmd));
        Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger, null);
        getManager().addJobInfo(getId(), stepLogger.getInfo());
        if (response.getFirst() != 0) {
            throw new RuntimeException("Failed to create flat hive table, error code " + response.getFirst());
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try {
            sqoopFlatHiveTable(config);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog(), e);
        }
    }

}
