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

package org.apache.kylin.source.hive;


import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CreateFlatHiveTableByLivyStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(CreateFlatHiveTableByLivyStep.class);
    protected final PatternedLogger stepLogger = new PatternedLogger(logger);

    protected void createFlatHiveTable(KylinConfig config) throws Exception {
        ImmutableList<String> sqls = ImmutableList.of(getInitStatement(), getCreateTableStatement());
        ExecutableManager executableManager = getManager();
        String jobId = getId();
        MRHiveDictUtil.runLivySqlJob(stepLogger, config, sqls, executableManager, jobId);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        stepLogger.setILogListener((infoKey, info) -> {
                    // only care two properties here
                    if (ExecutableConstants.YARN_APP_ID.equals(infoKey)
                            || ExecutableConstants.YARN_APP_URL.equals(infoKey)) {
                        getManager().addJobInfo(getId(), info);
                    }
                }
        );
        KylinConfig config = getCubeSpecificConfig();
        try {
            createFlatHiveTable(config);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog(), e);
        }
    }

    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    public String getInitStatement() {
        return getParam("HiveInit");
    }

    public void setCreateTableStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    public String getCreateTableStatement() {
        return getParam("HiveRedistributeData");
    }
}
