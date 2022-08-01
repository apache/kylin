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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.utils.HiveTransactionTableHelper.generateDropTableStatement;
import static org.apache.kylin.engine.spark.utils.HiveTransactionTableHelper.generateHiveInitStatements;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkCleanupTransactionalTableStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(SparkCleanupTransactionalTableStep.class);

    public SparkCleanupTransactionalTableStep() {
        this.setName(ExecutableConstants.STEP_INTERMEDIATE_TABLE_CLEANUP);
    }

    public SparkCleanupTransactionalTableStep(Object notSetId) {
        super(notSetId);
    }

    public static String generateDropTableCommand(String tableFullName) {
        // By default, the tableFullName information obtained is the full path of the table，
        // eg: TEST_CDP.TEST_HIVE_TX_INTERMEDIATE5c5851ef8544
        if (StringUtils.isEmpty(tableFullName)) {
            logger.info("The table name is empty.");
            return "";
        }

        String tableName = tableFullName;
        if (tableFullName.contains(".") && !tableFullName.endsWith(".")) {
            String database = tableFullName.substring(0, tableFullName.indexOf("."));
            tableName = tableFullName.substring(tableFullName.indexOf(".") + 1);
            return generateHiveInitStatements(database) + generateDropTableStatement(tableName);
        }
        return generateDropTableStatement(tableName);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String jobId = getParam(NBatchConstants.P_JOB_ID);
        String dir = config.getJobTmpTransactionalTableDir(getProject(), jobId);
        logger.info("should clean dir :{} ", dir);
        Path path = new Path(dir);
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                for (FileStatus fileStatus : fs.listStatus(path)) {
                    final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(config);
                    String tableFullName = fileStatus.getPath().getName();
                    hiveCmdBuilder.addStatement(generateDropTableCommand(tableFullName));
                    final String cmd = hiveCmdBuilder.toString();
                    doExecuteCliCommand(tableFullName, cmd);
                }
                fs.delete(path, true);
            }
        } catch (IOException e) {
            logger.error("Can not delete intermediate table.", e);
            throw new ExecuteException("Can not delete intermediate table");
        }

        return ExecuteResult.createSucceed();
    }

    private void doExecuteCliCommand(String tableFullName, String cmd) {
        if (StringUtils.isEmpty(cmd)) {
            return;
        }
        try {
            CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult result = cliCommandExecutor.execute(cmd, null);
            if (result.getCode() != 0) {
                logger.error("execute drop intermediate table return fail, table : {}, code :{}", tableFullName,
                        result.getCode());
            } else {
                logger.info("execute drop intermediate table succeeded, table :{} ", tableFullName);
            }
        } catch (ShellException e) {
            logger.error(String.format(Locale.ROOT, "execute drop intermediate table error, table :%s ", tableFullName),
                    e);
        }
    }

}
