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
package org.apache.kylin.tool.daemon;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.SecretKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapGuardianHATask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KapGuardianHATask.class);
    private final CliCommandExecutor cli;

    private final String statusCmd;
    private final String startCmd;

    public KapGuardianHATask() {
        this.cli = new CliCommandExecutor();

        String kylinHome = KylinConfig.getKylinHome();
        String pidFile = kylinHome + "/kgid";

        String errLog = kylinHome + "/logs/shell.stderr";
        String outLog = kylinHome + "/logs/shell.stdout";

        this.statusCmd = "sh " + kylinHome + "/sbin/guardian-get-process-status.sh " + pidFile;
        this.startCmd = "nohup sh " + kylinHome + "/bin/guardian.sh start 2>>" + errLog + " | tee -a " + outLog + " &";

        initKGSecretKey();
    }

    private void initKGSecretKey() {
        if (KylinConfig.getInstanceFromEnv().isGuardianEnabled()) {
            try {
                SecretKeyUtil.initKGSecretKey();
            } catch (Exception e) {
                logger.error("init kg secret key failed!", e);
            }
        }
    }

    @Override
    public void run() {
        try {
            CliCommandExecutor.CliCmdExecResult result = cli.execute(statusCmd, null);
            // 0 running, 1 stopped, -1 crashed
            int status = Integer.parseInt(result.getCmd().substring(0, result.getCmd().lastIndexOf('\n')));

            // start kg
            if (0 != status) {
                if (1 == status) {
                    logger.info("Guardian Process is not running, try to start it");
                } else if (-1 == status) {
                    logger.info("Guardian Process is crashed, try to start it");
                }

                logger.info("Starting Guardian Process");

                cli.execute(startCmd, null);

                logger.info("Guardian Process started");
            } else {
                logger.info("Guardian Process is running");
            }
        } catch (Exception e) {
            logger.error("Failed to monitor Guardian Process!", e);
        }
    }
}
