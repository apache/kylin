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

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KEProcessChecker extends AbstractHealthChecker {
    private static final Logger logger = LoggerFactory.getLogger(KEProcessChecker.class);

    public KEProcessChecker() {
        setPriority(0);
    }

    public String getProcessStatusCmd() {
        return "sh " + getKylinHome() + "/sbin/guardian-get-process-status.sh";
    }

    @Override
    CheckResult doCheck() {
        /*
        0: ke is running
        1: ke is stopped
        -1 ke is crashed
         */
        String cmd = getProcessStatusCmd();
        try {
            CliCommandExecutor.CliCmdExecResult result = getCommandExecutor().execute(cmd, null);
            int status = Integer.parseInt(result.getCmd().substring(0, result.getCmd().lastIndexOf('\n')));

            switch (status) {
            case 0:
                return new CheckResult(CheckStateEnum.NORMAL);
            case 1:
                return new CheckResult(CheckStateEnum.SUICIDE, "KE instance is normally stopped");
            case -1:
                return new CheckResult(CheckStateEnum.RESTART, "KE Instance is crashed");
            default:
                return new CheckResult(CheckStateEnum.WARN, "Unknown ke process status");
            }
        } catch (Exception e) {
            logger.error("Check KE process failed, cmd: {}", cmd, e);

            return new CheckResult(CheckStateEnum.WARN,
                    "Execute shell guardian-get-process-status.sh failed. " + e.getMessage());
        }
    }
}
