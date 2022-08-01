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
package org.apache.kylin.tool.daemon.handler;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.HandleResult;
import org.apache.kylin.tool.daemon.HandleStateEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartStateHandler extends AbstractCheckStateHandler {
    private static final Logger logger = LoggerFactory.getLogger(RestartStateHandler.class);

    @Override
    public HandleResult doHandle(CheckResult checkResult) {
        logger.info("Start to restart instance port[{}] ...", getServerPort());
        String cmd = "nohup sh " + KylinConfig.getKylinHome() + "/bin/kylin.sh restart > /dev/null 2>&1 &";
        try {
            getCommandExecutor().execute(cmd, null);
            logger.info("Success to restart instance port[{}] ...", getServerPort());
        } catch (Exception e) {
            logger.error("Failed to restart the instance port [{}], cmd: {}", getServerPort(), cmd);
        }

        return new HandleResult(HandleStateEnum.STOP_CHECK);
    }
}
