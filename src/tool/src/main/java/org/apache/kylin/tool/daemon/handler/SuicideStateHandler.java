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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.HandleResult;
import org.apache.kylin.tool.daemon.HandleStateEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuicideStateHandler extends AbstractCheckStateHandler {
    private static final Logger logger = LoggerFactory.getLogger(SuicideStateHandler.class);

    @Override
    public HandleResult doHandle(CheckResult checkResult) {
        logger.info("Start to suicide ...");

        String pidFile = getKylinHome() + "/kgid";

        File file = new File(pidFile);
        try {
            if (Files.deleteIfExists(file.toPath())) {
                logger.info("Deleted file: {}", pidFile);
            } else {
                logger.warn("Can not delete the file: {}", pidFile);
            }
        } catch (IOException e) {
            logger.error("Failed to delete the file: {}", pidFile);
        }

        Unsafe.systemExit(0);
        return new HandleResult(HandleStateEnum.STOP_CHECK);
    }
}
