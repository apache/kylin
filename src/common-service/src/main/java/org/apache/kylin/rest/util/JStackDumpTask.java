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
package org.apache.kylin.rest.util;

import java.io.File;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.tool.util.ToolUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JStackDumpTask implements Runnable {
    private final File outputDir;

    public JStackDumpTask() {
        outputDir = new File(KylinConfig.getKylinHome(), "logs");
    }

    public JStackDumpTask(File path) {
        this.outputDir = path;
    }

    @Override
    public void run() {
        log.trace("start dump stack");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        TimestampedRollingFileOutputDir rollingFileOutputDir = new TimestampedRollingFileOutputDir(outputDir,
                "jstack.timed.log", kylinConfig.getJStackDumpTaskLogsMaxNum());
        try {
            ToolUtil.dumpKylinJStack(rollingFileOutputDir.newOutputFile());
            log.trace("dump jstack successful");
        } catch (Exception e) {
            log.error("Error dump jstack info", e);
        }

    }
}
