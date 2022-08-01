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
package org.apache.kylin.tool;

import java.io.File;
import java.util.Locale;

import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonInfoTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static ClientEnvTool clientEnvTool = new ClientEnvTool();

    private CommonInfoTool() {
    }

    public static void exportClientInfo(File exportDir) {
        try {
            String[] clientArgs = { "-destDir", new File(exportDir, "client").getAbsolutePath(), "-compress", "false",
                    "-submodule", "true" };

            clientEnvTool.execute(clientArgs);
        } catch (Exception e) {
            logger.error("Failed to extract client env, ", e);
        }
    }

    public static void exportHadoopEnv(File exportDir) {
        try {
            File file = new File(exportDir, "hadoop_env");
            clientEnvTool.extractInfoByCmd("env>" + file.getAbsolutePath(), file);
        } catch (Exception e) {
            logger.warn("Error in export hadoop env, ", e);
        }
    }

    public static void exportKylinHomeDir(File exportDir) {
        try {
            File file = new File(exportDir, "catalog_info");
            String cmd = String.format(Locale.ROOT, "ls -lR %s>%s", ToolUtil.getKylinHome(), file.getAbsolutePath());
            clientEnvTool.extractInfoByCmd(cmd, file);
        } catch (Exception e) {
            logger.error("Error in export KYLIN_HOME dir, ", e);
        }
    }
}
