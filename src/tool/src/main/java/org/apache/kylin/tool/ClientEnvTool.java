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

import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientEnvTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String CLI_VERSION = "version";

    private static final String LINUX_DEFRAG = "/sys/kernel/mm/transparent_hugepage/defrag";
    private static final String LINUX_SWAP = "/proc/sys/vm/swappiness";
    private static final String LINUX_CPU = "/proc/cpuinfo";

    public ClientEnvTool() {
        super();
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) {
        // dump os info
        addFile(new File(LINUX_DEFRAG), new File(exportDir, "linux/transparent_hugepage"));
        addFile(new File(LINUX_SWAP), new File(exportDir, "linux/swappiness"));
        addFile(new File(LINUX_CPU), new File(exportDir, "linux"));

        File linuxDir = new File(exportDir, "linux");
        addShellOutput("lsb_release -a", linuxDir, "lsb_release");
        addShellOutput("df -h", linuxDir, "disk_usage");
        addShellOutput("free -m", linuxDir, "mem_usage_mb");
        addShellOutput("top -b -n 1 | head -n 30", linuxDir, "top");
        addShellOutput("ps aux|grep kylin", linuxDir, "kylin_processes");

        if (!getKapConfig().isCloud()) {
            // dump hadoop env
            addShellOutput("hadoop version", new File(exportDir, "hadoop"), CLI_VERSION);
            addShellOutput("hive --version", new File(exportDir, "hive"), CLI_VERSION, false, true);
            addShellOutput("beeline -n1 -p1 -e\"select 1;\"", new File(exportDir, "hive"), "beeline_version", false,
                    true);

            // include klist command output
            addShellOutput("klist", new File(exportDir, "kerberos"), "klist", false, true);
        }
    }

    protected void extractInfoByCmd(String cmd, File destFile) {
        try {
            if (!destFile.exists() && !destFile.createNewFile()) {
                logger.error("Failed to createNewFile destFile.");
            }

            logger.info("The command is: {}", cmd);
            getCmdExecutor().execute(cmd, null);
        } catch (Exception e) {
            logger.error("Failed to execute copyCmd", e);
        }
    }

}
