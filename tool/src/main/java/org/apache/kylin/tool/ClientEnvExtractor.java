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
 *
 */

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class ClientEnvExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(ClientEnvExtractor.class);
    private KylinConfig kylinConfig;
    private CliCommandExecutor cmdExecutor;

    public ClientEnvExtractor() throws IOException {
        super();

        packageType = "client";
        kylinConfig = KylinConfig.getInstanceFromEnv();
        cmdExecutor = kylinConfig.getCliCommandExecutor();
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        // dump os info
        addLocalFile("/sys/kernel/mm/transparent_hugepage/defrag", "linux/transparent_hugepage");
        addLocalFile("/proc/sys/vm/swappiness", "linux/swappiness");
        addLocalFile("/proc/cpuinfo", "linux");
        addShellOutput("lsb_release -a", "linux", "lsb_release");
        addShellOutput("df -h", "linux", "disk_usage");
        addShellOutput("free -m", "linux", "mem_usage_mb");
        addShellOutput("top -b -n 1 | head -n 30", "linux", "top");
        addShellOutput("ps aux|grep kylin", "linux", "kylin_processes");

        // dump hadoop env
        addShellOutput("hadoop version", "hadoop", "version");
        addShellOutput("hbase version", "hbase", "version");
        addShellOutput("hive --version", "hive", "version");
        addShellOutput("beeline --version", "hive", "beeline_version");
    }

    private void addLocalFile(String src, String destDir) {
        try {
            File srcFile = new File(src);
            File destDirFile = null;
            if (!StringUtils.isEmpty(destDir)) {
                destDirFile = new File(exportDir, destDir);
                FileUtils.forceMkdir(destDirFile);
            } else {
                destDirFile = exportDir;
            }
            FileUtils.forceMkdir(destDirFile);
            Files.copy(srcFile, new File(destDirFile, srcFile.getName()));
        } catch (Exception e) {
            logger.warn("Failed to copy " + src + ".", e);
        }
    }

    private void addShellOutput(String cmd, String destDir, String filename) {
        try {
            File destDirFile = null;
            if (!StringUtils.isEmpty(destDir)) {
                destDirFile = new File(exportDir, destDir);
                FileUtils.forceMkdir(destDirFile);
            } else {
                destDirFile = exportDir;
            }
            Pair<Integer, String> result = cmdExecutor.execute(cmd);
            String output = result.getSecond();
            FileUtils.writeStringToFile(new File(destDirFile, filename), output, Charset.defaultCharset());
        } catch (Exception e) {
            logger.warn("Failed to run command: " + cmd + ".", e);
        }
    }
}
