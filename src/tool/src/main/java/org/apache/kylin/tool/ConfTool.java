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
import java.nio.file.Files;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class ConfTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final Set<String> KYLIN_BIN_INCLUSION = Sets.newHashSet("kylin.sh");

    private ConfTool() {
    }

    public static void extractConf(File exportDir) {
        try {
            File confDir = new File(ToolUtil.getConfFolder());
            if (confDir.exists()) {
                FileUtils.copyDirectoryToDirectory(confDir, exportDir);
            } else {
                logger.error("Can not find the /conf dir: {}!", confDir.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.warn("Failed to copy /conf, ", e);
        }
    }

    public static void extractHadoopConf(File exportDir) {
        try {
            File hadoopConfDir = new File(ToolUtil.getHadoopConfFolder());
            if (hadoopConfDir.exists()) {
                FileUtils.copyDirectoryToDirectory(hadoopConfDir, exportDir);
            } else {
                logger.error("Can not find the hadoop_conf: {}!", hadoopConfDir.getAbsolutePath());
            }
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String buildHadoopConf = kylinConfig.getBuildConf();
            if (StringUtils.isNotEmpty(buildHadoopConf)) {
                File buildHadoopConfDir = new File(buildHadoopConf);
                if (buildHadoopConfDir.exists()) {
                    FileUtils.copyDirectoryToDirectory(buildHadoopConfDir, exportDir);
                } else {
                    logger.error("Can not find the write hadoop_conf: {}!", buildHadoopConfDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to copy /hadoop_conf, ", e);
        }
    }

    public static void extractBin(File exportDir) {
        File destBinDir = new File(exportDir, "bin");

        try {
            FileUtils.forceMkdir(destBinDir);

            File srcBinDir = new File(ToolUtil.getBinFolder());
            if (srcBinDir.exists()) {
                File[] binFiles = srcBinDir.listFiles();
                if (null != binFiles) {
                    for (File binFile : binFiles) {
                        String binFileName = binFile.getName();
                        if (KYLIN_BIN_INCLUSION.contains(binFileName)) {
                            Files.copy(binFile.toPath(), new File(destBinDir, binFile.getName()).toPath());
                            logger.info("copy file: {} {}", binFiles, destBinDir);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to export bin.", e);
        }
    }

}
