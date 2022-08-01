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
package org.apache.kylin.tool.util;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerInfoUtil {
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String UNKNOWN = "UNKNOWN";
    private static final String COMMIT_SHA1_V15 = "commit_SHA1";
    private static final String COMMIT_SHA1_V13 = "commit.sha1";
    private ServerInfoUtil() {
    }

    public static String getKylinClientInformation() {
        StringBuilder buf = new StringBuilder();

        String gitCommit = getGitCommitInfo();
        String kylinHome = KylinConfig.getKylinHome();

        buf.append("kylin.home: ").append(kylinHome == null ? UNKNOWN : new File(kylinHome).getAbsolutePath())
                .append("\n");

        // kap versions
        String kapVersion = null;
        try {
            File versionFile = new File(kylinHome, "VERSION");
            if (versionFile.exists()) {
                kapVersion = FileUtils.readFileToString(versionFile).trim();
            }
        } catch (Exception e) {
            logger.error("Failed to get kap.version. ", e);
        }
        buf.append("kap.version:").append(kapVersion == null ? UNKNOWN : kapVersion).append("\n");

        // others
        buf.append("commit:").append(gitCommit).append("\n");
        buf.append("os.name:").append(System.getProperty("os.name")).append("\n");
        buf.append("os.arch:").append(System.getProperty("os.arch")).append("\n");
        buf.append("os.version:").append(System.getProperty("os.version")).append("\n");
        buf.append("java.version:").append(System.getProperty("java.version")).append("\n");
        buf.append("java.vendor:").append(System.getProperty("java.vendor"));

        return buf.toString();
    }

    public static String getGitCommitInfo() {
        try {
            File commitFile = new File(KylinConfig.getKylinHome(), COMMIT_SHA1_V15);
            if (!commitFile.exists()) {
                commitFile = new File(KylinConfig.getKylinHome(), COMMIT_SHA1_V13);
            }
            List<String> lines = FileUtils.readLines(commitFile);
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                if (!line.startsWith("#")) {
                    sb.append(line).append(";");
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return StringUtils.EMPTY;
        }
    }
}
