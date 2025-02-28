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

import static org.apache.kylin.common.KylinExternalConfigLoader.KYLIN_CONF_PROPERTIES_FILE;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinConfigChecksumCLI {
    public static void main(String[] args) {
        val ret = execute(args);
        Unsafe.systemExit(ret);
    }

    public static int execute(String[] args) {
        if (args.length < 1) {
            log.error("Please input expected checksum!");
            return -1;
        }

        try {
            val expectedChecksum = args[0];
            val kylinConfDir = KylinConfig.getKylinConfDir();
            val propFile = new File(kylinConfDir, KYLIN_CONF_PROPERTIES_FILE);
            // may be not exists
            val propOverrideFile = new File(kylinConfDir, KYLIN_CONF_PROPERTIES_FILE + ".override");
            String propContent = FileUtils.readFileToString(propFile, StandardCharsets.UTF_8);
            if (propOverrideFile.exists()) {
                propContent += FileUtils.readFileToString(propOverrideFile, StandardCharsets.UTF_8);
            }

            val kylinConfigChecksum = DigestUtils.sha256Hex(propContent);
            if (!kylinConfigChecksum.equalsIgnoreCase(expectedChecksum)) {
                log.error(
                        "Kylin config checksum [{}] is not equal to expected checksum [{}], expected checksum is from file detached_by_km!",
                        kylinConfigChecksum, expectedChecksum);
                return 1;
            }
            return 0;
        } catch (Exception e) {
            log.error("An exception occurred when checking kylin config checksum, ex ", e);
            return -1;
        }
    }
}
