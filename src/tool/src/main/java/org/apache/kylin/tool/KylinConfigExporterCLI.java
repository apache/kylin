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
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinConfigExporterCLI {

    public static void main(String[] args) {
        if (ArrayUtils.isEmpty(args)) {
            log.error("Usage:   KylinConfigExporterCli ${FILE_PATH}");
            log.error("Example: KylinConfigExporterCli /tmp/t1.p");
            Unsafe.systemExit(1);
        }
        try {
            execute(args);
        } catch (Exception e) {
            Unsafe.systemExit(1);
        }
        Unsafe.systemExit(0);
    }

    public static void execute(String[] args) {
        String filePath = args[0];
        try {
            if (args.length == 1) {
                String exportString = KylinConfig.getInstanceFromEnv().exportAllToString();
                doExport(filePath, exportString);
                log.info("export configuration succeed, {}", filePath);
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            log.error("export configuration failed, {},", filePath, e);
            ExceptionUtils.rethrow(e);
        }
    }

    private static void doExport(String filePath, String exportStr) throws IOException {
        File kFile = new File(filePath);
        if (kFile.exists()) {
            throw new IllegalStateException("file already exists:" + filePath);
        }
        FileUtils.writeStringToFile(kFile, exportStr, StandardCharsets.UTF_8);
    }
}
