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

package org.apache.kylin.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.kylin.common.KylinConfig;

public class VersionUtil {
    public static final String KYLIN_VERSION = "kylin.version";

    public static void loadKylinVersion() {
        String version = System.getProperty(KYLIN_VERSION);
        if (version != null) {
            return;
        }
        File vfile = getDefaultVersionFile();
        if (vfile.exists()) {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(vfile), StandardCharsets.UTF_8));) {
                String l = in.readLine();
                if (l != null) {
                    System.setProperty(KYLIN_VERSION, l);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String getKylinVersion() {
        String version = System.getProperty(KYLIN_VERSION);
        return version == null ? "" : version;
    }

    public static File getDefaultVersionFile() {
        File kylinHome = KylinConfig.getKylinHomeAtBestEffort();
        return new File(kylinHome, "VERSION");
    }

}
