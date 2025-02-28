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
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Objects;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Strings;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessUtils {

    private ProcessUtils() {
    }

    public static int getPid(Process process) throws IllegalAccessException, NoSuchFieldException {
        String className = process.getClass().getName();
        Preconditions.checkState(className.equals("java.lang.UNIXProcess"));
        Field f = process.getClass().getDeclaredField("pid");
        Unsafe.changeAccessibleObject(f, true);
        return f.getInt(process);
    }

    public static boolean isAlive(int pid) {
        try {
            String line;
            Process p = Runtime.getRuntime().exec("ps -e");
            try (BufferedReader input = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), Charset.defaultCharset()))) {
                while (!Strings.isNullOrEmpty(line = input.readLine())) {
                    if (Objects.equals(line.trim().split("\\s+")[0], pid + "")) {
                        return true;
                    }
                }
            }
        } catch (Exception err) {
            log.info("Exception happened when check <{}>", pid, err);
        }
        return false;
    }

    public static String getCurrentId(final String fallback) {
        // Note: may fail in some JVM implementations
        // therefore fallback has to be provided

        // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
        final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        final int index = jvmName.indexOf('@');

        try {
            return Long.toString(Long.parseLong(jvmName.substring(0, index)));
        } catch (Exception e) {
            // ignore
        }
        return fallback;
    }
}
