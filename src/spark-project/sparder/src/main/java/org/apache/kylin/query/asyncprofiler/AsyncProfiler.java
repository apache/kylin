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

package org.apache.kylin.query.asyncprofiler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProfiler {

    private static final String LOCAL_DEV_LIB_PATH = "../spark-project/sparder/src/main/resources/async-profiler-lib/macOS/libasyncProfiler.so";
    private static final String LIB_PATH = "/async-profiler-lib/linux64/libasyncProfiler.so";

    private static final Logger logger = LoggerFactory.getLogger(AsyncProfiler.class);

    private static AsyncProfiler profiler;
    private boolean loaded = false;

    private AsyncProfiler() {
        try {
            boolean isTestingOnLocalMac = System.getProperty("os.name", "").contains("Mac")
                    || System.getProperty("os.name", "").contains("OS X");
            if (isTestingOnLocalMac) {
                System.load(new java.io.File(LOCAL_DEV_LIB_PATH).getAbsolutePath());
            } else {
                final java.nio.file.Path tmpLib = java.io.File.createTempFile("libasyncProfiler", ".so").toPath();
                java.nio.file.Files.copy(AsyncProfilerTool.class.getResourceAsStream(LIB_PATH), tmpLib,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                System.load(tmpLib.toAbsolutePath().toString());
            }
            loaded = true;
        } catch (Throwable e) {
            logger.error("async lib loading failed.", e);
        }
    }

    public static synchronized AsyncProfiler getInstance() {
        if (profiler == null) {
            profiler = new AsyncProfiler();
        }
        return profiler;
    }

    public boolean isLoaded() {
        return loaded;
    }

    public void stop() throws IllegalStateException {
        if (loaded) {
            stop0();
        } else {
            logger.error("invalid operation stop(). async lib loading failed.");
        }
    }

    public String execute(String command) throws IllegalArgumentException, IllegalStateException, IOException {
        if (loaded) {
            return execute0(command);
        } else {
            logger.error("invalid operation execute(). async lib loading failed.");
            return "";
        }
    }

    public native void start0(String event, long interval, boolean reset) throws IllegalStateException;

    public native void stop0() throws IllegalStateException;

    public native String execute0(String command) throws IllegalArgumentException, IllegalStateException, IOException;

    public native void filterThread0(Thread thread, boolean enable);
}
