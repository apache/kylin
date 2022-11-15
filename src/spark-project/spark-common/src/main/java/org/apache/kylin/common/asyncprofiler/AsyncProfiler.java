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

package org.apache.kylin.common.asyncprofiler;

import static org.apache.kylin.common.constant.AsyncProfilerConstants.ASYNC_PROFILER_LIB_LINUX_ARM64;
import static org.apache.kylin.common.constant.AsyncProfilerConstants.ASYNC_PROFILER_LIB_LINUX_X64;
import static org.apache.kylin.common.constant.AsyncProfilerConstants.ASYNC_PROFILER_LIB_MAC;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;

import org.apache.kylin.common.KylinConfigBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProfiler {

    private static final Logger logger = LoggerFactory.getLogger(AsyncProfiler.class);

    private static final String LIB_PARENT = "/async-profiler-lib/";
    private static AsyncProfiler profiler;
    private boolean loaded = false;

    public static synchronized AsyncProfiler getInstance(boolean loadLocalLib) {
        if (profiler == null) {
            profiler = new AsyncProfiler(loadLocalLib);
        }
        return profiler;
    }

    private AsyncProfiler(boolean loadLocalLib) {
        try {
            boolean isTestingOnLocalMac = System.getProperty("os.name", "").contains("Mac")
                    || System.getProperty("os.name", "").contains("OS X");
            if (isTestingOnLocalMac) {
                loadLibAsyncProfilerSO(LIB_PARENT + ASYNC_PROFILER_LIB_MAC);
            } else {
                String libName;
                File libPath;

                // Select native lib loading based on machine architecture
                AsyncArchUtil.ArchType archType = AsyncArchUtil.getProcessor();
                logger.info("Machine's archType: {}", archType);
                switch (archType) {
                case LINUX_ARM64:
                    libName = ASYNC_PROFILER_LIB_LINUX_ARM64;
                    break;
                case LINUX_X64:
                default:
                    libName = ASYNC_PROFILER_LIB_LINUX_X64;
                    break;
                }

                // Adapting load paths based on Spark deployment patterns
                if (loadLocalLib) {
                    libPath = new File(KylinConfigBase.getKylinHome() + "/lib/" + libName);
                } else {
                    libPath = new File(libName);
                }
                logger.info("AsyncProfiler libPath: {}, exists: {}", libPath.getAbsolutePath(),
                        Files.exists(libPath.toPath()));
                // check this for ut
                if (libPath.exists()) {
                    System.load(libPath.getAbsolutePath());
                } else {
                    loadLibAsyncProfilerSO(LIB_PARENT + libName);
                }
            }
            loaded = true;
        } catch (Exception e) {
            logger.error("async lib loading failed.", e);
        }
    }

    private void loadLibAsyncProfilerSO(String libPath) throws IOException {
        File asyncProfilerLib = File.createTempFile("libasyncProfiler", ".so");
        java.nio.file.Files.copy(Objects.requireNonNull(AsyncProfilerTool.class.getResourceAsStream(libPath)),
                asyncProfilerLib.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        logger.info("AsyncProfiler will try to load from libPath: {}, exists: {}", asyncProfilerLib.getAbsolutePath(),
                asyncProfilerLib.exists());
        System.load(asyncProfilerLib.getAbsolutePath());
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