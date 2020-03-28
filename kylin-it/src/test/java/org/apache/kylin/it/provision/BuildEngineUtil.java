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

package org.apache.kylin.it.provision;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.rest.job.StorageCleanupJob;

import java.util.Map;
import java.util.Properties;

public class BuildEngineUtil {

    protected static String LINE = "============================================================\n";

    public static boolean isFastBuildMode() {
        String fastModeStr = System.getProperty("fastBuildMode");
        if (fastModeStr == null)
            fastModeStr = System.getenv("KYLIN_CI_FASTBUILD");
        boolean flag = "true".equalsIgnoreCase(fastModeStr);
        if (flag) {
            System.out.println(LINE + "Set fast build mode to TRUE.");
        }
        return flag;
    }

    public static boolean isSimpleBuildMode() {
        String simpleModeStr = System.getProperty("simpleBuildMode");
        if (simpleModeStr == null)
            simpleModeStr = System.getenv("KYLIN_CI_SIMPLEBUILD");
        boolean flag = "true".equalsIgnoreCase(simpleModeStr);
        if (flag) {
            System.out.println(LINE + "Set simple build mode to TRUE.");
        }
        return flag;
    }

    public static void isBuildSkip() {
        if ("true".equalsIgnoreCase(System.getProperty("skipBuild"))) {
            System.out.println(LINE + "Skip building job.");
            System.exit(0);
        }
    }

    public static void isNRTSkip() {
        if ("true".equalsIgnoreCase(System.getProperty("skipNRT"))) {
            System.out.println(LINE + "Skip building Near Real-time job.");
            System.exit(0);
        }
    }

    public static void isRealtimeSkip() {
        if ("true".equalsIgnoreCase(System.getProperty("skipRealtime"))) {
            System.out.println(LINE + "Skip building Real-time OLAP job.");
            System.exit(0);
        }
    }

    public static int cleanupOldStorage() {
        String[] args = {"--delete", "true"};
        try {
            StorageCleanupJob cli = new StorageCleanupJob();
            cli.execute(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void printEnv() {
        String[] cps = System.getProperty("java.class.path").split(":");
        Map<String, String> systemVariables = System.getenv();
        Properties systemProperties = System.getProperties();
        System.out.println(LINE + ">>> Print Classpath <<<");
        for (String cp : cps) {
            System.out.println("    " + cp);
        }

        System.out.println(LINE +
                ">>> Print System Variables <<<");
        for (Map.Entry<String, String> entry : systemVariables.entrySet()) {
            System.out.println("    " + entry.getKey() + " : " + entry.getValue());
        }

        System.out.println(LINE +
                ">>> Print System Property <<<");
        for (Map.Entry<Object, Object> entry : systemProperties.entrySet()) {
            System.out.println("    " + entry.getKey() + " : " + entry.getValue());
        }
    }

    protected static ExecutableState waitForJob(ExecutableManager jobService, String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                return job.getStatus();
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
