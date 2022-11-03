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
package org.apache.kylin.rest.util;

import java.io.InputStream;
import java.util.Locale;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.asyncprofiler.Message;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.plugin.asyncprofiler.ProfilerStatus;

import lombok.SneakyThrows;

public class BuildAsyncProfileHelper {

    private BuildAsyncProfileHelper(){}

    public static final String NOT_EXIST = "not_exist";

    @SneakyThrows
    public static String getProfileStatus(String project, String jobStepId) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Path actionPath = new Path(kylinConfig.getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        if (!HadoopUtil.getWorkingFileSystem().exists(actionPath)) {
            return NOT_EXIST;
        }
        return HadoopUtil.readStringFromHdfs(actionPath);
    }

    @SneakyThrows
    public static void startProfile(String project, String jobStepId, String param) {
        String status = getProfileStatus(project, jobStepId);
        checkInvalidStatus(status);

        if (ProfilerStatus.RUNNING().equals(status)) {
            throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingStartedError()));
        }

        String cmd = Message.createDriverMessage(Message.START(), param);
        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/action");
        HadoopUtil.writeStringToHdfs(cmd, actionPath);
    }

    @SneakyThrows
    public static InputStream dump(String project, String jobStepId, String param) {
        String status = getProfileStatus(project, jobStepId);
        checkInvalidStatus(status);
        if (ProfilerStatus.IDLE().equals(status)) {
            throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotStartError()));
        }
        String cmd = Message.createDriverMessage(Message.DUMP(), param);
        Path actionPath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/action");
        HadoopUtil.writeStringToHdfs(cmd, actionPath);
        Path dumpFilePath = new Path(
                KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/dump.tar.gz");

        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        long timeout = KylinConfig.getInstanceFromEnv().buildJobProfilingResultTimeout();
        while (!ProfilerStatus.DUMPED().equals(getProfileStatus(project, jobStepId)) && timeout >= 0) {
            Thread.sleep(500);
            timeout -= 500;
        }
        if (!fileSystem.exists(dumpFilePath)) {
            throw new KylinException(JobErrorCode.PROFILING_COLLECT_TIMEOUT, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingCollectTimeout()));
        }
        return fileSystem.open(dumpFilePath);
    }

    public static void checkInvalidStatus(String status) {
        if (NOT_EXIST.equals(status)) {
            throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingJobNotStartError()));
        }
        if (ProfilerStatus.CLOSED().equals(status)) {
            throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingJobFinishedError()));
        }
    }

}