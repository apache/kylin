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

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.plugin.asyncprofiler.ProfilerStatus;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BuildAsyncProfileHelperTest extends NLocalFileMetadataTestCase {

    String project = "default";
    String jobStepId = "0cb5ea2e-adfe-be86-a04a-e2d385fd27ad-c11baf56-a593-4c5f-d546-1fa86c2d54ad_01";
    String startParams = "start,event=cpu";
    String dumpParams = "flamegraph";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }


    @Test
    public void testGetProfileStatus() {
        Assert.assertEquals(BuildAsyncProfileHelper.NOT_EXIST, BuildAsyncProfileHelper.getProfileStatus(project, jobStepId));
    }

    @Test
    public void testStartProfileError() throws IOException {
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);

        Assert.assertThrows("profiler is started already", KylinException.class,
                () -> BuildAsyncProfileHelper.startProfile(project, jobStepId, startParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testStartProfile() throws IOException {
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs("", actionPath);
        String errorMsg = "";
        try {
            BuildAsyncProfileHelper.startProfile(project, jobStepId, startParams);
        } catch (Exception e) {
            errorMsg = e.getMessage();
        }
        Assert.assertEquals(0, errorMsg.length());
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDumpError() throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.engine.async-profiler-result-timeout", "1s");
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.IDLE(), actionPath);
        Assert.assertThrows("profiler is not start yet", KylinException.class,
                () -> BuildAsyncProfileHelper.dump(project, jobStepId, dumpParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDumpTimeoutNegative() throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.engine.async-profiler-result-timeout", "-1s");
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);
        Assert.assertThrows("collect dump timeout,please retry later", KylinException.class,
                () -> BuildAsyncProfileHelper.dump(project, jobStepId, dumpParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDumpTwice() throws IOException {
        overwriteSystemProp("kylin.engine.async-profiler-result-timeout", "3s");
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.DUMPED(), actionPath);
        Assert.assertThrows("collect dump timeout,please retry later", KylinException.class,
                () -> BuildAsyncProfileHelper.dump(project, jobStepId, dumpParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDumpTimeout() throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.engine.async-profiler-result-timeout", "1s");
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);
        Assert.assertThrows("collect dump timeout,please retry later", KylinException.class,
                () -> BuildAsyncProfileHelper.dump(project, jobStepId, dumpParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
    }

    @Test
    public void testDump() throws IOException {
        overwriteSystemProp("kylin.engine.async-profiler-result-timeout", "3s");
        Path actionPath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/status");
        Path dumpFilePath = new Path(KylinConfig.getInstanceFromEnv().getJobTmpProfilerFlagsDir(project, jobStepId) + "/dump.tar.gz");
        HadoopUtil.writeStringToHdfs(ProfilerStatus.RUNNING(), actionPath);

        Thread t1 = new Thread(() -> {
            try {
                await().pollDelay(new Duration(1, TimeUnit.MILLISECONDS)).until(() -> true);
                HadoopUtil.writeStringToHdfs("", dumpFilePath);
            } catch (IOException ignored) {
            }
        });
        t1.start();

        Assert.assertNotNull(BuildAsyncProfileHelper.dump(project, jobStepId, dumpParams));
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), actionPath);
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), dumpFilePath);
        t1.interrupt();
    }

    @Test
    public void testCheckInvalidStatusNotStart() {
        Assert.assertThrows("job does not start yet", KylinException.class,
                () -> BuildAsyncProfileHelper.checkInvalidStatus(BuildAsyncProfileHelper.NOT_EXIST));
    }

    @Test
    public void testCheckInvalidStatusFinished() {
        String status = ProfilerStatus.CLOSED();
        Assert.assertThrows("job is finished already", KylinException.class,
                () -> BuildAsyncProfileHelper.checkInvalidStatus(status));
    }

    @Test
    public void testCheckInvalidStatus() {
        BuildAsyncProfileHelper.checkInvalidStatus("");
    }
}
