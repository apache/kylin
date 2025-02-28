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
package org.apache.kylin.tool.domain;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.apache.kylin.tool.daemon.checker.KEStatusChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KEStatusCheckerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        KEStatusChecker checker = Mockito.spy(new KEStatusChecker());

        CheckResult checkResult;
        for (int i = 0; i < 4; i++) {
            checkResult = checker.check();
            Assert.assertEquals(CheckStateEnum.WARN, checkResult.getCheckState());
        }

        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, checkResult.getCheckState());

        KEStatusChecker.SparkStatus sparkStatus = new KEStatusChecker.SparkStatus(0, 0);
        KEStatusChecker.CanceledSlowQueryStatus slowQueryStatus = new KEStatusChecker.CanceledSlowQueryStatus("1", 1,
                System.currentTimeMillis(), 400);
        KEStatusChecker.Status status = new KEStatusChecker.Status(sparkStatus, Lists.newArrayList(slowQueryStatus));
        KEStatusChecker.EnvelopeResponse response = new KEStatusChecker.EnvelopeResponse<>("000", status, "");

        Mockito.doReturn(response).when(checker).getHealthStatus();
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, checkResult.getCheckState());

        sparkStatus.setFailureTimes(3);
        sparkStatus.setLastFailureTime(System.currentTimeMillis());
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, checkResult.getCheckState());
        sparkStatus.setFailureTimes(0);

        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, checkResult.getCheckState());

        slowQueryStatus.setCanceledTimes(3);
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, checkResult.getCheckState());

        slowQueryStatus.setCanceledTimes(1);
        checkResult = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, checkResult.getCheckState());
    }

}
