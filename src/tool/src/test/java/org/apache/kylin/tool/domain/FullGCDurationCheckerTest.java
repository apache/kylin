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

import static org.apache.kylin.tool.daemon.CheckStateEnum.NORMAL;
import static org.apache.kylin.tool.daemon.CheckStateEnum.QUERY_DOWNGRADE;
import static org.apache.kylin.tool.daemon.CheckStateEnum.QUERY_UPGRADE;
import static org.apache.kylin.tool.daemon.CheckStateEnum.RESTART;
import static org.apache.kylin.tool.daemon.CheckStateEnum.WARN;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.apache.kylin.tool.daemon.checker.FullGCDurationChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class FullGCDurationCheckerTest extends NLocalFileMetadataTestCase {

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
        FullGCDurationChecker checker = Mockito.spy(new FullGCDurationChecker());

        CheckResult checkResult = checker.check();
        Assert.assertEquals(WARN, checkResult.getCheckState());

        double[] gcTimes = new double[] { 1.0, 1.0, 1.0, 1.0, 1.0, 20.0, 21.0, 40.0, 41.0, 60.0, 95.0, 80.0, 80.0, 80.0,
                80.0, 99.0 };
        CheckStateEnum[] states = new CheckStateEnum[] { NORMAL, NORMAL, NORMAL, NORMAL, NORMAL, QUERY_UPGRADE, NORMAL,
                NORMAL, QUERY_DOWNGRADE, QUERY_DOWNGRADE, RESTART, QUERY_DOWNGRADE, QUERY_DOWNGRADE, NORMAL, NORMAL,
                QUERY_UPGRADE };

        long now = System.currentTimeMillis();
        for (int i = 0; i < gcTimes.length; i++) {
            Mockito.doReturn(now).when(checker).getNowTime();
            Mockito.doReturn(gcTimes[i]).when(checker).getGCTime();
            checkResult = checker.check();
            Assert.assertEquals(states[i], checkResult.getCheckState());
            now += 20 * 1000;
        }
    }

}
