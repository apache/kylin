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
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateEnum;
import org.apache.kylin.tool.daemon.checker.KEProcessChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KEProcessCheckerTest extends NLocalFileMetadataTestCase {

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
        KEProcessChecker checker = Mockito.spy(new KEProcessChecker());

        CheckResult result = checker.check();
        Assert.assertEquals(CheckStateEnum.WARN, result.getCheckState());

        Mockito.doReturn("echo 0").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.NORMAL, result.getCheckState());

        Mockito.doReturn("echo 1").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.SUICIDE, result.getCheckState());

        Mockito.doReturn("echo -1").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.RESTART, result.getCheckState());

        Mockito.doReturn("echo 2").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.WARN, result.getCheckState());

        Mockito.doReturn("echo -2").when(checker).getProcessStatusCmd();

        result = checker.check();
        Assert.assertEquals(CheckStateEnum.WARN, result.getCheckState());
    }

}
