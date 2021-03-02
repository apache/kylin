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

package org.apache.kylin.common.notify;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Ignore("convenient trial tool for dev")
public class DingTalkServiceTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSendEmail() throws IOException {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        DingTalkService dingTalkservice = new DingTalkService(config);
        boolean sent = sendTestEmail(dingTalkservice);
        assert !sent;

        System.setProperty("kylin.job.notification-enabled", "false");
        // set kylin.job.notification-enabled=false, and run again, this time should be no mail delivered
        dingTalkservice = new DingTalkService(config);
        sent = sendTestEmail(dingTalkservice);
        assert !sent;

    }

    private boolean sendTestEmail(DingTalkService dingTalkService) {

        List<String> receivers = new ArrayList<String>(1);
        receivers.add("xxxx");
        return dingTalkService.sendContent(receivers, "A test dingtalk from Kylin", "Hello!");
    }
}
