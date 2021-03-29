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
import org.apache.kylin.common.notify.util.NotificationConstants;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void testSendDingTalk() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<String> receivers = new ArrayList<String>(1);
        receivers.add("98a3994aecc22763b135b91e953fd32eec60e1bf10190f7af4eca77049c433e@13675555555");
        Map<String, List<String>> receiversMap = new HashMap();
        receiversMap.put(NotificationConstants.NOTIFY_DINGTALK_LIST, receivers);

        Map<String, Object> content = new HashMap();
        content.put("info", "dingtalk notification");

        Pair<String[], Map<String, Object>> mapPair = Pair.newPair(new String[]{"test"}, content);
        NotificationContext notificationInfo = new NotificationContext(config, receiversMap, NotificationConstants.JOB_ERROR, mapPair);

        DingTalkService dingTalkservice = new DingTalkService(notificationInfo);
        boolean sent = sendTestDingTalk(dingTalkservice);
        assert !sent;

        System.setProperty("kylin.job.notification-enabled", "false");
        // set kylin.job.notification-enabled=false, and run again, this time should be no mail delivered
        dingTalkservice = new DingTalkService(notificationInfo);
        sent = sendTestDingTalk(dingTalkservice);
        assert !sent;
    }

    private boolean sendTestDingTalk(DingTalkService dingTalkService) {
        return dingTalkService.sendNotification();
    }
}
