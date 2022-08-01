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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("convenient trial tool for dev")
public class MailServiceTest extends AbstractTestCase {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testSendEmail() throws IOException {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        MailService mailservice = new MailService(config);
        boolean sent = sendTestEmail(mailservice);
        assert sent;

        overwriteSystemProp("kylin.job.notification-enabled", "false");
        // set kylin.job.notification-enabled=false, and run again, this time should be no mail delivered
        mailservice = new MailService(config);
        sent = sendTestEmail(mailservice);
        assert !sent;
    }

    @Test
    public void testMailHelper() {
        overwriteSystemProp("kylin.capacity.notification-enabled", "true");
        overwriteSystemProp("kylin.capacity.notification-emails", "foobar@foobar.com");
        boolean sent = MailHelper.notifyUserForOverCapacity(100L, 81L);
        assert sent;
    }

    private boolean sendTestEmail(MailService mailservice) {

        List<String> receivers = new ArrayList<String>(1);
        receivers.add("foobar@foobar.com");
        return mailservice.sendMail(receivers, "A test email from Kylin", "Hello!");
    }
}
