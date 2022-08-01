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

package org.apache.kylin.query.util;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PushDownUtilTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testTryPushDownQuery_ForcedToPushDownBasic() throws Exception {
        try {
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryPushDownQuery(queryParams);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertFalse(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testTryPushDownQuery_PushDownDisable() throws Exception {
        try {
            overwriteSystemProp("kylin.query.pushdown-enabled", "false");
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryPushDownQuery(queryParams);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertFalse(e instanceof IllegalArgumentException);
        }
    }
}
