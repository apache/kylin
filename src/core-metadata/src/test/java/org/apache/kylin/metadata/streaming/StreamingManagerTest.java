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

package org.apache.kylin.metadata.streaming;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamingManagerTest extends AbstractTestCase {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("kylin.env", "UT");
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testBasics() throws IOException {
        StreamingManager mgr = StreamingManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<StreamingConfig> origin = mgr.listAllStreaming();

        // test create
        {
            StreamingConfig streamingConfig = new StreamingConfig();
            streamingConfig.setName("name_for_test");
            streamingConfig.setType("type for test");
            mgr.createStreamingConfig(streamingConfig);
            List<StreamingConfig> reloadAll = mgr.reloadAll();
            Assert.assertEquals(origin.size() + 1, reloadAll.size());
        }

        // test update
        {
            StreamingConfig streamingConfig = mgr.getStreamingConfig("name_for_test");
            streamingConfig.setType("updated type");
            mgr.updateStreamingConfig(streamingConfig);
            List<StreamingConfig> reloadAll = mgr.reloadAll();
            Assert.assertEquals(origin.size() + 1, reloadAll.size());
            streamingConfig = mgr.getStreamingConfig("name_for_test");
            Assert.assertEquals("updated type", streamingConfig.getType());
        }
    }
}
