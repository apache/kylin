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

package org.apache.kylin.metadata.cube.model;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NDataSegDetailsManagerTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager dsMgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataSegDetailsManager mgr = NDataSegDetailsManager.getInstance(testConfig, projectDefault);

        NDataflow df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        NDataSegment segment = df.getLastSegment();
        Assert.assertNotNull(segment);

        NDataSegDetails details = mgr.getForSegment(segment);
        Assert.assertNotNull(details);
        Assert.assertEquals(segment.getId(), details.getUuid());
        Assert.assertEquals(8, details.getLayouts().size());
        Assert.assertSame(segment.getConfig().base(), details.getConfig().base());

        mgr.updateDetails(segment, copyForWrite -> {});
        details = mgr.getForSegment(segment);
        Assert.assertNotNull(details);

        mgr.removeForSegment(details.getUuid());
        Assert.assertNull(mgr.getForSegment(segment));
    }
}
