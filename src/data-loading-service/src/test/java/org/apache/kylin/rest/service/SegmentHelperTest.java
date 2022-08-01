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
package org.apache.kylin.rest.service;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import lombok.val;

public class SegmentHelperTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    private SegmentHelper segmentHelper = new SegmentHelper();

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRemoveSegment() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        int segSize = df.getSegments().size();
        val segId = df.getSegments().iterator().next().getId();

        segmentHelper.removeSegment(DEFAULT_PROJECT, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Sets.newHashSet(segId));

        df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        int segSize2 = df.getSegments().size();
        Assert.assertEquals(segSize, segSize2 + 1);

        // handle again, will not reduce dataFlow's segments
        segmentHelper.removeSegment(DEFAULT_PROJECT, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Sets.newHashSet(segId));

        df = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        int segSize3 = df.getSegments().size();
        Assert.assertEquals(segSize2, segSize3);
    }
}
