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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DiagnosisServiceTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testDiagnosisService() throws IOException {

        BadQueryHistory history = BadQueryHistoryManager.getInstance(getTestConfig())
                .getBadQueriesForProject("default");
        List<BadQueryEntry> allEntries = new ArrayList<>();
        allEntries.addAll(history.getEntries());
        DiagnosisService diagnosisService = new DiagnosisService();
        Object obj = diagnosisService.getQueries(0, 10, BadQueryEntry.ADJ_PUSHDOWN, allEntries).get("badQueries");
        List<BadQueryEntry> pushDownList = (List<BadQueryEntry>) obj;
        Assert.assertEquals(1, pushDownList.size());

        obj = diagnosisService.getQueries(0, 10, BadQueryEntry.ADJ_SLOW, allEntries).get("badQueries");
        List<BadQueryEntry> slowList = (List<BadQueryEntry>) obj;
        Assert.assertEquals(2, slowList.size());

    }
}
