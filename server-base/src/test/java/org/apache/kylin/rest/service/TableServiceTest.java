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
import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.rest.response.TableSnapshotResponse;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TableServiceTest extends LocalFileMetadataTestCase {
    private TableService tableService;

    @Before
    public void setUp() {
        this.createTestMetadata();
        tableService = new TableService();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetTableSnapshots() throws IOException {
        TableSignature tableSignature = new TableSignature("TEST_CAL_DT.csv", 100, System.currentTimeMillis());
        List<TableSnapshotResponse> snapshotResponseList = tableService.internalGetLookupTableSnapshots("EDW.TEST_CAL_DT", tableSignature);
        Assert.assertEquals(8, snapshotResponseList.size());
    }
}
