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

package org.apache.kylin.source.hive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.ReadableTable.TableReader;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangli9
 * 
 */
public class ITSnapshotManagerTest extends HBaseMetadataTestCase {

    SnapshotManager snapshotMgr;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        snapshotMgr = SnapshotManager.getInstance(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void basicTest() throws Exception {
        String tableName = "EDW.TEST_SITES";
        TableDesc tableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc(tableName);
        ReadableTable hiveTable = SourceFactory.createReadableTable(tableDesc);
        String snapshotPath = snapshotMgr.buildSnapshot(hiveTable, tableDesc).getResourcePath();

        snapshotMgr.wipeoutCache();

        SnapshotTable snapshot = snapshotMgr.getSnapshotTable(snapshotPath);

        // compare hive & snapshot
        TableReader hiveReader = hiveTable.getReader();
        TableReader snapshotReader = snapshot.getReader();

        while (true) {
            boolean hiveNext = hiveReader.next();
            boolean snapshotNext = snapshotReader.next();
            assertEquals(hiveNext, snapshotNext);

            if (hiveNext == false)
                break;

            String[] hiveRow = hiveReader.getRow();
            String[] snapshotRow = snapshotReader.getRow();
            assertArrayEquals(hiveRow, snapshotRow);
        }
    }
}
