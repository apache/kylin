/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.dict;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.dict.lookup.HiveTable;
import com.kylinolap.dict.lookup.SnapshotManager;
import com.kylinolap.dict.lookup.SnapshotTable;
import com.kylinolap.dict.lookup.TableReader;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * @author yangli9
 * 
 */
public class SnapshotManagerTest extends HBaseMetadataTestCase {

    SnapshotManager snapshotMgr;

    @Before
    public void setup() throws Exception {
        createTestMetadata();

        snapshotMgr = SnapshotManager.getInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void basicTest() throws Exception {
        String tableName = "TEST_SITES";
        HiveTable hiveTable = new HiveTable(MetadataManager.getInstance(this.getTestConfig()), tableName);
        TableDesc tableDesc = MetadataManager.getInstance(this.getTestConfig()).getTableDesc(tableName);
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
