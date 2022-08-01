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

package org.apache.kylin.tool.garbage;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SnapshotCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";
    private String dataflowId;
    private NTableMetadataManager tableMetadataManager;
    private NDataflowManager dataflowManager;
    private String tableName;

    @Before
    public void init() {
        createTestMetadata();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

        tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, DEFAULT_PROJECT);
        dataflowManager = NDataflowManager.getInstance(kylinConfig, DEFAULT_PROJECT);

        // assert that snapshot exists
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        Set<TableDesc> tables = dataflow.getModel().getLookupTables().stream().map(tableRef -> tableRef.getTableDesc())
                .collect(Collectors.toSet());

        String stalePath = "default/table_snapshot/mock";
        tables.forEach(tableDesc -> {
            tableDesc.setLastSnapshotPath(stalePath);
            tableMetadataManager.updateTableDesc(tableDesc);
        });
        tableName = tables.iterator().next().getIdentity();

        Assert.assertTrue(tables.size() > 0);
        Assert.assertFalse(StringUtils.isEmpty(tableMetadataManager.getTableDesc(tableName).getLastSnapshotPath()));

    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testSnapshotCleanerCleanStaleSnapshots() {
        SnapshotCleaner snapshotCleaner = new SnapshotCleaner(DEFAULT_PROJECT);
        snapshotCleaner.prepare();
        UnitOfWork.doInTransactionWithRetry(() -> {
            snapshotCleaner.cleanup();
            return 0;
        }, DEFAULT_PROJECT);

        // assert that snapshots are cleared
        Assert.assertTrue(StringUtils.isEmpty(tableMetadataManager.getTableDesc(tableName).getLastSnapshotPath()));
        Assert.assertEquals(-1, tableMetadataManager.getOrCreateTableExt(tableName).getOriginalSize());
    }

}
