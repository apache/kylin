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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.MockupReadableTable;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SnapshotManagerTest extends LocalFileMetadataTestCase {

    private KylinConfig kylinConfig;
    private SnapshotManager snapshotManager;
    List<String[]> expect;
    List<String[]> dif;
    // test data for scd1
    List<String[]> contentAtTime1;
    List<String[]> contentAtTime2;
    List<String[]> contentAtTime3;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        String[] s1 = new String[] { "1", "CN" };
        String[] s2 = new String[] { "2", "NA" };
        String[] s3 = new String[] { "3", "NA" };
        String[] s4 = new String[] { "4", "KR" };
        String[] s5 = new String[] { "5", "JP" };
        String[] s6 = new String[] { "6", "CA" };
        expect = Lists.newArrayList(s1, s2, s3, s4, s5);
        dif = Lists.newArrayList(s1, s2, s3, s4, s6);

        contentAtTime1 = Lists.newArrayList(s1, s2, s3, s4, s5, s6);
        String[] s22 = new String[] { "2", "SP" };
        contentAtTime2 = Lists.newArrayList(s1, s22, s3, s4, s5);
        String[] s23 = new String[] { "2", "US" };
        contentAtTime3 = Lists.newArrayList(s1, s23, s3, s4, s5);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private TableDesc genTableDesc(String tableName) {
        TableDesc table = TableDesc.mockup(tableName);
        ColumnDesc desc1 = new ColumnDesc("1", "id", "string", null, null, null, null);
        desc1.setId("1");
        desc1.setDatatype("long");
        ColumnDesc desc2 = new ColumnDesc("2", "country", "string", null, null, null, null);
        desc2.setId("2");
        desc2.setDatatype("string");
        ColumnDesc[] columns = { desc1, desc2 };
        table.setColumns(columns);
        table.init(kylinConfig, "default");
        return table;
    }

    private IReadableTable genTable(String path, List<String[]> content) {
        IReadableTable.TableSignature signature = new IReadableTable.TableSignature(path, content.size(), 0);
        return new MockupReadableTable(content, signature, true);
    }

    @Test
    public void testCheckByContent() throws IOException, InterruptedException {
        runTestCase();
    }

    public void runTestCase() throws IOException, InterruptedException {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        snapshotManager = SnapshotManager.getInstance(kylinConfig);
        SnapshotTable origin = snapshotManager.buildSnapshot(genTable("./origin", expect), genTableDesc("TEST_TABLE"),
                kylinConfig);

        // sleep 1 second to avoid file resource store precision lost
        Thread.sleep(1000);
        SnapshotTable dup = snapshotManager.buildSnapshot(genTable("./dup", expect), genTableDesc("TEST_TABLE"),
                kylinConfig);
        // assert same snapshot file
        Assert.assertEquals(origin.getUuid(), dup.getUuid());
        Assert.assertEquals(origin.getResourcePath(), dup.getResourcePath());

        // assert the file has been updated
        long originLastModified = origin.getLastModified();
        long dupLastModified = dup.getLastModified();
        Assert.assertTrue(dupLastModified > originLastModified);

        SnapshotTable actual = snapshotManager.getSnapshotTable(origin.getResourcePath());
        IReadableTable.TableReader reader = actual.getReader();
        Assert.assertEquals(expect.size(), actual.getRowCount());
        int i = 0;
        while (reader.next()) {
            Assert.assertEquals(stringJoin(expect.get(i++)), stringJoin(reader.getRow()));
        }

        SnapshotTable difTable = snapshotManager.buildSnapshot(genTable("./dif", dif), genTableDesc("TEST_TABLE"),
                kylinConfig);
        Assert.assertNotEquals(origin.getUuid(), difTable.getUuid());
    }

    @Test
    public void testBuildSameSnapshotSameTime() throws InterruptedException, IOException {
        final int threadCount = 3;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        final TableDesc tableDesc = genTableDesc("TEST_TABLE");

        kylinConfig = KylinConfig.getInstanceFromEnv();
        snapshotManager = SnapshotManager.getInstance(kylinConfig);
        ResourceStore store = ResourceStore.getStore(kylinConfig);

        for (int i = 0; i < threadCount; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        snapshotManager.buildSnapshot(genTable("./origin", expect), tableDesc, kylinConfig);
                    } catch (IOException e) {
                        Assert.fail();
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            });
        }
        countDownLatch.await();
        Assert.assertEquals(1, store.listResources("/table_snapshot/NULL.TEST_TABLE").size());
    }

    private String stringJoin(String[] strings) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            builder.append(strings[i]);
            if (i < strings.length - 1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }
}
