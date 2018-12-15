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

package org.apache.kylin.dict.lookup.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.dict.lookup.IExtLookupProvider;
import org.apache.kylin.dict.lookup.IExtLookupTableCache;
import org.apache.kylin.dict.lookup.IExtLookupTableCache.CacheState;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.LookupProviderFactory;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 */
public class RocksDBLookupTableCacheTest extends LocalFileMetadataTestCase {
    private static final String TABLE_COUNTRY = "DEFAULT.TEST_COUNTRY";
    private static final String MOCK_EXT_LOOKUP = "mock";
    private TableDesc tableDesc;
    private KylinConfig kylinConfig;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        kylinConfig = getTestConfig();
        tableDesc = metadataManager.getTableDesc(TABLE_COUNTRY, "default");
        cleanCache();
        LookupProviderFactory.registerLookupProvider(MOCK_EXT_LOOKUP, MockedLookupProvider.class.getName());
    }

    private void cleanCache() {
        FileUtils.deleteQuietly(new File(kylinConfig.getExtTableSnapshotLocalCachePath()));
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
        cleanCache();
    }

    @Test
    public void testBuildTableCache() throws Exception {
        String snapshotID = RandomUtil.randomUUID().toString();
        ExtTableSnapshotInfo snapshotInfo = buildSnapshotCache(snapshotID, 10000);
        assertEquals(CacheState.AVAILABLE, RocksDBLookupTableCache.getInstance(kylinConfig).getCacheState(snapshotInfo));
    }

    private ExtTableSnapshotInfo buildSnapshotCache(String snapshotID, int rowCnt) throws Exception {
        ExtTableSnapshotInfo snapshotInfo = new ExtTableSnapshotInfo();
        snapshotInfo.setTableName(TABLE_COUNTRY);
        snapshotInfo.setUuid(snapshotID);
        snapshotInfo.setStorageType(MOCK_EXT_LOOKUP);
        snapshotInfo.setKeyColumns(new String[] { "COUNTRY" });
        snapshotInfo.setRowCnt(rowCnt);
        snapshotInfo.setSignature(new TableSignature("/test", rowCnt, System.currentTimeMillis()));

        ExtTableSnapshotInfoManager.getInstance(kylinConfig).save(snapshotInfo);
        RocksDBLookupTableCache cache = RocksDBLookupTableCache.getInstance(kylinConfig);
        cache.buildSnapshotCache(tableDesc, snapshotInfo, getLookupTableWithRandomData(rowCnt));

        while (cache.getCacheState(snapshotInfo) == CacheState.IN_BUILDING) {
            Thread.sleep(500);
        }
        return snapshotInfo;
    }

    @Test
    public void testRestoreCacheFromFiles() throws Exception {
        String snapshotID = RandomUtil.randomUUID().toString();
        String snapshotCacheBasePath = RocksDBLookupTableCache.getCacheBasePath(kylinConfig) + File.separator
                + TABLE_COUNTRY + File.separator + snapshotID;
        String dbPath = snapshotCacheBasePath + File.separator + "db";
        RocksDBLookupBuilder builder = new RocksDBLookupBuilder(tableDesc, new String[] { "COUNTRY" }, dbPath);
        builder.build(getLookupTableWithRandomData(10000));
        String stateFilePath = snapshotCacheBasePath + File.separator + "STATE";
        Files.write(CacheState.AVAILABLE.name(), new File(stateFilePath), Charsets.UTF_8);

        RocksDBLookupTableCache cache = RocksDBLookupTableCache.getInstance(kylinConfig);
        ExtTableSnapshotInfo snapshotInfo = new ExtTableSnapshotInfo();
        snapshotInfo.setTableName(TABLE_COUNTRY);
        snapshotInfo.setUuid(snapshotID);
        snapshotInfo.setStorageType(MOCK_EXT_LOOKUP);
        snapshotInfo.setKeyColumns(new String[] { "COUNTRY" });
        ILookupTable lookupTable = cache.getCachedLookupTable(tableDesc, snapshotInfo, false);
        int rowCnt = 0;
        for (String[] strings : lookupTable) {
            rowCnt++;
        }
        lookupTable.close();
        assertEquals(10000, rowCnt);
    }

    @Test
    public void testEvict() throws Exception {
        kylinConfig.setProperty("kylin.snapshot.ext.local.cache.max-size-gb", "0.005");
        int snapshotNum = 10;
        int snapshotRowCnt = 100000;
        for (int i = 0; i < snapshotNum; i++) {
            buildSnapshotCache(RandomUtil.randomUUID().toString(), snapshotRowCnt);
        }
        assertTrue(RocksDBLookupTableCache.getInstance(kylinConfig).getTotalCacheSize() < 0.006 * 1024 * 1024 * 1024);
    }

    @Test
    public void testCheckCacheState() throws Exception {
        ExtTableSnapshotInfo snapshotInfo = buildSnapshotCache(RandomUtil.randomUUID().toString(), 1000);
        RocksDBLookupTableCache cache = RocksDBLookupTableCache.getInstance(kylinConfig);
        ILookupTable cachedLookupTable = cache.getCachedLookupTable(tableDesc, snapshotInfo, false);
        assertNotNull(cachedLookupTable);
        cachedLookupTable.close();

        ExtTableSnapshotInfoManager.getInstance(kylinConfig).removeSnapshot(snapshotInfo.getTableName(), snapshotInfo.getId());
        cache.checkCacheState();
        String cacheLocalPath = cache.getSnapshotCachePath(snapshotInfo.getTableName(), snapshotInfo.getId());
        // won't cleanup because it is newly created in last 1 hour
        assertTrue(new File(cacheLocalPath).exists());

        // change the volatile value
        kylinConfig.setProperty("kylin.snapshot.ext.local.cache.check.volatile", "0");
        cache.checkCacheState();
        // this time it should be removed.
        assertFalse(new File(cacheLocalPath).exists());

        cachedLookupTable = cache.getCachedLookupTable(tableDesc, snapshotInfo, false);
        assertNull(cachedLookupTable);
    }

    public static class MockedLookupProvider implements IExtLookupProvider {

        @Override
        public ILookupTable getLookupTable(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshot) {
            return getLookupTableWithRandomData(extTableSnapshot.getRowCnt());
        }

        @Override
        public IExtLookupTableCache getLocalCache() {
            return null;
        }

        @Override
        public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            return null;
        }
    }

    private static ILookupTable getLookupTableWithRandomData(final long rowNum) {
        return new ILookupTable() {
            Random random = new Random();

            @Override
            public String[] getRow(Array<String> key) {
                return new String[0];
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public Iterator<String[]> iterator() {
                return new Iterator<String[]>() {
                    private int iterCnt = 0;

                    @Override
                    public boolean hasNext() {
                        return iterCnt < rowNum;
                    }

                    @Override
                    public String[] next() {
                        iterCnt++;
                        return genRandomRow(iterCnt);
                    }

                    @Override
                    public void remove() {

                    }
                };
            }

            private String[] genRandomRow(int id) {
                return new String[] { "keyyyyy" + id, String.valueOf(random.nextDouble()),
                        String.valueOf(random.nextDouble()), "Andorra" + id };
            }
        };
    }

}
