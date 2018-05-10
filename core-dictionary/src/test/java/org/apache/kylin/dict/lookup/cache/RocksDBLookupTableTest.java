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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 */
public class RocksDBLookupTableTest extends LocalFileMetadataTestCase {

    private TableDesc tableDesc;
    private RocksDBLookupTable lookupTable;
    private Random random;
    private int sourceRowNum;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        this.random = new Random();
        tableDesc = metadataManager.getTableDesc("TEST_COUNTRY", "default");
        sourceRowNum = 10000;
        genTestData();
        lookupTable = new RocksDBLookupTable(tableDesc, new String[] { "COUNTRY" }, "lookup_cache/TEST_COUNTRY");
    }

    private void genTestData() {
        removeTestDataIfExists();
        File file = new File("lookup_cache/TEST_COUNTRY");
        file.mkdirs();
        RocksDBLookupBuilder builder = new RocksDBLookupBuilder(tableDesc, new String[] { "COUNTRY" },
                "lookup_cache/TEST_COUNTRY");
        long start = System.currentTimeMillis();
        builder.build(getLookupTableWithRandomData(sourceRowNum));
        long take = System.currentTimeMillis() - start;
        System.out.println("take:" + take + " ms to complete build");
    }

    private void removeTestDataIfExists() {
        FileUtils.deleteQuietly(new File("lookup_cache"));
    }

    @After
    public void tearDown() throws IOException {
        cleanupTestMetadata();
        removeTestDataIfExists();
        lookupTable.close();
    }

    @Test
    public void testIterator() throws Exception {
        System.out.println("start iterator table");
        long start = System.currentTimeMillis();
        Iterator<String[]> iter = lookupTable.iterator();
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        long take = System.currentTimeMillis() - start;
        System.out.println("scan " + count + " rows, take " + take + " ms");

    }

    @Test
    public void testGet() throws Exception {
        int getNum = 3000;
        List<String[]> keys = Lists.newArrayList();
        for (int i = 0; i < getNum; i++) {
            String[] keyi = new String[] { "keyyyyy" + random.nextInt(sourceRowNum) };
            keys.add(keyi);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < getNum; i++) {
            String[] row = lookupTable.getRow(new Array<>(keys.get(i)));
            if (row == null) {
                System.out.println("null value for key:" + Arrays.toString(keys.get(i)));
            }
        }
        long take = System.currentTimeMillis() - start;
        System.out.println("muliti get " + getNum + " rows, take " + take + " ms");
    }

    private ILookupTable getLookupTableWithRandomData(final int rowNum) {
        return new ILookupTable() {
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
        };
    }

    private String[] genRandomRow(int id) {
        return new String[] { "keyyyyy" + id, String.valueOf(random.nextDouble()), String.valueOf(random.nextDouble()),
                "Andorra" + id };
    }

}
