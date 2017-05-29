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

package org.apache.kylin.storage.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ITHBaseResourceStoreTest extends HBaseMetadataTestCase {

    private KylinConfig kylinConfig;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHBaseStore() throws Exception {
        String storeName = "org.apache.kylin.storage.hbase.HBaseResourceStore";
        ResourceStoreTest.testAStore(ResourceStoreTest.mockUrl("hbase", kylinConfig), kylinConfig);
    }

    @Test
    public void testHBaseStoreWithLargeCell() throws Exception {
        String path = "/cube/_test_large_cell.json";
        String largeContent = "THIS_IS_A_LARGE_CELL";
        StringEntity content = new StringEntity(largeContent);
        String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig, ResourceStoreTest.mockUrl("hbase", kylinConfig));
        HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());
        Configuration hconf = store.getConnection().getConfiguration();
        int origSize = Integer.parseInt(hconf.get("hbase.client.keyvalue.maxsize", "10485760"));

        try {
            hconf.set("hbase.client.keyvalue.maxsize", String.valueOf(largeContent.length() - 1));

            store.deleteResource(path);

            store.putResource(path, content, StringEntity.serializer);
            assertTrue(store.exists(path));
            StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
            assertEquals(content, t);

            Path redirectPath = ((HBaseResourceStore) store).bigCellHDFSPath(path);

            FileSystem fileSystem = FileSystem.get(hconf);
            assertTrue(fileSystem.exists(redirectPath));

            FSDataInputStream in = fileSystem.open(redirectPath);
            assertEquals(largeContent, in.readUTF());
            in.close();

            store.deleteResource(path);
            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            hconf.set("hbase.client.keyvalue.maxsize", "" + origSize);
            store.deleteResource(path);
        }
    }

}
