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

package org.apache.kylin.engine.mr;

import java.io.File;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.lookup.LookupBytesTable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangli9
 */
public class LookupTableTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws Exception {
        TableDesc siteTable = MetadataManager.getInstance(getTestConfig()).getTableDesc("EDW.TEST_SITES");
        TableDesc categoryTable = MetadataManager.getInstance(getTestConfig()).getTableDesc("DEFAULT.test_category_groupings");
        LookupBytesTable lookup;

        System.out.println("============================================================================");

        File f = new File(LOCALMETA_TEST_DATA + "/data/EDW.TEST_SITES.csv");
        lookup = new LookupBytesTable(siteTable, new String[] { "SITE_ID" }, new DFSFileTable("file://" + f.getAbsolutePath(), 10));
        lookup.dump();

        System.out.println("============================================================================");

        f = new File(LOCALMETA_TEST_DATA + "/data/DEFAULT.TEST_CATEGORY_GROUPINGS.csv");
        lookup = new LookupBytesTable(categoryTable, new String[] { "leaf_categ_id", "site_id" }, new DFSFileTable("file://" + f.getAbsolutePath(), 36));
        lookup.dump();

        System.out.println("============================================================================");

        ByteArray k1 = new ByteArray(Bytes.toBytes("533"));
        ByteArray k2 = new ByteArray(Bytes.toBytes("0"));
        Array<ByteArray> key = new Array<ByteArray>(new ByteArray[] { k1, k2 });
        System.out.println(lookup.getRow(key));
    }
}
