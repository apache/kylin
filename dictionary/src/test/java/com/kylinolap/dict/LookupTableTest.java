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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.Array;
import com.kylinolap.common.util.ByteArray;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.dict.lookup.FileTable;
import com.kylinolap.dict.lookup.LookupBytesTable;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.schema.TableDesc;

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
        TableDesc siteTable = MetadataManager.getInstance(this.getTestConfig()).getTableDesc("TEST_SITES");
        TableDesc categoryTable = MetadataManager.getInstance(this.getTestConfig()).getTableDesc("test_category_groupings");
        LookupBytesTable lookup;

        System.out.println("============================================================================");

        lookup = new LookupBytesTable(siteTable, new String[] { "SITE_ID" }, new FileTable(LOCALMETA_TEST_DATA + "/data/TEST_SITES.csv", 10));
        lookup.dump();

        System.out.println("============================================================================");

        lookup = new LookupBytesTable(categoryTable, new String[] { "leaf_categ_id", "site_id" }, new FileTable(LOCALMETA_TEST_DATA + "/data/TEST_CATEGORY_GROUPINGS.csv", 36));
        lookup.dump();

        System.out.println("============================================================================");

        ByteArray k1 = new ByteArray(Bytes.toBytes("533"));
        ByteArray k2 = new ByteArray(Bytes.toBytes("0"));
        Array<ByteArray> key = new Array<ByteArray>(new ByteArray[] { k1, k2 });
        System.out.println(lookup.getRow(key));
    }
}
