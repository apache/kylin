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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.lookup.cache.RocksDBLookupRowEncoder.KV;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RocksDBLookupRowEncoderTest extends LocalFileMetadataTestCase {
    private TableDesc tableDesc;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        tableDesc = metadataManager.getTableDesc("TEST_COUNTRY", "default");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testEnDeCode() {
        RocksDBLookupRowEncoder lookupRowEncoder = new RocksDBLookupRowEncoder(tableDesc, new String[] { "COUNTRY" });
        String[] row = new String[] { "AD", "42.546245", "1.601554", "Andorra" };
        KV kv = lookupRowEncoder.encode(row);

        String[] decodeRow = lookupRowEncoder.decode(kv);
        assertArrayEquals(row, decodeRow);
    }

    @Test
    public void testEnDeCodeWithNullValue() {
        RocksDBLookupRowEncoder lookupRowEncoder = new RocksDBLookupRowEncoder(tableDesc, new String[] { "COUNTRY" });
        String[] row = new String[] { "AD", "42.546245", "1.601554", null };
        KV kv = lookupRowEncoder.encode(row);

        String[] decodeRow = lookupRowEncoder.decode(kv);
        assertNull(decodeRow[3]);
        assertArrayEquals(row, decodeRow);
    }

    @Test
    public void testEnDeCodeWithMultiKeys() {
        RocksDBLookupRowEncoder lookupRowEncoder = new RocksDBLookupRowEncoder(tableDesc, new String[] { "COUNTRY",
                "NAME" });
        String[] row = new String[] { "AD", "42.546245", "1.601554", "Andorra" };
        KV kv = lookupRowEncoder.encode(row);

        String[] decodeRow = lookupRowEncoder.decode(kv);
        assertArrayEquals(row, decodeRow);
    }

}
