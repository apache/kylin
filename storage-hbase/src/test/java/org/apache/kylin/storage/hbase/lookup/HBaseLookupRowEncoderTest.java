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

package org.apache.kylin.storage.hbase.lookup;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.NavigableMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.storage.hbase.lookup.HBaseLookupRowEncoder.HBaseRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseLookupRowEncoderTest extends LocalFileMetadataTestCase {
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
        HBaseLookupRowEncoder lookupRowEncoder = new HBaseLookupRowEncoder(tableDesc, new String[] { "COUNTRY" }, 1);
        String[] row = new String[] { "AD", "42.546245", "1.601554", "Andorra" };
        HBaseRow hBaseRow = lookupRowEncoder.encode(row);

        assertEquals(6, hBaseRow.getRowKey().length);
        assertEquals(3, hBaseRow.getQualifierValMap().size());
        NavigableMap<byte[], byte[]> qualifierMap = hBaseRow.getQualifierValMap();
        assertEquals("42.546245", Bytes.toString(qualifierMap.get(Bytes.toBytes("1"))));
        assertEquals("1.601554", Bytes.toString(qualifierMap.get(Bytes.toBytes("2"))));
        String[] decodeRow = lookupRowEncoder.decode(hBaseRow);
        assertArrayEquals(row, decodeRow);
    }

    @Test
    public void testEnDeCodeWithNullValue() {
        HBaseLookupRowEncoder lookupRowEncoder = new HBaseLookupRowEncoder(tableDesc, new String[] { "COUNTRY" }, 1);
        String[] row = new String[] { "AD", "42.546245", "1.601554", null };
        HBaseRow hBaseRow = lookupRowEncoder.encode(row);

        assertEquals(6, hBaseRow.getRowKey().length);
        assertEquals(3, hBaseRow.getQualifierValMap().size());
        NavigableMap<byte[], byte[]> qualifierMap = hBaseRow.getQualifierValMap();
        assertEquals("42.546245", Bytes.toString(qualifierMap.get(Bytes.toBytes("1"))));
        assertEquals("1.601554", Bytes.toString(qualifierMap.get(Bytes.toBytes("2"))));
        String[] decodeRow = lookupRowEncoder.decode(hBaseRow);
        assertNull(decodeRow[3]);
        assertArrayEquals(row, decodeRow);
    }

    @Test
    public void testEnDeCodeWithMultiKeys() {
        HBaseLookupRowEncoder lookupRowEncoder = new HBaseLookupRowEncoder(tableDesc,
                new String[] { "COUNTRY", "NAME" }, 1);
        String[] row = new String[] { "AD", "42.546245", "1.601554", "Andorra" };
        HBaseRow hBaseRow = lookupRowEncoder.encode(row);

        assertEquals(2, hBaseRow.getQualifierValMap().size());
        NavigableMap<byte[], byte[]> qualifierMap = hBaseRow.getQualifierValMap();
        assertEquals("42.546245", Bytes.toString(qualifierMap.get(Bytes.toBytes("1"))));
        assertEquals("1.601554", Bytes.toString(qualifierMap.get(Bytes.toBytes("2"))));
        String[] decodeRow = lookupRowEncoder.decode(hBaseRow);
        assertArrayEquals(row, decodeRow);
    }

}
