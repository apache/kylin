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

package org.apache.kylin.storage.hbase.ii;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.storage.hbase.cube.v1.HBaseClientKVIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author yangli9
 */
public class ITInvertedIndexHBaseTest extends HBaseMetadataTestCase {

    IIInstance ii;
    IISegment seg;
    HConnection hconn;

    TableRecordInfo info;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();

        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_left_join");
        this.seg = ii.getFirstSegment();

        Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
        hconn = HConnectionManager.createConnection(hconf);

        this.info = new TableRecordInfo(seg);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLoad() throws Exception {

        String tableName = seg.getStorageLocationIdentifier();
        IIKeyValueCodec codec = new IIKeyValueCodec(info.getDigest());

        List<Slice> slices = Lists.newArrayList();
        HBaseClientKVIterator kvIterator = new HBaseClientKVIterator(hconn, tableName, IIDesc.HBASE_FAMILY_BYTES);
        try {
            for (Slice slice : codec.decodeKeyValue(kvIterator)) {
                slices.add(slice);
            }
        } finally {
            kvIterator.close();
        }

        List<TableRecord> records = iterateRecords(slices);
        //dump(records);
        System.out.println("table name:" + tableName + " has " + records.size() + " records");
    }

    private List<TableRecord> iterateRecords(List<Slice> slices) {
        List<TableRecord> records = Lists.newArrayList();
        for (Slice slice : slices) {
            for (RawTableRecord rec : slice) {
                records.add(new TableRecord((RawTableRecord) rec.clone(), info));
            }
        }
        return records;
    }

    @SuppressWarnings("unused")
    private void dump(Iterable<TableRecord> records) {
        for (TableRecord rec : records) {
            byte[] x = rec.getBytes();
            String y = BytesUtil.toReadableText(x);
            System.out.println(y);
            System.out.println();
        }
    }

}
