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

import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author yangli9
 */
public class InvertedIndexHBaseTest extends HBaseMetadataTestCase {

    IIInstance ii;
    IISegment seg;
    Connection hconn;

    TableRecordInfo info;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();

        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii");
        this.seg = ii.getFirstSegment();

        this.hconn = HBaseConnection.get();

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
        HBaseClientKVIterator kvIterator = new HBaseClientKVIterator(hconn, tableName, IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_QUALIFIER_BYTES);
        try {
            for (Slice slice : codec.decodeKeyValue(kvIterator)) {
                slices.add(slice);
            }
        } finally {
            kvIterator.close();
        }

        List<TableRecord> records = iterateRecords(slices);
        dump(records);
        System.out.println(records.size() + " records");
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

    private void dump(Iterable<TableRecord> records) {
        for (TableRecord rec : records) {
            System.out.println(rec.toString());

            byte[] x = rec.getBytes();
            String y = BytesUtil.toReadableText(x);
            System.out.println(y);
            System.out.println();
        }
    }

}
