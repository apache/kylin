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

package com.kylinolap.storage.hbase;

import java.util.List;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.invertedindex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;

/**
 * @author yangli9
 */
public class InvertedIndexHBaseTest extends HBaseMetadataTestCase {

    CubeInstance cube;
    CubeSegment seg;
    HConnection hconn;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();

        this.cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_ii");
        this.seg = cube.getFirstSegment();

        String hbaseUrl = KylinConfig.getInstanceFromEnv().getStorageUrl();
        Configuration hconf = HadoopUtil.newHBaseConfiguration(hbaseUrl);
        hconn = HConnectionManager.createConnection(hconf);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLoad() throws Exception {

        String tableName = seg.getStorageLocationIdentifier();
        IIKeyValueCodec codec = new IIKeyValueCodec(new TableRecordInfo(seg));

        List<Slice> slices = Lists.newArrayList();
        HBaseClientKVIterator kvIterator = new HBaseClientKVIterator(hconn, tableName, InvertedIndexDesc.HBASE_FAMILY_BYTES, InvertedIndexDesc.HBASE_QUALIFIER_BYTES);
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
            for (TableRecordBytes rec : slice) {
                records.add((TableRecord) rec.clone());
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
