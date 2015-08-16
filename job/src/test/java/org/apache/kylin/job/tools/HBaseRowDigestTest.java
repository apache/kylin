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

package org.apache.kylin.job.tools;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by Hongbin Ma(Binmahone) on 2/6/15.
 */
@Ignore
public class HBaseRowDigestTest extends HBaseMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    private static final byte[] CF = "f".getBytes();
    private static final byte[] QN = "c".getBytes();
    static ImmutableBytesWritable k = new ImmutableBytesWritable();
    static ImmutableBytesWritable v = new ImmutableBytesWritable();

    @Test
    public static void test() throws IOException {
        String hbaseUrl = "hbase"; // use hbase-site.xml on classpath
        Connection conn = null;
        Table table = null;
        try {
            conn = HBaseConnection.get(hbaseUrl);
            table = conn.getTable(TableName.valueOf("KYLIN_II_YTYWP3CQGJ"));
            ResultScanner scanner = table.getScanner(CF, QN);
            StringBuffer sb = new StringBuffer();
            while (true) {
                Result r = scanner.next();
                if (r == null)
                    break;

                Cell[] cells = r.rawCells();
                Cell c = cells[0];

                k.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
                v.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());

                byte[] row = k.copyBytes();
                byte[] value = v.copyBytes();
                //                byte[] row = r.getRow();
                //                byte[] value = r.getValue(CF, QN);
                //
                sb.append("row length: " + row.length + "\r\n");
                sb.append(BytesUtil.toReadableText(row) + "\r\n");
                sb.append("value length: " + value.length + "\r\n");
                sb.append(BytesUtil.toReadableText(value) + "\r\n");
            }
            System.out.println(sb.toString());
            FileUtils.writeStringToFile(new File("/Users/honma/Desktop/a3"), sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                table.close();
            if (conn != null)
                conn.close();
        }

    }
}
