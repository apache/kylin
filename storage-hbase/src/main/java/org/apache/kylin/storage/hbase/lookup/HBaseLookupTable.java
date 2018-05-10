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

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.lookup.HBaseLookupRowEncoder.HBaseRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Use HBase as lookup table storage
 */
public class HBaseLookupTable implements ILookupTable{
    protected static final Logger logger = LoggerFactory.getLogger(HBaseLookupTable.class);

    private TableName lookupTableName;
    private Table table;

    private HBaseLookupRowEncoder encoder;

    public HBaseLookupTable(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshot) {
        String tableName = extTableSnapshot.getStorageLocationIdentifier();
        this.lookupTableName = TableName.valueOf(tableName);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Connection connection = HBaseConnection.get(kylinConfig.getStorageUrl());
        try {
            table = connection.getTable(lookupTableName);
        } catch (IOException e) {
            throw new RuntimeException("error when connect HBase", e);
        }

        String[] keyColumns = extTableSnapshot.getKeyColumns();
        encoder = new HBaseLookupRowEncoder(tableDesc, keyColumns, extTableSnapshot.getShardNum());
    }

    @Override
    public String[] getRow(Array<String> key) {
        byte[] encodedKey = encoder.encodeRowKey(key.data);
        Get get = new Get(encodedKey);
        try {
            Result result = table.get(get);
            if (result.isEmpty()) {
                return null;
            }
            return encoder.decode(new HBaseRow(result.getRow(), result.getFamilyMap(HBaseLookupRowEncoder.CF)));
        } catch (IOException e) {
            throw new RuntimeException("error when get row from hBase", e);
        }
    }

    @Override
    public Iterator<String[]> iterator() {
        return new HBaseScanBasedIterator(table);
    }

    @Override
    public void close() throws IOException{
        table.close();
    }

    private class HBaseScanBasedIterator implements Iterator<String[]> {
        private Iterator<Result> scannerIterator;
        private long counter;

        public HBaseScanBasedIterator(Table table) {
            try {
                Scan scan = new Scan();
                scan.setCaching(1000);
                ResultScanner scanner = table.getScanner(HBaseLookupRowEncoder.CF);
                scannerIterator = scanner.iterator();
            } catch (IOException e) {
                logger.error("error when scan HBase", e);
            }
        }

        @Override
        public boolean hasNext() {
            return scannerIterator.hasNext();
        }

        @Override
        public String[] next() {
            counter ++;
            if (counter % 100000 == 0) {
                logger.info("scanned {} rows from hBase", counter);
            }
            Result result = scannerIterator.next();
            byte[] rowKey = result.getRow();
            NavigableMap<byte[], byte[]> qualifierValMap = result.getFamilyMap(HBaseLookupRowEncoder.CF);
            return encoder.decode(new HBaseRow(rowKey, qualifierValMap));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }
    }
}
