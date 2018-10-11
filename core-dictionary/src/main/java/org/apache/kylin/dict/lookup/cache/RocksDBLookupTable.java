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

import java.io.IOException;
import java.util.Iterator;

import org.apache.kylin.common.util.Array;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.cache.RocksDBLookupRowEncoder.KV;
import org.apache.kylin.metadata.model.TableDesc;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBLookupTable implements ILookupTable {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBLookupTable.class);
    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;
    private Options options;

    private RocksDBLookupRowEncoder rowEncoder;

    public RocksDBLookupTable(TableDesc tableDesc, String[] keyColumns, String dbPath) {
        this.options = new Options();
        this.rowEncoder = new RocksDBLookupRowEncoder(tableDesc, keyColumns);
        try {
            this.rocksDB = RocksDB.openReadOnly(options, dbPath);
        } catch (RocksDBException e) {
            throw new IllegalStateException("cannot open rocks db in path:" + dbPath, e);
        }
    }

    @Override
    public String[] getRow(Array<String> key) {
        byte[] encodeKey = rowEncoder.encodeStringsWithLenPfx(key.data, false);
        try {
            byte[] value = rocksDB.get(encodeKey);
            if (value == null) {
                return null;
            }
            return rowEncoder.decode(new KV(encodeKey, value));
        } catch (RocksDBException e) {
            throw new IllegalStateException("error when get key from rocksdb", e);
        }
    }

    @Override
    public Iterator<String[]> iterator() {
        final RocksIterator rocksIterator = getRocksIterator();
        rocksIterator.seekToFirst();

        return new Iterator<String[]>() {
            int counter;

            @Override
            public boolean hasNext() {
                boolean valid = rocksIterator.isValid();
                if (!valid) {
                    rocksIterator.close();
                }
                return valid;
            }

            @Override
            public String[] next() {
                counter++;
                if (counter % 100000 == 0) {
                    logger.info("scanned {} rows from rocksDB", counter);
                }
                String[] result = rowEncoder.decode(new KV(rocksIterator.key(), rocksIterator.value()));
                rocksIterator.next();
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("not support operation");
            }
        };
    }

    private RocksIterator getRocksIterator() {
        return rocksDB.newIterator();
    }

    @Override
    public void close() throws IOException {
        options.close();
        if (rocksDB != null) {
            rocksDB.close();
        }
    }
}
