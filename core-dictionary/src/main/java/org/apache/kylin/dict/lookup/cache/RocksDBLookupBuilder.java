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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.cache.RocksDBLookupRowEncoder.KV;
import org.apache.kylin.metadata.model.TableDesc;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBLookupBuilder {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBLookupBuilder.class);

    static {
        RocksDB.loadLibrary();
    }
    private Options options;
    private String dbPath;
    private TableDesc tableDesc;
    private RocksDBLookupRowEncoder encoder;
    private int writeBatchSize;

    public RocksDBLookupBuilder(TableDesc tableDesc, String[] keyColumns, String dbPath) {
        this.tableDesc = tableDesc;
        this.encoder = new RocksDBLookupRowEncoder(tableDesc, keyColumns);
        this.dbPath = dbPath;
        this.writeBatchSize = 500;
        this.options = new Options();
        options.setCreateIfMissing(true).setWriteBufferSize(8 * SizeUnit.KB).setMaxWriteBufferNumber(3)
                .setMaxBackgroundCompactions(5).setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);

    }

    public void build(ILookupTable srcLookupTable) {
        File dbFolder = new File(dbPath);
        if (dbFolder.exists()) {
            logger.info("remove rocksdb folder:{} to rebuild table cache:{}", dbPath, tableDesc.getIdentity());
            FileUtils.deleteQuietly(dbFolder);
        } else {
            logger.info("create new rocksdb folder:{} for table cache:{}", dbPath, tableDesc.getIdentity());
            dbFolder.mkdirs();
        }
        logger.info("start to build lookup table:{} to rocks db:{}", tableDesc.getIdentity(), dbPath);
        try (RocksDB rocksDB = RocksDB.open(options, dbPath)) {
            // todo use batch may improve write performance
            for (String[] row : srcLookupTable) {
                KV kv = encoder.encode(row);
                rocksDB.put(kv.getKey(), kv.getValue());
            }
        } catch (RocksDBException e) {
            logger.error("error when put data to rocksDB", e);
            throw new RuntimeException("error when write data to rocks db", e);
        }

        logger.info("source table:{} has been written to rocks db:{}", tableDesc.getIdentity(), dbPath);
    }
}
