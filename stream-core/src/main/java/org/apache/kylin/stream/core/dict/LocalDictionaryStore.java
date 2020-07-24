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
package org.apache.kylin.stream.core.dict;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.common.util.ByteArray;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_FOR_EXCEPTION;
import static org.apache.kylin.stream.core.dict.StreamingDictionaryClient.ID_UNKNOWN;

/**
 * Please refer to https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
 */
public class LocalDictionaryStore implements Closeable {
    private static Logger logger = LoggerFactory.getLogger(LocalDictionaryStore.class);
    private RocksDB db;
    private File dictPath;
    private String baseStorePath = "DictCache";
    private Map<ByteArray, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
    String cubeName;

    public LocalDictionaryStore(String tableColumn) {
        this.dictPath = new File(baseStorePath, tableColumn);
        this.cubeName = tableColumn;
    }

    public void init(String[] cfs) throws Exception {
        logger.debug("Checking streaming dict local store for {} at {}.", cubeName, String.join(", ", cfs));
        if (!dictPath.exists() && dictPath.mkdirs()) {
            logger.warn("Create {} failed.", dictPath);
        }
        // maybe following options is naive, should improve in the future
        try (DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundCompactions(5)
                .setWritableFileMaxBufferSize(400 * SizeUnit.KB)) {
            String dataPath = dictPath.getAbsolutePath() + "/data";
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>(); // to be fill in
            for (String family : cfs) {
                ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
                        family.getBytes(StandardCharsets.UTF_8));
                columnFamilyDescriptorList.add(columnFamilyDescriptor);
            }
            logger.debug("Try to open rocksdb {}.", dataPath);
            db = RocksDB.open(options, dataPath, columnFamilyDescriptorList, columnFamilyHandleList);
            Preconditions.checkNotNull(db, "RocksDB cannot created for some reasons.");
            for (int i = 0; i < columnFamilyHandleList.size(); i++) {
                columnFamilyHandleMap.put(new ByteArray(cfs[i].getBytes(StandardCharsets.UTF_8)),
                        columnFamilyHandleList.get(i));
            }
        } catch (Exception e) {
            logger.error("Init rocks db failed.", e);
            throw e;
        }
        logger.debug("Init local dict succeed.");
    }

    public boolean put(ByteArray column, String key, Integer value) {
        try {
            ColumnFamilyHandle handle = columnFamilyHandleMap.get(column);
            Preconditions.checkNotNull(handle,
                    new String(column.array(), StandardCharsets.UTF_8) + " cannot find matched handle.");
            db.put(handle, key.getBytes(StandardCharsets.UTF_8), value.toString().getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (Exception rdbe) {
            logger.error("Put failed.", rdbe);
            return false;
        }
    }

    /**
     * Try get dictId for value.
     *
     * @return ID_UNKNOWN if not exists locally; ID_FOR_EXCEPTION for Exception;
     *      other positive value for real dictId
     */
    public int encode(ByteArray column, String value) {
        byte[] values;
        try {
            ColumnFamilyHandle handle = columnFamilyHandleMap.get(column);
            Preconditions.checkNotNull(handle,
                    new String(column.array(), StandardCharsets.UTF_8) + " cannot find matched handle.");
            values = db.get(handle, value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception rdbe) {
            logger.error("Can not get from rocksDB.", rdbe);
            return ID_FOR_EXCEPTION;
        }
        if (values != null) {
            if (values.length == 0) {
                logger.warn("Empty values for {}", value);
                return ID_UNKNOWN;
            } else {
                try {
                    return Integer.parseInt(new String(values, StandardCharsets.UTF_8));
                } catch (Exception e) {
                    logger.error("parseInt " + new ByteArray(values).toString(), e);
                    return ID_FOR_EXCEPTION;
                }
            }
        }
        return ID_UNKNOWN;
    }

    @Override
    public void close() {
        logger.debug("Close rocks db.");
        for (ColumnFamilyHandle familyHandle : columnFamilyHandleMap.values()) {
            familyHandle.close();
        }
        db.close();
    }
}
