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

package com.kylinolap.dict.lookup;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.tool.HiveClient;

/**
 * @author yangli9
 * 
 */
public class HiveTable implements ReadableTable {

    private static final Logger logger = LoggerFactory.getLogger(HiveTable.class);

    private String hiveTable;
    private int nColumns;
    private String hdfsLocation;
    private FileTable fileTable;

    public HiveTable(MetadataManager metaMgr, String table) {
        this.hiveTable = table;
        this.nColumns = metaMgr.getTableDesc(table).getColumnCount();
    }

    @Override
    public String getColumnDelimeter() throws IOException {
        return getFileTable().getColumnDelimeter();
    }

    @Override
    public TableReader getReader() throws IOException {
        return getFileTable().getReader();
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return getFileTable().getSignature();
    }

    private FileTable getFileTable() throws IOException {
        if (fileTable == null) {
            fileTable = new FileTable(getHDFSLocation(true), nColumns);
        }
        return fileTable;
    }

    public String getHDFSLocation(boolean needFilePath) throws IOException {
        if (hdfsLocation == null) {
            hdfsLocation = computeHDFSLocation(needFilePath);
        }
        return hdfsLocation;
    }

    private String computeHDFSLocation(boolean needFilePath) throws IOException {

        String override = KylinConfig.getInstanceFromEnv().getOverrideHiveTableLocation(hiveTable);
        if (override != null) {
            logger.debug("Override hive table location " + hiveTable + " -- " + override);
            return override;
        }

        HiveMetaStoreClient hiveClient = HiveClient.getInstance().getMetaStoreClient();
        Table table = null;
        try {
            table = hiveClient.getTable(hiveTable);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        
        return table.getSd().getLocation();

    }

    @Override
    public String toString() {
        return "hive:" + hiveTable;
    }

}
