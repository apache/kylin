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

package org.apache.kylin.source.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISourceMetadataExplorer;

public class HiveMetadataExplorer implements ISourceMetadataExplorer {

    @Override
    public List<String> listDatabases() throws Exception {
        IHiveClient hiveClient = HiveClientFactory.getHiveClient();
        return hiveClient.getHiveDbNames();
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        IHiveClient hiveClient = HiveClientFactory.getHiveClient();
        return hiveClient.getHiveTableNames(database);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String tableName) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        IHiveClient hiveClient = HiveClientFactory.getHiveClient();
        MetadataManager metaMgr = MetadataManager.getInstance(config);

        HiveTableMeta hiveTableMeta;
        try {
            hiveTableMeta = hiveClient.getHiveTableMeta(database, tableName);
        } catch (Exception e) {
            throw new RuntimeException("cannot get HiveTableMeta", e);
        }

        TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName);
        if (tableDesc == null) {
            tableDesc = new TableDesc();
            tableDesc.setDatabase(database.toUpperCase());
            tableDesc.setName(tableName.toUpperCase());
            tableDesc.setUuid(UUID.randomUUID().toString());
            tableDesc.setLastModified(0);
        }
        if (hiveTableMeta.tableType != null) {
            tableDesc.setTableType(hiveTableMeta.tableType);
        }

        int columnNumber = hiveTableMeta.allColumns.size();
        List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
        for (int i = 0; i < columnNumber; i++) {
            HiveTableMeta.HiveTableColumnMeta field = hiveTableMeta.allColumns.get(i);
            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(field.name.toUpperCase());
            // use "double" in kylin for "float"
            if ("float".equalsIgnoreCase(field.dataType)) {
                cdesc.setDatatype("double");
            } else {
                cdesc.setDatatype(field.dataType);
            }
            cdesc.setId(String.valueOf(i + 1));
            cdesc.setComment(field.comment);
            columns.add(cdesc);
        }
        tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));

        StringBuffer partitionColumnString = new StringBuffer();
        for (int i = 0, n = hiveTableMeta.partitionColumns.size(); i < n; i++) {
            if (i > 0)
                partitionColumnString.append(", ");
            partitionColumnString.append(hiveTableMeta.partitionColumns.get(i).name.toUpperCase());
        }

        TableExtDesc tableExtDesc = metaMgr.getTableExt(tableDesc.getIdentity());
        tableExtDesc.addDataSourceProp("location", hiveTableMeta.sdLocation);
        tableExtDesc.addDataSourceProp("owner", hiveTableMeta.owner);
        tableExtDesc.addDataSourceProp("last_access_time", String.valueOf(hiveTableMeta.lastAccessTime));
        tableExtDesc.addDataSourceProp("partition_column", partitionColumnString.toString());
        tableExtDesc.addDataSourceProp("total_file_size", String.valueOf(hiveTableMeta.fileSize));
        tableExtDesc.addDataSourceProp("total_file_number", String.valueOf(hiveTableMeta.fileNum));
        tableExtDesc.addDataSourceProp("hive_inputFormat", hiveTableMeta.sdInputFormat);
        tableExtDesc.addDataSourceProp("hive_outputFormat", hiveTableMeta.sdOutputFormat);
        tableExtDesc.addDataSourceProp("skip_header_line_count", String.valueOf(hiveTableMeta.skipHeaderLineCount));

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }
}
