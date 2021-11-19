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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.spark.sql.SparderContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class SparkHiveClient implements IHiveClient {
    //key in hive metadata map
    private static final String CHAR_VARCHAR_TYPE_STRING = "__CHAR_VARCHAR_TYPE_STRING";
    private static final String HIVE_COMMENT = "comment";
    private static final String HIVE_TABLE_ROWS = "numRows";
    private static final String TABLE_TOTAL_SIZE = "totalSize";
    private static final String TABLE_FILE_NUM = "numFiles";

    protected SparkSession ss;
    protected SessionCatalog catalog;

    public SparkHiveClient() {
        ss = SparderContext.getOriginalSparkSession();
        catalog = ss.sessionState().catalog();
    }


    @Override
    public void executeHQL(String hql) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeHQL(String[] hqls) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public HiveTableMeta getHiveTableMeta(String database, String tableName) throws Exception {
        HiveTableMetaBuilder builder = new HiveTableMetaBuilder();
        CatalogTable catalogTable = catalog
                .getTempViewOrPermanentTableMetadata(new TableIdentifier(tableName, Option.apply(database)));
        scala.collection.immutable.List<StructField> structFieldList = catalogTable.schema().toList();
        Iterator<StructField> structFieldIterator = structFieldList.iterator();

        List<HiveTableMeta.HiveTableColumnMeta> allColumns = Lists.newArrayList();
        List<HiveTableMeta.HiveTableColumnMeta> partitionColumns = Lists.newArrayList();
        while (structFieldIterator.hasNext()) {
            StructField structField = structFieldIterator.next();
            String name = structField.name();
            String hiveDataType = structField.dataType().simpleString();
            Metadata metadata = structField.metadata();
            String description = metadata.contains(HIVE_COMMENT) ? metadata.getString(HIVE_COMMENT) : "";
            String datatype = metadata.contains(CHAR_VARCHAR_TYPE_STRING) ? metadata.getString(CHAR_VARCHAR_TYPE_STRING) : hiveDataType;

            allColumns.add(new HiveTableMeta.HiveTableColumnMeta(name, datatype, description));
            if (catalogTable.partitionColumnNames().contains(name)) {
                partitionColumns.add(new HiveTableMeta.HiveTableColumnMeta(name, datatype, description));
            }
        }

        Map<String, String> properties = catalogTable.ignoredProperties();
        builder.setAllColumns(allColumns);
        builder.setPartitionColumns(partitionColumns);
        if (catalogTable.tableType().equals(CatalogTableType.MANAGED())) {
            builder.setSdLocation(catalogTable.location().getPath());
        } else {
            builder.setSdLocation("unknown");
        }
        long totalSize = properties.contains(TABLE_TOTAL_SIZE) ? Long.parseLong(properties.apply(TABLE_TOTAL_SIZE)) : 0L;
        builder.setFileSize(totalSize);
        long totalFileNum = properties.contains(TABLE_FILE_NUM) ? Long.parseLong(properties.apply(TABLE_FILE_NUM)) : 0L;
        builder.setFileNum(totalFileNum);
        builder.setIsNative(catalogTable.tableType().equals(CatalogTableType.MANAGED()));
        builder.setTableName(tableName);
        builder.setSdInputFormat(catalogTable.storage().inputFormat().toString());
        builder.setSdOutputFormat(catalogTable.storage().outputFormat().toString());
        builder.setOwner(catalogTable.owner());
        builder.setLastAccessTime(catalogTable.lastAccessTime());
        builder.setTableType(catalogTable.tableType().name());

        return builder.createHiveTableMeta();
    }

    @Override
    public List<String> getHiveDbNames() throws Exception {
        return scala.collection.JavaConversions.seqAsJavaList(catalog.listDatabases());
    }

    @Override
    public List<String> getHiveTableNames(String database) throws Exception {
        List<TableIdentifier> tableIdentifiers = scala.collection.JavaConversions.seqAsJavaList(catalog.listTables(database));
        List<String> tableNames = tableIdentifiers.stream().map(table -> table.table()).collect(Collectors.toList());
        return tableNames;
    }

    @Override
    public long getHiveTableRows(String database, String tableName) throws Exception {
        Map<String, String> properties = catalog.getTempViewOrPermanentTableMetadata(new TableIdentifier(tableName, Option.apply(database))).ignoredProperties();
        long hiveTableRows = properties.contains(HIVE_TABLE_ROWS) ? Long.parseLong(properties.apply(HIVE_TABLE_ROWS)) : 0L;
        return hiveTableRows;
    }

    /*
    This method was originally used for pushdown query.
    The method of pushdown query in kylin4 is PushDownRunnerSparkImpl.executeQuery, so getHiveResult is not implemented here.
     */
    @Override
    public List<Object[]> getHiveResult(String sql) throws Exception {
        return null;
    }
}
