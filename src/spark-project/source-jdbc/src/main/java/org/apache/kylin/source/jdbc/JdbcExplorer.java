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
package org.apache.kylin.source.jdbc;

import static org.apache.kylin.common.exception.ServerErrorCode.DDL_CHECK_ERROR;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.SupportsSparkCatalog;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcExplorer implements ISourceMetadataExplorer, ISampleDataDeployer, SupportsSparkCatalog, Serializable {

    private JdbcConnector dataSource;

    public JdbcExplorer(JdbcConnector dataSource) {
        this.dataSource = dataSource;
    }

    public static boolean isPrecisionApplicable(String typeName) {
        return isScaleApplicable(typeName) || DataType.STRING_FAMILY.contains(typeName);
    }

    public static boolean isScaleApplicable(String typeName) {
        return typeName.equals("decimal") || typeName.equals("numeric"); // double and float are not allowed neither in hive.
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        String[] sql = dataSource.buildSqlToCreateSchema(database);
        dataSource.executeUpdate(sql);
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        LinkedHashMap<String, String> columnInfo = Maps.newLinkedHashMap();
        for (ColumnDesc columnDesc : table.getColumns()) {
            columnInfo.put(columnDesc.getName(), columnDesc.getTypeName());
        }
        String[] sqls = dataSource.buildSqlToCreateTable(table.getIdentity(), columnInfo);
        dataSource.executeUpdate(sqls);
    }

    @Override
    public void loadSampleData(String tableName, String tmpDataDir) {
        throw new UnsupportedOperationException("Unsupported load sample data");
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) {
        throw new UnsupportedOperationException("Unsupported create wrapper view");
    }

    @Override
    public List<String> listDatabases() throws Exception {
        List<String> databases = dataSource.listDatabases();
        if (KylinConfig.getInstanceFromEnv().isDDLLogicalViewEnabled()) {
            String logicalViewDB = KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB();
            databases.forEach(db -> {
                if (db.equalsIgnoreCase(logicalViewDB)) {
                    throw new KylinException(DDL_CHECK_ERROR,
                            "Logical view database should not be duplicated " + "with normal hive database!!!");
                }
            });
            List<String> databasesWithLogicalDB = Lists.newArrayList();
            databasesWithLogicalDB.add(logicalViewDB);
            databasesWithLogicalDB.addAll(databases);
            databases = databasesWithLogicalDB;
        }
        return databases;
    }

    public static boolean isJdbcLogicalViewDataBase(String database) {
        String logicalViewDB = KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB();
        return database.equals(logicalViewDB) && KylinConfig.getInstanceFromEnv().isDDLLogicalViewEnabled();
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        if (isJdbcLogicalViewDataBase(database)) {
            return SourceFactory.getSparkSource().getSourceMetadataExplorer().listTables(database);
        }
        return dataSource.listTables(database);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj) throws Exception {
        if (isJdbcLogicalViewDataBase(database)) {
            Pair<TableDesc, TableExtDesc> pair = SourceFactory.getSparkSource().getSourceMetadataExplorer()
                    .loadTableMetadata(database, table, prj);
            pair.getFirst().setSourceType(ISourceAware.ID_JDBC);
            return pair;
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, prj);
        TableDesc tableDesc = metaMgr.getTableDesc(database + "." + table);
        // make a new TableDesc instance, don't modify the one in use
        if (tableDesc == null) {
            tableDesc = new TableDesc();
            tableDesc.setDatabase(database);
            tableDesc.setName(table);
            tableDesc.setLastModified(0);
        } else {
            tableDesc = new TableDesc(tableDesc);
        }

        tableDesc.setSourceType(ISourceAware.ID_JDBC);
        tableDesc.init(prj);

        CachedRowSet tables = dataSource.getTable(database, table);
        String tableType = null;
        while (tables.next()) {
            tableType = tables.getString("TABLE_TYPE");
        }
        if (tableType != null) {
            tableDesc.setTableType(tableType);
        } else {
            throw new RuntimeException(String.format(Locale.ROOT, "table %s not found in schema:%s", table, database));
        }

        CachedRowSet columns = dataSource.listColumns(database, table);
        List<ColumnDesc> columnDescs = Lists.newArrayList();
        while (columns.next()) {
            String cname = columns.getString("COLUMN_NAME");
            int type = columns.getInt("DATA_TYPE");
            int csize = columns.getInt("COLUMN_SIZE");
            int digits = columns.getInt("DECIMAL_DIGITS");
            int pos = columns.getInt("ORDINAL_POSITION");
            String remarks = columns.getString("REMARKS");

            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName(cname.toUpperCase(Locale.ROOT));
            columnDesc.setCaseSensitiveName(cname);
            String kylinType = dataSource.toKylinTypeName(type);
            if ("any".equals(kylinType)) {
                String typeName = columns.getString("TYPE_NAME");
                int kylinTypeId = dataSource.toKylinTypeId(typeName, type);
                kylinType = dataSource.toKylinTypeName(kylinTypeId);
            }
            int precision = (isPrecisionApplicable(kylinType) && csize >= 0) ? csize : -1;
            precision = Math.min(precision, KylinConfig.getInstanceFromEnv().getDefaultVarcharPrecision());
            int scale = (isScaleApplicable(kylinType) && digits >= 0) ? digits : -1;
            columnDesc.setDatatype(new DataType(kylinType, precision, scale).toString());
            columnDesc.setId(String.valueOf(pos));
            columnDesc.setComment(remarks);
            columnDescs.add(columnDesc);
        }

        tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[columnDescs.size()]));

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkDatabaseAccess(String database) {
        return true;
    }

    @Override
    public boolean checkTablesAccess(Set<String> tables) {
        return true;
    }

    @Override
    public Set<String> getTablePartitions(String database, String table, String prj, String partitionCols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String addCatalog(KylinConfig kylinConfig, String sql, String project) {
        return dataSource.addCatalog(kylinConfig, sql, project);
    }    
    
    @Override
    public Map<String, String> getSourceCatalogConf(KylinConfig kylinConfig, String project) {
        return dataSource.getSourceCatalogConf(kylinConfig, project);
    }
}
