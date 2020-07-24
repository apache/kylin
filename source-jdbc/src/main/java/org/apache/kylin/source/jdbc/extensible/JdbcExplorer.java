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
package org.apache.kylin.source.jdbc.extensible;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.jdbc.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class JdbcExplorer implements ISourceMetadataExplorer, ISampleDataDeployer {
    private static final Logger logger = LoggerFactory.getLogger(JdbcExplorer.class);

    private JdbcConnector dataSource;

    public JdbcExplorer(JdbcConnector dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        return dataSource.listDatabases();
    }

    @Override
    public List<String> listTables(String schema) throws SQLException {
        return dataSource.listTables(schema);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj)
            throws SQLException {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase(database.toUpperCase(Locale.ROOT));
        tableDesc.setName(table.toUpperCase(Locale.ROOT));
        tableDesc.setUuid(UUID.randomUUID().toString());
        tableDesc.setLastModified(0);
        tableDesc.setProject(prj);
        tableDesc.setSourceType(JdbcSource.SOURCE_ID);

        try (CachedRowSet tables = dataSource.getTable(database, table)) {
            String tableType = null;
            while (tables.next()) {
                tableType = tables.getString("TABLE_TYPE");
            }
            if (tableType != null) {
                tableDesc.setTableType(tableType);
            } else {
                throw new RuntimeException(String.format(Locale.ROOT, "table %s not found in schema:%s", table, database));
            }
        }

        try (CachedRowSet columns = dataSource.listColumns(database, table)) {
            List<ColumnDesc> columnDescs = new ArrayList<>();

            while (columns.next()) {
                String cname = columns.getString("COLUMN_NAME");
                int type = columns.getInt("DATA_TYPE");
                int csize = columns.getInt("COLUMN_SIZE");
                int digits = columns.getInt("DECIMAL_DIGITS");
                int pos = columns.getInt("ORDINAL_POSITION");
                String remarks = columns.getString("REMARKS");

                ColumnDesc cdesc = new ColumnDesc();
                cdesc.setName(cname.toUpperCase(Locale.ROOT));

                String kylinType = dataSource.toKylinTypeName(type);
                if ("any".equals(kylinType)) {
                    String typeName = columns.getString("TYPE_NAME");
                    int kylinTypeId = dataSource.toKylinTypeId(typeName, type);
                    kylinType = dataSource.toKylinTypeName(kylinTypeId);
                }
                int precision = (SqlUtil.isPrecisionApplicable(kylinType) && csize > 0) ? csize : -1;
                precision = Math.min(precision, KylinConfig.getInstanceFromEnv().getDefaultVarcharPrecision());
                int scale = (SqlUtil.isScaleApplicable(kylinType) && digits > 0) ? digits : -1;

                cdesc.setDatatype(new DataType(kylinType, precision, scale).toString());
                cdesc.setId(String.valueOf(pos));
                cdesc.setComment(remarks);
                columnDescs.add(cdesc);
            }

            tableDesc.setColumns(columnDescs.toArray(new ColumnDesc[columnDescs.size()]));

            TableExtDesc tableExtDesc = new TableExtDesc();
            tableExtDesc.setIdentity(tableDesc.getIdentity());
            tableExtDesc.setUuid(UUID.randomUUID().toString());
            tableExtDesc.setLastModified(0);
            tableExtDesc.init(prj);

            return Pair.newPair(tableDesc, tableExtDesc);
        }
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        String[] sql = dataSource.buildSqlToCreateSchema(database);
        dataSource.executeUpdate(sql);
    }

    @Override
    public void loadSampleData(String tableName, String tmpDataDir) throws Exception {
        String[] sql = dataSource.buildSqlToLoadDataFromLocal(tableName, tmpDataDir);
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
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        String[] sqls = dataSource.buildSqlToCreateView(viewName, "SELECT * FROM " + origTableName);
        dataSource.executeUpdate(sqls);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public ColumnDesc[] evalQueryMetadata(String query) {
        if (StringUtils.isEmpty(query)) {
            throw new RuntimeException("Evaluate query shall not be empty.");
        }

        try (Connection conn = dataSource.getConnection();
            Statement state = conn.createStatement();
            ResultSet rs = state.executeQuery(dataSource.convertSql(query))) {
            ResultSetMetaData rsMeta = rs.getMetaData();
            ColumnDesc[] columnDescs = new ColumnDesc[rsMeta.getColumnCount()];
            for (int i = 0; i < columnDescs.length; i++) {
                columnDescs[i] = new ColumnDesc();
                columnDescs[i].setName(rsMeta.getColumnName(i + 1).toUpperCase(Locale.ROOT));

                String kylinType = dataSource.toKylinTypeName(rsMeta.getColumnType(i + 1));
                int precision = (SqlUtil.isPrecisionApplicable(kylinType) && rsMeta.getPrecision(i + 1) > 0)
                        ? rsMeta.getPrecision(i + 1)
                        : -1;
                int scale = (SqlUtil.isScaleApplicable(kylinType) && rsMeta.getScale(i + 1) > 0)
                        ? rsMeta.getScale(i + 1)
                        : -1;

                columnDescs[i].setDatatype(new DataType(kylinType, precision, scale).toString());
                columnDescs[i].setId(String.valueOf(i + 1));
            }
            return columnDescs;
        } catch (Exception e) {
            throw new RuntimeException("Cannot evaluate metadata of query: " + query, e);
        }
    }

    @Override
    public void validateSQL(String query) throws Exception {
        dataSource.testSql(dataSource.convertSql(query));
    }
}
