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
package org.apache.kylin.sdk.datasource.adaptor;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;


public class PostgresqlAdaptor extends DefaultAdaptor {
    public PostgresqlAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection()) {
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet schemaList = metaData.getSchemas();
            while (schemaList.next()) {
                String schmea = schemaList.getString("TABLE_SCHEM");
                ret.add(schmea);
            }
        }
        return ret;
    }

    @Override
    public List<String> listTables(String catalog) throws SQLException {
        List<String> ret = new ArrayList<>();
        String[] tableTypes = {"TABLE"};
        try (Connection con = getConnection(); ResultSet res = con.getMetaData().getTables(null, catalog, null, tableTypes)) {
            String table;
            while (res.next()) {
                table = res.getString("TABLE_NAME");
                ret.add(table);
            }
        }
        return ret;
    }

    @Override
    public CachedRowSet getTable(String catalog, String table) throws SQLException {
        if (configurer.isCaseSensitive()) {
            catalog = getRealCatalogName(catalog);
            table = getRealTableName(catalog, table);
        }
        try (Connection conn = getConnection(); ResultSet rs = conn.getMetaData().getTables(catalog, null, table, null)) {
            return cacheResultSet(rs);
        }
    }

    @Override
    public CachedRowSet getTableColumns(String catalog, String table) throws SQLException {
        if (configurer.isCaseSensitive()) {
            catalog = getRealCatalogName(catalog);
            table = getRealTableName(catalog, table);
        }
        try (Connection conn = getConnection();
             ResultSet rs = conn.getMetaData().getColumns(catalog, null, table, null)) {
            return cacheResultSet(rs);
        }
    }

    private String getRealCatalogName(String catalog) throws SQLException {
        List<String> catalogs = super.listDatabasesWithCache();
        for (String s : catalogs) {
            if (s.equalsIgnoreCase(catalog)) {
                catalog = s;
                break;
            }
        }
        return catalog;
    }

    private String getRealTableName(String catalog, String table) throws SQLException {
        List<String> tables = super.listTables(catalog);
        for (String t : tables) {
            if (t.equalsIgnoreCase(table)) {
                table = t;
                break;
            }
        }
        return table;
    }

}
