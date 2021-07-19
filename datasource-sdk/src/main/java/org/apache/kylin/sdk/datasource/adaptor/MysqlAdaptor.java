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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.CachedRowSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MysqlAdaptor extends DefaultAdaptor {
    private static final Logger logger = LoggerFactory.getLogger(MysqlAdaptor.class);

    public static final String ALLOW_LOAD_LOCAL_IN_FILE_NAME = "allowLoadLocalInfile=true";
    public static final String AUTO_DESERIALIZE = "autoDeserialize=true";
    public static final String ALLOW_LOCAL_IN_FILE_NAME = "allowLocalInfile=true";
    public static final String ALLOW_URL_IN_LOCAL_IN_FILE_NAME = "allowUrlInLocalInfile=true";
    private static final String APPEND_PARAMS = "allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false";

    public MysqlAdaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection()) {
            ret.add(con.getCatalog());
        }
        return ret;
    }

    @Override
    public List<String> listTables(String catalog) throws SQLException {
        List<String> ret = new ArrayList<>();
        try (Connection con = getConnection(); ResultSet res = con.getMetaData().getTables(catalog, null, null, null)) {
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
        if(configurer.isCaseSensitive()){
            catalog = getRealCatalogName(catalog);
            table = getRealTableName(catalog, table);
        }
        try (Connection conn = getConnection();ResultSet rs = conn.getMetaData().getTables(catalog, null, table, null)) {
            return cacheResultSet(rs);
        }
    }

    @Override
    public CachedRowSet getTableColumns(String catalog, String table) throws SQLException {
        if(configurer.isCaseSensitive()){
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

    @Override
    protected String filterPassword(String password) {
        if (password.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in password field is filtered", AUTO_DESERIALIZE);
            password = password.replace(AUTO_DESERIALIZE, "");
        }
        return password;
    }

    @Override
    protected String filterUser(String user) {
        if (user.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in username field is filtered", AUTO_DESERIALIZE);
            user = user.replace(AUTO_DESERIALIZE, "");
        }
        logger.debug("username : {}", user);
        return user;
    }

    @Override
    protected String filterUrl(String url) {
        if (url.contains("?")) {
            if (url.contains(ALLOW_LOAD_LOCAL_IN_FILE_NAME)) {
                url.replace(ALLOW_LOAD_LOCAL_IN_FILE_NAME, "allowLoadLocalInfile=false");
            }
            if (url.contains(AUTO_DESERIALIZE)) {
                url.replace(AUTO_DESERIALIZE, "allowLoadLocalInfile=false");
            }
            if (url.contains(ALLOW_LOCAL_IN_FILE_NAME)) {
                url.replace(ALLOW_LOCAL_IN_FILE_NAME, "allowLoadLocalInfile=false");
            }
            if (url.contains(ALLOW_URL_IN_LOCAL_IN_FILE_NAME)) {
                url.replace(ALLOW_URL_IN_LOCAL_IN_FILE_NAME, "allowLoadLocalInfile=false");
            }
        } else {
            url = url + "?" + APPEND_PARAMS;
        }
        return url;
    }
}
