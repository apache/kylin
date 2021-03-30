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
package org.apache.kylin.source.jdbc.metadata;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.SourceDialect;
import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.jdbc.SqlUtil;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class SQLServerJdbcMetadata extends DefaultJdbcMetadata {
    public SQLServerJdbcMetadata(DBConnConf dbConnConf) {
        super(dbConnConf);
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        Set<String> ret = new HashSet<>();
        try (Connection con = SqlUtil.getConnection(dbconf)) {

            String database = con.getCatalog();
            Preconditions.checkArgument(StringUtils.isNotEmpty(database),
                    "SQL Server needs a specific database in " + "connection string.");

            try (ResultSet rs = con.getMetaData().getTables(database, null, null, null)) {
                String schema;
                String catalog;
                while (rs.next()) {
                    schema = rs.getString("TABLE_SCHEM");
                    catalog = rs.getString("TABLE_CAT");
                    if (database.equals(catalog) || schema.equals("dbo")) {
                        ret.add(schema);
                    }
                }
            }
        }
        return new ArrayList<>(ret);
    }

    @Override
    public SourceDialect getDialect() {
        return SourceDialect.MSSQL;
    }
}
