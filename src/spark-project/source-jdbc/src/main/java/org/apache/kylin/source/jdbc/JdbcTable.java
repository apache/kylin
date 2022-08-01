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

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.source.IReadableTable;

public class JdbcTable implements IReadableTable {
    private final JdbcConnector dataSource;
    private final String database;
    private final String tableName;
    private final TableDesc tableDesc;

    public JdbcTable(JdbcConnector dataSource, TableDesc tableDesc) {
        this.dataSource = dataSource;
        this.tableDesc = tableDesc;
        this.database = tableDesc.getDatabase();
        this.tableName = tableDesc.getName();
    }

    @Override
    public TableReader getReader() throws IOException {
        return new JdbcTableReader(dataSource, tableDesc);
    }

    @Override
    public TableSignature getSignature() {
        String path = String.format(Locale.ROOT, "%s.%s", database, tableName);
        return new TableSignature(path, 0, System.currentTimeMillis());
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public String toString() {
        return "database=[" + database + "], table=[" + tableName + "]";
    }
}
