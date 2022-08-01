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
package org.apache.kylin.engine.spark.mockup.external;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.externalCatalog.api.ApiException;
import org.apache.kylin.externalCatalog.api.catalog.Database;
import org.apache.kylin.externalCatalog.api.catalog.IExternalCatalog;
import org.apache.kylin.externalCatalog.api.catalog.Partition;
import org.apache.kylin.externalCatalog.api.catalog.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Maps;

public class UBSViewCatalog implements IExternalCatalog {

    static public final String VIEW_NAME = "x";
    static public final String DB_NAME = "UBSVIEWCATALOG";

    static private final Database DEFAULT = new Database("UBSVIEWCATALOG", "", "", Maps.newHashMap());

    static private final Table xTable = createDefaultTable();

    public UBSViewCatalog(Configuration hadoopConfig) {
    }

    static private Table createDefaultTable() {
        Table t = new Table(VIEW_NAME, DB_NAME);
        t.setFields(Collections.emptyList());
        return t;
    }

    @Override
    public List<String> getDatabases(String databasePattern) throws ApiException {
        return Collections.singletonList(DB_NAME);
    }

    @Override
    public Database getDatabase(String databaseName) throws ApiException {
        return DEFAULT;
    }

    @Override
    public Table getTable(String dbName, String tableName, boolean throwException) throws ApiException {
        if (VIEW_NAME.equalsIgnoreCase(tableName)) {
            return xTable;
        } else {
            return null;
        }

    }

    @Override
    public List<String> getTables(String dbName, String tablePattern) throws ApiException {
        return null;
    }

    @Override
    public Dataset<Row> getTableData(SparkSession session, String dbName, String tableName, boolean throwException)
            throws ApiException {
        if (tableName.equalsIgnoreCase(VIEW_NAME)) {
            String viewSql = "select id, sum(t0) from dim group by id";
            return session.sql(viewSql);
        }
        return null;
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tablePattern) throws ApiException {
        return null;
    }
}
