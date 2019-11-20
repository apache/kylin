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

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParser;

import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.source.spark.ScalaUtils;
import org.apache.kylin.source.spark.SparkDataSourceCache;
import org.apache.kylin.source.spark.SparkDataSourceDesc;
import org.apache.kylin.source.spark.SparkSqlSource;


/**
 * This step is to allow spark datasource data to participate in building a flat table. There are
 * several main steps:
 *      1. Identify which spark datasource table is used in the create table ddl of flat table
 *      2. Register the table corresponding to spark datasource to the hive metastore.
 *          - Create db if db does not exist
 *          - If an hive table with the same name already exists, an error is reported
 *          - If an datasource table with the same name already exists, replace it
 *      3. Create a flat table with spark sql
 * The spark datasource table in the hive metastore is not deleted after the flat table is created,
 * because another build task might use the table.
 */
public class CreateSparkDataSourceTableStep extends AbstractExecutable {
    private final String defaultDb;
    private final String selectDataSql;

    public CreateSparkDataSourceTableStep(String defaultDb, String selectDataSql) {
        this.defaultDb = defaultDb;
        this.selectDataSql = selectDataSql;
    }

    private boolean isIdentifier(SqlNode sqlNode) {
        return sqlNode instanceof SqlIdentifier;
    }

    private boolean isIdentifierAlias(SqlNode sqlNode) {
        boolean ret = false;
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getKind() == SqlKind.AS
                && sqlBasicCall.operands.length == 2
                && isIdentifier(sqlBasicCall.operands[0])) {
                ret = true;
            }
        }
        return ret;
    }

    private boolean isIdentifierOrAlias(SqlNode sqlNode) {
        return isIdentifier(sqlNode) || isIdentifierAlias(sqlNode);
    }

    private SqlIdentifier getIdentifier(SqlNode sqlNode) {
        if (isIdentifier(sqlNode)) {
            return (SqlIdentifier)sqlNode;
        } else if (isIdentifierAlias(sqlNode)) {
            return (SqlIdentifier) ((SqlBasicCall)sqlNode).getOperands()[0];
        } else {
            return null;
        }
    }

    private void findSparkDataSourceTable(SqlNode sqlNode, List<Tuple2<String, String>> tables) {
        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect)sqlNode;
            findSparkDataSourceTable(sqlSelect.getFrom(), tables);
        } else if (sqlNode instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin)sqlNode;
            findSparkDataSourceTable(sqlJoin.getLeft(), tables);
            findSparkDataSourceTable(sqlJoin.getRight(), tables);
        } else if (isIdentifierOrAlias(sqlNode)) {
            SqlIdentifier sqlIdentifier = getIdentifier(sqlNode);
            String db;
            String tableName;
            switch (sqlIdentifier.names.size()) {
                case 1:
                    db = defaultDb;
                    tableName = sqlIdentifier.names.get(0);
                    break;
                case 2:
                    db = sqlIdentifier.names.get(0);
                    tableName = sqlIdentifier.names.get(1);
                    break;
                default:
                    throw new RuntimeException("Illegal table name: " + sqlIdentifier.toString());
            }

            if (null != SparkDataSourceCache.get(db + "." + tableName)) {
                tables.add(new Tuple2<>(db, tableName));
            }
        } else {
            // do nothing
        }
    }

    private void createTableForSparkDataSource(Tuple2<String, String> dbTable) throws IOException, URISyntaxException {
        // Create database if not exists
        ExternalCatalog catalog = SparkSqlSource.sparkSession().sessionState()
                .catalog().externalCatalog();
        String dbName = dbTable._1();
        if (!catalog.databaseExists(dbName)) {
            URI locationUri = Files.createTempDirectory("db_").normalize().toUri();
            CatalogDatabase catalogDatabase = CatalogDatabase.apply(dbName,
                    "Database for Kylin spark datasource tables",
                    locationUri,
                    new HashMap<String, String>());
            // ignoreIfExists is true to avoid another step just create db
            catalog.createDatabase(catalogDatabase, true);
        }

        // Create table if not exists
        String tableName = dbTable._2();
        SparkDataSourceDesc dataSourceDesc = SparkDataSourceCache.get(dbName + "." + tableName);
        DataSource dataSource = dataSourceDesc.toDataSource();

        TableIdentifier identifier = TableIdentifier.apply(tableName, Option.apply(dbName));
        CatalogTableType tableType = CatalogTableType.EXTERNAL();
        URI locationUri = new URI(dataSource.paths().mkString(","));
        CatalogStorageFormat storageFormat = CatalogStorageFormat.apply(Option.apply(locationUri),
                Option.empty(), Option.empty(), Option.empty(), false,
                new HashMap<String, String>());

        CatalogTable catalogTable = CatalogTable.apply(identifier,
                tableType,
                storageFormat,
                dataSource.resolveRelation(false).schema(),
                Option.apply(dataSource.className()),
                dataSource.partitionColumns(),
                dataSource.bucketSpec(),
                "Kylin",
                System.currentTimeMillis(),
                -1,
                "",
                dataSource.options(),
                Option.empty(),
                Option.empty(),
                Option.empty(),
                ScalaUtils.emptySeq(),
                false,
                true,
                new HashMap<String, String>()
        );
        if (false == catalog.tableExists(dbName, tableName)) {
            // ignoreIfExists is true to avoid another step just create table
            catalog.createTable(catalogTable, true);
        }
    }

    @Override
    public ExecuteResult doWork(ExecutableContext context)
            throws ExecuteException, PersistentException {
        ExecuteResult executeResult = new ExecuteResult();
        try {
            // step1: find spark datasource tables
            SqlParser sqlParser = SqlParser.create(selectDataSql);
            SqlNode sqlNode = sqlParser.parseQuery();
            List<Tuple2<String, String>> dbTables = Lists.newArrayList();
            findSparkDataSourceTable(sqlNode, dbTables);

            // step2: create table for every spark datasource table in hive metastore
            for (Tuple2<String, String> dbTable: dbTables) {
                createTableForSparkDataSource(dbTable);
            }
        } catch (Exception e) {
            logger.error("create spark datasource table step failed: " +
                    ExceptionUtils.getStackTrace(e));
            executeResult = ExecuteResult.createFailed(e);
        }

        return executeResult;
    }
}
