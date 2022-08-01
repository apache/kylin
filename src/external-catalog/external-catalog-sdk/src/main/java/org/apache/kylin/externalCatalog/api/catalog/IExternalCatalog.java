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
package org.apache.kylin.externalCatalog.api.catalog;

import org.apache.kylin.externalCatalog.api.ApiException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public interface IExternalCatalog {
    /**
     * Get all existing databases that match the given pattern.
     * The matching occurs as per Java regular expressions
     * @param databasePattern java re pattern
     * @return List of database names.
     * @throws ApiException
     */
    List<String> getDatabases(String databasePattern) throws ApiException;

    /**
     * Get a Database Object
     * @param databaseName name of the database to fetch
     * @return the database object
     * @throws ApiException
     */
    Database getDatabase(String databaseName) throws ApiException;

    /**
     * Returns metadata of the table
     * @param dbName the name of the database
     * @param tableName the name of the table
     * @param throwException controls whether an exception is thrown or a returns a null when table not found
     * @return the table or if throwException is false a null value.
     * @throws ApiException
     */
    Table getTable(String dbName, String tableName, boolean throwException) throws ApiException;

    /**
     * Returns all existing tables from the specified database which match the given
     * pattern. The matching occurs as per Java regular expressions.
     * @param dbName database name
     * @param tablePattern java re pattern
     * @return list of table names
     * @throws ApiException
     */
    List<String> getTables(String dbName, String tablePattern) throws ApiException;

    /**
     * Returns data of the table
     *
     * @param session spark session
     * @param dbName the name of the database
     * @param tableName the name of the table
     * @param throwException controls whether an exception is thrown or a returns a null when table not found
     * @return the table data or if throwException is false a null value.
     * @throws ApiException
     */
    Dataset<Row> getTableData(SparkSession session, String dbName, String tableName, boolean throwException)
            throws ApiException;

    /**
     * List the metadata of all partitions that belong to the specified table, assuming it exists.
     *
     * @param dbName database name
     * @param tablePattern table name
     *
     */
    List<Partition> listPartitions(String dbName, String tablePattern) throws ApiException;
}
