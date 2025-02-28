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
package org.apache.kylin.engine.spark.source;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.source.IReadableTable.TableReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class NSparkTableReader implements TableReader {
    private String dbName;
    private String tableName;
    private SparkSession ss;
    private List<Row> records;
    private Iterator<Row> iterator;
    private Row currentRow;

    public NSparkTableReader(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
        initialize();
    }

    public static String[] getRowAsStringArray(Row record) {
        StructField[] fields = record.schema().fields();
        String[] arr = new String[fields.length];
        for (int i = 0; i < arr.length; i++) {
            Object o = record.get(i);
            arr[i] = (o == null) ? null : o.toString();
        }
        return arr;
    }

    private void initialize() {
        ss = SparderEnv.getSparkSession();
        String master = ss.sparkContext().master();
        String tableIdentity = tableName;
        // spark sql can not add the database prefix when create tempView from csv,
        // but when working with hive, it need the database prefix
        if (!master.toLowerCase(Locale.ROOT).contains("local")) {
            tableIdentity = String.format(Locale.ROOT, "%s.%s", dbName, tableName);
        }
        records = SparkSqlUtil.queryAll(ss, tableIdentity);
        iterator = records.iterator();
    }

    @Override
    public boolean next() throws IOException {
        boolean hasNext = iterator != null && iterator.hasNext();
        if (hasNext) {
            currentRow = iterator.next();
        }
        return hasNext;
    }

    @Override
    public String[] getRow() {
        return getRowAsStringArray(currentRow);
    }

    @Override
    public void close() throws IOException {
        this.records = null;
        this.iterator = null;
        this.currentRow = null;
    }
}
