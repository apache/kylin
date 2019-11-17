/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.engine.spark.source;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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

    private void initialize() {
        ss = SparderEnv.getSparkSession();
        String master = ss.sparkContext().master();
        String tableIdentity = tableName;
        // spark sql can not add the database prefix when create tempView from csv, but when working with hive, it need the database prefix
        if (!master.toLowerCase().contains("local")) {
            tableIdentity = String.format("%s.%s", dbName, tableName);
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

    public static String[] getRowAsStringArray(Row record) {
        StructField[] fields = record.schema().fields();
        String[] arr = new String[fields.length];
        for (int i = 0; i < arr.length; i++) {
            Object o = record.get(i);
            arr[i] = (o == null) ? null : o.toString();
        }
        return arr;
    }

    @Override
    public void close() throws IOException {
        this.records = null;
        this.iterator = null;
        this.currentRow = null;
    }
}
