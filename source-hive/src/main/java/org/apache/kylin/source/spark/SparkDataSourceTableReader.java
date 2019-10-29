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

package org.apache.kylin.source.spark;

import java.io.IOException;
import java.util.Iterator;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.sources.BaseRelation;


/**
 * Reader to read a spark datasource table.
 */
public class SparkDataSourceTableReader implements IReadableTable.TableReader {
    private TableDesc tableDesc;
    private Iterator<Row> rowsIterator;

    public SparkDataSourceTableReader(TableDesc tableDesc) {
        this.tableDesc = tableDesc;
        this.rowsIterator = getTableDataset();
    }

    private Iterator<Row> getTableDataset() {
        SparkDataSourceDesc dataSourceDesc = SparkDataSourceCache.get(tableDesc.getName());
        DataSource dataSource = dataSourceDesc.toDataSource();
        BaseRelation baseRelation = dataSource.resolveRelation(false);
        Dataset<Row> dataset = SparkSqlSource.sparkSession().baseRelationToDataFrame(baseRelation);
        return dataset.toLocalIterator();
    }

    @Override
    public boolean next() throws IOException {
        return rowsIterator.hasNext();
    }

    @Override
    public String[] getRow() {
        Row row = rowsIterator.next();
        int colsSize = row.size();
        String[] columns = new String[colsSize];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = row.get(i).toString();
        }
        return columns;
    }

    @Override
    public void close() throws IOException {

    }
}
