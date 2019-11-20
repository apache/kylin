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

import java.util.Iterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.sources.BaseRelation;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class SparkDataSourceDescTest extends SparkDataSourceTestBase {

    @Test
    public void testCsvDataSource() {
        for (int i = 1; i <= 4; i++) {
            String dsId = "ds" + String.valueOf(i);
            SparkDataSourceDesc dataSourceDesc = getDataSourceDesc(dsId);
            DataSource dataSource = dataSourceDesc.toDataSource();
            assertTrue(null != dataSource);
            BaseRelation baseRelation = dataSource.resolveRelation(false);
            assertTrue(null != baseRelation);
            Dataset<Row> dataset = spark.baseRelationToDataFrame(baseRelation);
            assertTrue(null != dataset);

            checkDatasetRows(dataset);
        }
    }

    private void checkDatasetRows(Dataset<Row> dataset) {
        Iterator<Row> iterator = dataset.orderBy("salary").toLocalIterator();
        Row row0 = iterator.next();
        assertTrue(row0.getString(0).equals("Michael"));
        assertTrue(row0.get(1).toString().equals("3000"));

        Row row1 = iterator.next();
        assertTrue(row1.getString(0).equals("Justin"));
        assertTrue(row1.get(1).toString().equals("3500"));

        Row row2 = iterator.next();
        assertTrue(row2.getString(0).equals("Berta"));
        assertTrue(row2.get(1).toString().equals("4000"));

        Row row3 = iterator.next();
        assertTrue(row3.getString(0).equals("Andy"));
        assertTrue(row3.get(1).toString().equals("4500"));

        assertFalse(iterator.hasNext());
    }

    private SparkDataSourceDesc getDataSourceDesc(String id) {
        SparkDataSourceDesc dataSourceDesc = new SparkDataSourceDesc(
                config.getSparkSqlDataSourceClassName(id),
                config.getSparkSqlDataSourcePaths(id),
                config.getSparkSqlDataSourceSchema(id),
                config.getSparkSqlDataSourcePartitionCols(id),
                config.getSparkSqlDataSourceBucketSpec(id),
                config.getSparkSqlDatasourceOptions(id));
        return dataSourceDesc;
    }
}
